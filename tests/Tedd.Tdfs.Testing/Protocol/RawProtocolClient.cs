using System.Globalization;
using System.Text;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>A protocol client that bypasses all SDK validation (PATH-012, SEC-004/005/014/015).</summary>
/// <remarks>
/// <para>The client owns one transport under its network identity (<c>client:raw{n}</c>, or a backup node's name) and a
/// primary connection to the node it was opened for. <see cref="AuthenticateAsync"/> runs the real handshake on it; volume
/// requests lazily open one session per volume (<c>OpenSession</c>), allocate a fresh operation id per mutating call and
/// are routed to the volume's leader (connections to other nodes authenticate with the same identity), following
/// <c>NotLeader</c> hints. Every member reports the server's error code instead of throwing for refusals: transport loss
/// is <see cref="DfsErrorCode.Unavailable"/>, a closed connection during a raw frame is
/// <see cref="DfsErrorCode.ProtocolError"/>, no reply in time is <see cref="DfsErrorCode.Timeout"/>.</para>
/// <para>Requests are encoded exactly as given: paths are sent as the wire UTF-8 of the string (no normalisation), enum
/// values and flags verbatim.</para>
/// </remarks>
public sealed partial class RawProtocolClient : IAsyncDisposable
{
    private const int MaxRouteAttempts = 8;

    private readonly object _gate = new();
    private readonly TestCluster _cluster;
    private readonly IRpcTransportFactory _factory;
    private readonly IRandomSource _random;
    private readonly Guid _expectedClusterId;
    private readonly string _endpoint;
    private readonly Dictionary<string, IRpcConnection> _peers = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, RawSession> _sessions = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (VolumeId Id, ulong Epoch, string? Leader)> _routes = new(StringComparer.Ordinal);
    private IRpcTransport? _transport;
    private IRpcConnection? _connection;
    private DfsErrorCode _refusal;
    private DfsCredentials? _credentials;
    private PeerRole _role = PeerRole.Client;
    private bool _disposed;

    private RawProtocolClient(TestCluster cluster, string networkName, string nodeName, string endpoint, Guid expectedClusterId,
        IRpcTransportFactory factory)
    {
        _cluster = cluster;
        NetworkName = networkName;
        NodeName = nodeName;
        _endpoint = endpoint;
        _expectedClusterId = expectedClusterId;
        _factory = factory;
        _random = cluster.ForkRandom("raw/" + networkName + "/" + nodeName);
    }

    /// <summary>True while the underlying connection is open (and its handshake was not refused).</summary>
    public bool IsConnected => _refusal == DfsErrorCode.None && _connection is { IsOpen: true } && !_disposed;

    /// <summary>The client's network identity.</summary>
    internal string NetworkName { get; }

    /// <summary>The node the primary connection goes to.</summary>
    internal string NodeName { get; }

    /// <summary>The cluster the client belongs to.</summary>
    internal TestCluster Cluster => _cluster;

    /// <summary>Timeout of one request (harness time).</summary>
    internal static TimeSpan CallTimeout { get; } = TimeSpan.FromSeconds(10);

    /// <summary>Runs the authentication handshake; returns the resulting error code (<see cref="DfsErrorCode.None"/> on success).</summary>
    public async Task<DfsErrorCode> AuthenticateAsync(DfsCredentials credentials)
    {
        ArgumentNullException.ThrowIfNull(credentials);
        if (_refusal != DfsErrorCode.None)
            return _refusal;
        IRpcConnection connection = _connection ?? throw new InvalidOperationException("The raw client is not connected.");
        DfsErrorCode code = await HandshakeAsync(connection, credentials, PeerRole.Client);
        if (code == DfsErrorCode.None)
        {
            lock (_gate)
            {
                _credentials = credentials;
                _role = PeerRole.Client;
            }
        }

        return code;
    }

    /// <summary>
    /// Sends a path operation with the path verbatim (wire UTF-8 of the string). Kinds: CreateDirectory, Delete,
    /// Open (CreateNew, write), Move, GetInfo, WriteAll.
    /// </summary>
    public Task<DfsErrorCode> SendPathOperationAsync(string volume, DfsOperationKind kind, string rawPath, string? rawPath2 = null)
    {
        ArgumentNullException.ThrowIfNull(rawPath);
        return SendPathOperationCoreAsync(volume, kind, rawPath, rawPath2, extensions: null, rawPathBytes: null);
    }

    /// <summary>Sends a path operation whose path is arbitrary (possibly malformed) UTF-8 bytes.</summary>
    public Task<DfsErrorCode> SendRawPathBytesAsync(string volume, DfsOperationKind kind, byte[] rawUtf8PathBytes)
    {
        ArgumentNullException.ThrowIfNull(rawUtf8PathBytes);
        return SendPathOperationCoreAsync(volume, kind, rawPath: null, rawPath2: null, extensions: null, rawPathBytes: rawUtf8PathBytes);
    }

    /// <summary>Sends arbitrary bytes; <see cref="DfsErrorCode.ProtocolError"/> when the server closes the connection.</summary>
    public async Task<DfsErrorCode> SendFrameAsync(ReadOnlyMemory<byte> frame)
    {
        if (_refusal != DfsErrorCode.None)
            return _refusal;
        IRpcConnection? connection = _connection;
        if (connection is null || !connection.IsOpen)
            return DfsErrorCode.ProtocolError;
        try
        {
            ReadOnlyMemory<byte> reply = await connection.CallAsync(frame, CallTimeout, CancellationToken.None);
            return NetworkFabric.CodeOf(reply.Span);
        }
        catch (TimeoutException)
        {
            return connection.IsOpen ? DfsErrorCode.Timeout : DfsErrorCode.ProtocolError;
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException or IOException)
        {
            return DfsErrorCode.ProtocolError;
        }
    }

    /// <summary>Reads an object directly from a storage node without a capability.</summary>
    public async Task<DfsErrorCode> GetObjectDirectAsync(string volume, ObjectId id)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        (DfsErrorCode code, VolumeId volumeId) = await ResolveVolumeIdAsync(volume);
        if (code != DfsErrorCode.None)
            return code;
        (DfsErrorCode error, _) = await CallPrimaryAsync(new GetObjectMessage(ReadOnlyMemory<byte>.Empty, volumeId, id, 0, long.MaxValue));
        return error;
    }

    /// <summary>Writes an object directly to a storage node without a capability.</summary>
    public async Task<DfsErrorCode> PutObjectDirectAsync(string volume, ObjectId id, ReadOnlyMemory<byte> data)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        (DfsErrorCode code, VolumeId volumeId) = await ResolveVolumeIdAsync(volume);
        if (code != DfsErrorCode.None)
            return code;
        var put = new PutObjectMessage(ReadOnlyMemory<byte>.Empty, volumeId, default, id, ObjectChecksum.Compute(data.Span), data.Length, ObjectEncodings.Plain,
            0, data);
        (DfsErrorCode error, _) = await CallPrimaryAsync(put);
        return error;
    }

    /// <summary>Typed round-trip; an error reply is returned as a message.</summary>
    /// <exception cref="InvalidOperationException">The connection is closed or was refused.</exception>
    /// <exception cref="TimeoutException">No reply in time.</exception>
    public async Task<IWireMessage> CallAsync(IWireMessage request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (_refusal != DfsErrorCode.None)
            return new ErrorReplyMessage(_refusal, "The handshake was refused.");
        IRpcConnection connection = _connection ?? throw new InvalidOperationException("The raw client is not connected.");
        try
        {
            ReadOnlyMemory<byte> reply = await connection.CallAsync(MessageCodec.Encode(request), CallTimeout, CancellationToken.None);
            return MessageCodec.Decode(reply.Span);
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException)
        {
            throw new InvalidOperationException($"The raw connection to {NodeName} is closed.", ex);
        }
    }

    /// <summary>Closes the connection.</summary>
    public async ValueTask DisposeAsync()
    {
        IRpcTransport? transport;
        RawSession[] sessions;
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            transport = _transport;
            _transport = null;
            sessions = [.. _sessions.Values];
            _peers.Clear();
        }

        foreach (RawSession session in sessions)
            session.StopKeepAlive();
        if (transport is not null)
        {
            try
            {
                await transport.DisposeAsync();
            }
            catch (Exception ex) when (ex is ObjectDisposedException or RpcConnectionException or IOException)
            {
                // Already gone.
            }
        }
    }

    // -------------------------------------------------------------------------------------------- construction

    /// <summary>Connects a raw client under <paramref name="networkName"/> to <paramref name="endpoint"/> (unauthenticated).</summary>
    internal static async Task<RawProtocolClient> ConnectAsync(TestCluster cluster, string networkName, string nodeName, string endpoint,
        Guid expectedClusterId, CancellationToken ct, bool isHarness = false)
    {
        IRpcTransportFactory factory = cluster.Fabric.CreateFactory(new TransportIdentity
        {
            Owner = cluster,
            Clock = isHarness ? cluster.Runtime.Time : cluster.Runtime.CreateProcessClock(networkName),
            IsHarness = isHarness,
            ClusterId = expectedClusterId,
            TrustedPins = cluster.TrustedPins,
        });
        var client = new RawProtocolClient(cluster, networkName, nodeName, endpoint, expectedClusterId, factory);
        await client.OpenPrimaryAsync(ct);
        cluster.Record($"raw client {networkName}: connected to {nodeName}");
        return client;
    }

    /// <summary>Connects a raw client to a live node presenting a backup node's node-level identity (LOCK-020).</summary>
    internal static async Task<RawProtocolClient> ConnectAsBackupAsync(TestCluster cluster, TestBackupNode backup, string nodeName, string endpoint,
        CancellationToken ct)
    {
        IRpcTransportFactory factory = cluster.Fabric.CreateFactory(new TransportIdentity
        {
            Owner = cluster,
            Clock = backup.Clock,
            ClusterId = cluster.ClusterId,
            Credentials = backup.ExportCredentials,
            TrustedPins = cluster.TrustedPins,
        });
        var client = new RawProtocolClient(cluster, backup.Name, nodeName, endpoint, cluster.ClusterId, factory);
        await client.OpenPrimaryAsync(ct);
        DfsErrorCode code = await client.HandshakeAsync(client._connection!, backup.ExportCredentials, PeerRole.Backup);
        lock (client._gate)
        {
            if (code == DfsErrorCode.None)
            {
                client._credentials = backup.ExportCredentials;
                client._role = PeerRole.Backup;
            }
            else
            {
                client._refusal = code;
            }
        }

        cluster.Record($"raw client {backup.Name} (backup identity): connected to {nodeName}, handshake {code}");
        return client;
    }

    private async Task OpenPrimaryAsync(CancellationToken ct)
    {
        IRpcTransport transport = _factory.Create(NetworkName, string.Empty);
        lock (_gate)
            _transport = transport;
        _connection = await transport.ConnectAsync(_endpoint, ct);
    }

    // -------------------------------------------------------------------------------------------- shared request machinery

    /// <summary>Runs the principal handshake on <paramref name="connection"/>; the error code of a refusal.</summary>
    private async Task<DfsErrorCode> HandshakeAsync(IRpcConnection connection, DfsCredentials credentials, PeerRole role)
    {
        IRandomSource random;
        lock (_gate)
            random = _random.Fork("hello/" + _random.NextUInt64().ToString("x16", CultureInfo.InvariantCulture));
        try
        {
            await HandshakeClient.AuthenticatePrincipalAsync(connection, _expectedClusterId, NetworkName, credentials, random, CallTimeout, role,
                CancellationToken.None);
            return DfsErrorCode.None;
        }
        catch (TimeoutException ex)
        {
            _cluster.Record($"raw client {NetworkName}: handshake with {NodeName} timed out ({ex.Message})");
            return DfsErrorCode.Timeout;
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException || (ex is IOException && ex is not DfsException))
        {
            _cluster.Record($"raw client {NetworkName}: handshake with {NodeName} lost its connection ({ex.GetType().Name}: {ex.Message})");
            return DfsErrorCode.Unavailable;
        }
        catch (Exception ex) when (ex is DfsException or UnauthorizedAccessException or ArgumentException)
        {
            DfsErrorCode code = DfsErrors.ToCode(ex);
            if (code == DfsErrorCode.None)
                code = DfsErrorCode.AuthenticationFailed;
            _cluster.Record($"raw client {NetworkName}: handshake with {NodeName} refused ({code})");
            return code;
        }
    }

    /// <summary>Sends one message on the primary connection; returns its error code and the decoded reply.</summary>
    internal async Task<(DfsErrorCode Code, IWireMessage? Reply)> CallPrimaryAsync(IWireMessage request)
    {
        if (_refusal != DfsErrorCode.None)
            return (_refusal, null);
        IRpcConnection? connection = _connection;
        if (connection is null || !connection.IsOpen)
            return (DfsErrorCode.Unavailable, null);
        return await CallOnAsync(connection, MessageCodec.Encode(request));
    }

    /// <summary>Sends a frame on a connection; returns the reply's error code and the decoded reply.</summary>
    private static async Task<(DfsErrorCode Code, IWireMessage? Reply)> CallOnAsync(IRpcConnection connection, ReadOnlyMemory<byte> frame)
    {
        try
        {
            ReadOnlyMemory<byte> raw = await connection.CallAsync(frame, CallTimeout, CancellationToken.None);
            IWireMessage reply;
            try
            {
                reply = MessageCodec.Decode(raw.Span);
            }
            catch (DfsProtocolException)
            {
                return (DfsErrorCode.ProtocolError, null);
            }

            return (reply is ErrorReplyMessage error ? error.Code : DfsErrorCode.None, reply);
        }
        catch (TimeoutException)
        {
            return (DfsErrorCode.Timeout, null);
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException or IOException)
        {
            return (DfsErrorCode.Unavailable, null);
        }
    }

    /// <summary>An open connection to <paramref name="endpoint"/> authenticated like the primary one.</summary>
    private async Task<(DfsErrorCode Code, IRpcConnection? Connection)> ConnectionToAsync(string endpoint)
    {
        if (string.Equals(NetworkFabric.NormalizeEndpoint(endpoint), NetworkFabric.NormalizeEndpoint(_endpoint), StringComparison.OrdinalIgnoreCase))
            return _connection is { IsOpen: true } primary ? (DfsErrorCode.None, primary) : (DfsErrorCode.Unavailable, null);
        IRpcTransport transport;
        DfsCredentials? credentials;
        PeerRole role;
        lock (_gate)
        {
            if (_disposed)
                return (DfsErrorCode.Unavailable, null);
            if (_peers.TryGetValue(endpoint, out IRpcConnection? existing) && existing.IsOpen)
                return (DfsErrorCode.None, existing);
            transport = _transport ?? throw new ObjectDisposedException(nameof(RawProtocolClient));
            credentials = _credentials;
            role = _role;
        }

        IRpcConnection connection;
        try
        {
            connection = await transport.ConnectAsync(endpoint, CancellationToken.None);
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException or IOException or TimeoutException)
        {
            return (DfsErrorCode.Unavailable, null);
        }

        if (credentials is not null)
        {
            DfsErrorCode code = await HandshakeAsync(connection, credentials, role);
            if (code != DfsErrorCode.None)
            {
                await connection.DisposeAsync();
                return (code, null);
            }
        }

        lock (_gate)
            _peers[endpoint] = connection;
        return (DfsErrorCode.None, connection);
    }

    /// <summary>Sends a frame to the node at <paramref name="endpoint"/> (authenticated like the primary connection).</summary>
    internal async Task<(DfsErrorCode Code, IWireMessage? Reply)> CallNodeAsync(string endpoint, ReadOnlyMemory<byte> frame)
    {
        if (_refusal != DfsErrorCode.None)
            return (_refusal, null);
        (DfsErrorCode code, IRpcConnection? connection) = await ConnectionToAsync(endpoint);
        return connection is null ? (code, null) : await CallOnAsync(connection, frame);
    }

    /// <summary>The volume's id (and epoch, leader endpoint) as routed by the node this client is connected to.</summary>
    internal async Task<(DfsErrorCode Code, VolumeId Id)> ResolveVolumeIdAsync(string volume)
    {
        (DfsErrorCode code, _) = await RouteAsync(volume, refresh: false);
        lock (_gate)
            return _routes.TryGetValue(volume, out (VolumeId Id, ulong Epoch, string? Leader) route) ? (code, route.Id) : (code, default);
    }

    /// <summary>The endpoint of <paramref name="volume"/>'s leader (or the primary endpoint when unknown).</summary>
    private async Task<(DfsErrorCode Code, string Endpoint)> RouteAsync(string volume, bool refresh)
    {
        lock (_gate)
        {
            if (!refresh && _routes.TryGetValue(volume, out (VolumeId Id, ulong Epoch, string? Leader) cached))
                return (DfsErrorCode.None, cached.Leader ?? _endpoint);
        }

        (DfsErrorCode code, IWireMessage? reply) = await CallPrimaryAsync(new GetVolumeRouteMessage(volume, RequireLeader: false));
        if (reply is VolumeRouteResultMessage route)
        {
            lock (_gate)
                _routes[volume] = (route.Volume.Id, route.Volume.Epoch, route.LeaderEndpoint);
            return (DfsErrorCode.None, route.LeaderEndpoint ?? _endpoint);
        }

        return (code == DfsErrorCode.None ? DfsErrorCode.ProtocolError : code, _endpoint);
    }

    /// <summary>
    /// Sends a volume request built by <paramref name="build"/> (given the header: session and, for mutations, a fresh
    /// operation id reused by every retry) to the volume's leader, following <c>NotLeader</c> hints.
    /// </summary>
    internal async Task<(DfsErrorCode Code, IWireMessage? Reply)> SendVolumeRequestAsync(string volume, bool mutating, Func<RequestHeader, byte[]> build,
        bool needsSession = true)
    {
        if (_refusal != DfsErrorCode.None)
            return (_refusal, null);
        ulong session = 0;
        DfsOperationId operation = default;
        if (needsSession)
        {
            (DfsErrorCode sessionCode, RawSession? raw) = await GetSessionAsync(volume);
            if (raw is null)
                return (sessionCode, null);
            session = raw.Session;
            if (mutating)
                operation = raw.NextOperation();
        }

        byte[] frame = build(new RequestHeader(volume, Session: session, Operation: operation));
        return await SendToLeaderAsync(volume, frame);
    }

    /// <summary>Sends a prepared frame to the leader of <paramref name="volume"/>, following <c>NotLeader</c> hints.</summary>
    internal async Task<(DfsErrorCode Code, IWireMessage? Reply)> SendToLeaderAsync(string volume, byte[] frame)
    {
        (DfsErrorCode routeCode, string endpoint) = await RouteAsync(volume, refresh: false);
        if (routeCode is not (DfsErrorCode.None or DfsErrorCode.NotLeader))
            return (routeCode, null);
        (DfsErrorCode Code, IWireMessage? Reply) last = (DfsErrorCode.Unavailable, null);
        for (int attempt = 0; attempt < MaxRouteAttempts; attempt++)
        {
            (DfsErrorCode connectCode, IRpcConnection? connection) = await ConnectionToAsync(endpoint);
            if (connection is null)
            {
                last = (connectCode, null);
            }
            else
            {
                last = await CallOnAsync(connection, frame);
                if (last.Reply is not ErrorReplyMessage { Code: DfsErrorCode.NotLeader } notLeader)
                    return last;
                if (notLeader.LeaderHint is { } hint && !string.Equals(hint, endpoint, StringComparison.OrdinalIgnoreCase))
                {
                    endpoint = hint;
                    continue;
                }
            }

            await _cluster.Runtime.DelayAsync(_cluster.Options.Timings.Tick * _cluster.Options.Timings.HeartbeatTicks);
            (_, endpoint) = await RouteAsync(volume, refresh: true);
        }

        return last;
    }

    /// <summary>This client's session on <paramref name="volume"/>, opened on first use.</summary>
    internal async Task<(DfsErrorCode Code, RawSession? Session)> GetSessionAsync(string volume)
    {
        lock (_gate)
        {
            if (_sessions.TryGetValue(volume, out RawSession? existing))
                return (DfsErrorCode.None, existing);
        }

        UInt128 nonce;
        lock (_gate)
            nonce = new UInt128(_random.NextUInt64(), _random.NextUInt64());
        byte[] frame = MessageCodec.Encode(new OpenSessionMessage(new RequestHeader(volume), NetworkName, nonce));
        (DfsErrorCode code, IWireMessage? reply) = await SendToLeaderAsync(volume, frame);
        if (reply is not OpenSessionResultMessage opened)
            return (code == DfsErrorCode.None ? DfsErrorCode.ProtocolError : code, null);
        lock (_gate)
        {
            if (_sessions.TryGetValue(volume, out RawSession? raced))
                return (DfsErrorCode.None, raced);
            var session = new RawSession(this, volume, opened.Session, opened.FirstSequence, opened.LeaseDuration);
            _sessions[volume] = session;
            return (DfsErrorCode.None, session);
        }
    }

    /// <summary>Encodes <paramref name="message"/>; when <paramref name="rawPath"/> is set, the string field equal to <paramref name="placeholder"/> is replaced by those bytes.</summary>
    internal static byte[] Encode(IWireMessage message, string? placeholder = null, byte[]? rawPath = null)
    {
        byte[] frame = MessageCodec.Encode(message);
        if (placeholder is null || rawPath is null)
            return frame;
        byte[] needle = EncodeStringField(Encoding.UTF8.GetBytes(placeholder));
        int at = frame.AsSpan().IndexOf(needle);
        if (at < 0)
            throw new InvalidOperationException("The path placeholder was not found in the encoded request.");
        byte[] replacement = EncodeStringField(rawPath);
        return [.. frame.AsSpan(0, at), .. replacement, .. frame.AsSpan(at + needle.Length)];
    }

    private static byte[] EncodeStringField(ReadOnlySpan<byte> utf8)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>(utf8.Length + 10);
        var writer = new WireWriter(buffer);
        writer.WriteUtf8(utf8);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>A unique placeholder path used to splice raw path bytes into an encoded request.</summary>
    private string NewPlaceholder()
    {
        lock (_gate)
            return "/__tdfs-raw-path-" + _random.NextUInt64().ToString("x16", CultureInfo.InvariantCulture) + "__";
    }

    private async Task<DfsErrorCode> SendPathOperationCoreAsync(string volume, DfsOperationKind kind, string? rawPath, string? rawPath2,
        IReadOnlyList<WireExtension>? extensions, byte[]? rawPathBytes)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        string? placeholder = rawPathBytes is null ? null : NewPlaceholder();
        string path = placeholder ?? rawPath!;
        bool mutating = kind != DfsOperationKind.GetInfo;
        if (kind == DfsOperationKind.Move && rawPath2 is null && rawPathBytes is null)
            throw new ArgumentException("A move needs a destination path.", nameof(rawPath2));

        (DfsErrorCode code, IWireMessage? reply) = await SendVolumeRequestAsync(volume, mutating, header =>
        {
            IWireMessage message = kind switch
            {
                DfsOperationKind.CreateDirectory => new CreateDirectoryMessage(header, path, Recursive: false, SnapshotName: null),
                DfsOperationKind.Delete => new DeleteMessage(header, path, Recursive: false, ExpectDirectory: false, SnapshotName: null),
                DfsOperationKind.Open => new OpenHandleMessage(header, path, FileMode.CreateNew, FileAccess.Write, FileShare.None, FileOptions.None,
                    VersionPinned: false, WriteThrough: false, StoragePolicy: null),
                DfsOperationKind.Move => new MoveMessage(header, path, rawPath2 ?? path + "-moved", Overwrite: false, ExpectDirectory: false, SnapshotName: null),
                DfsOperationKind.GetInfo => new GetInfoMessage(header, path, default, 0),
                DfsOperationKind.WriteAll => new WriteAllMessage(header, path, 0, [], [], default, StoragePolicy: null, SnapshotName: null, CreateParents: false,
                    ExpectedRevision: 0),
                _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Raw path operations support CreateDirectory, Delete, Open, Move, GetInfo and WriteAll."),
            };
            if (extensions is { Count: > 0 })
                message = WithExtraExtensions(message, extensions);
            return Encode(message, placeholder, rawPathBytes);
        });

        if (kind == DfsOperationKind.Open && reply is OpenHandleResultMessage opened)
            await SendVolumeRequestAsync(volume, mutating: true, header => Encode(new CloseHandleMessage(header, opened.Handle, opened.WriterGeneration)));
        return code;
    }

    /// <summary>A volume session of this raw client.</summary>
    internal sealed class RawSession
    {
        private readonly RawProtocolClient _owner;
        private readonly object _gate = new();
        private readonly HashSet<HandleId> _autoRenew = [];
        private readonly Dictionary<HandleId, ulong> _generations = [];
        private ulong _nextSequence;
        private bool _keepAliveRunning;
        private bool _stopped;

        public RawSession(RawProtocolClient owner, string volume, ulong session, ulong firstSequence, TimeSpan leaseDuration)
        {
            _owner = owner;
            Volume = volume;
            Session = session;
            _nextSequence = firstSequence;
            LeaseDuration = leaseDuration;
        }

        public string Volume { get; }

        public ulong Session { get; }

        public TimeSpan LeaseDuration { get; }

        public DfsOperationId NextOperation()
        {
            lock (_gate)
                return new DfsOperationId(Session, _nextSequence++);
        }

        public void RememberGeneration(HandleId handle, ulong generation)
        {
            lock (_gate)
                _generations[handle] = generation;
        }

        public ulong GenerationOf(HandleId handle)
        {
            lock (_gate)
                return _generations.TryGetValue(handle, out ulong generation) ? generation : 0;
        }

        /// <summary>Keeps <paramref name="handle"/> renewed by this session's keep-alive loop (started on first use).</summary>
        public void AutoRenew(HandleId handle)
        {
            bool start;
            lock (_gate)
            {
                _autoRenew.Add(handle);
                start = !_keepAliveRunning && !_stopped;
                _keepAliveRunning |= start;
            }

            if (start)
                _ = KeepAliveLoopAsync();
        }

        public void Forget(HandleId handle)
        {
            lock (_gate)
            {
                _autoRenew.Remove(handle);
                _generations.Remove(handle);
            }
        }

        public void StopKeepAlive()
        {
            lock (_gate)
                _stopped = true;
        }

        private async Task KeepAliveLoopAsync()
        {
            TimeSpan period = LeaseDuration > TimeSpan.Zero ? LeaseDuration / 3 : TimeSpan.FromSeconds(1);
            while (true)
            {
                await _owner._cluster.Runtime.DelayAsync(period);
                HandleId[] handles;
                lock (_gate)
                {
                    // A raw client the test never disposed must not keep renewing (and keep a timer loop alive) after its
                    // cluster is gone.
                    if (_stopped || _owner._disposed || _owner._cluster.IsDisposed)
                        return;
                    handles = [.. _autoRenew];
                }

                await _owner.SendVolumeRequestAsync(Volume, mutating: false, header => Encode(new KeepAliveMessage(header, handles, [], [])));
            }
        }
    }
}
