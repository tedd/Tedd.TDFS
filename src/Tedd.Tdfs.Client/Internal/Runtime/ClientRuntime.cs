using System.Collections.Immutable;
using System.Globalization;
using Tedd.Tdfs.Client.Internal.Data;
using Tedd.Tdfs.Client.Internal.Rpc;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// The services of one <see cref="DfsClient"/> (INTERNALS.md §2): options, injected clock/randomness/transport/faults/
/// mutations, the connection pool, the cluster map, the RPC invoker, the data plane, the per-volume contexts, the bound
/// cluster identity and the client lifecycle (in-flight tracking, shutdown, abandonment).
/// </summary>
internal sealed class ClientRuntime
{
    /// <summary>Protocol features this SDK build understands (none beyond protocol version 1).</summary>
    public static readonly IReadOnlySet<string> SupportedFeatures = ImmutableSortedSet.Create<string>(StringComparer.Ordinal);

    private const int Running = 0;
    private const int Disposing = 1;
    private const int Disposed = 2;

    private readonly Lock _sync = new();
    private readonly CancellationTokenSource _kill = new();
    private readonly Dictionary<string, VolumeContext> _volumes = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Salt, int Iterations), ClientKeys> _keys = [];
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _unresponsive = new(StringComparer.Ordinal);
    private int _state;
    private int _inFlight;
    private TaskCompletionSource? _drained;
    private Task? _shutdown;

    private ClientRuntime(DfsClientOptions options, DfsClientOptionsInternal internals)
    {
        Options = options;
        Internals = internals;
        Time = options.TimeProvider;
        Random = new LockedRandom(internals.Random ?? SystemRandomSource.Instance);
        Faults = internals.Faults ?? NoFaults.Instance;
        Mutations = internals.Mutations ?? SafetyMutations.None;
        NetworkName = string.IsNullOrEmpty(internals.NetworkName) ? "client:" + options.ClientName : internals.NetworkName;
        Transport = CreateTransport(options, internals, NetworkName);
        Pool = new ConnectionPool(this);
        Map = new ClusterMapCache(options.SeedEndpoints);
        Rpc = new RpcInvoker(this);
        Data = new DataPlane(this);
        ControlRouter = new ControlRouter(this);
        AnyNodeRouter = new AnyNodeRouter(this);
    }

    /// <summary>The validated public options.</summary>
    public DfsClientOptions Options { get; }

    /// <summary>Injected advanced settings.</summary>
    public DfsClientOptionsInternal Internals { get; }

    /// <summary>Clock for every deadline, backoff and lease renewal.</summary>
    public TimeProvider Time { get; }

    /// <summary>How long an endpoint that let a data-plane request time out is tried after the others.</summary>
    private static readonly TimeSpan UnresponsiveMemory = TimeSpan.FromSeconds(10);

    /// <summary>Remembers that <paramref name="endpoint"/> let a request time out (paused, partitioned or overloaded).</summary>
    public void NoteUnresponsive(string endpoint) => _unresponsive[endpoint] = Time.GetTimestamp();

    /// <summary>Forgets that <paramref name="endpoint"/> was unresponsive (it answered).</summary>
    public void NoteResponsive(string endpoint) => _unresponsive.TryRemove(endpoint, out _);

    /// <summary>
    /// True when <paramref name="endpoint"/> let a request time out recently and has not answered since: replica reads try
    /// it after the other replicas, so a silent node does not hold up every read (and outlive its read capability).
    /// </summary>
    public bool IsRecentlyUnresponsive(string endpoint) =>
        _unresponsive.TryGetValue(endpoint, out long since) && Time.GetElapsedTime(since) < UnresponsiveMemory;

    /// <summary>Thread-safe injected randomness (nonces, object ids, retry jitter).</summary>
    public LockedRandom Random { get; }

    /// <summary>Fault injector consulted at client sites.</summary>
    public IFaultInjector Faults { get; }

    /// <summary>SDK-side safety mutations (negative controls only).</summary>
    public SafetyMutations Mutations { get; }

    /// <summary>Network identity of the client (fault contexts, transport name).</summary>
    public string NetworkName { get; }

    /// <summary>The client's transport (connect-only).</summary>
    public IRpcTransport Transport { get; }

    /// <summary>Authenticated connections per endpoint.</summary>
    public ConnectionPool Pool { get; }

    /// <summary>Cluster map cache.</summary>
    public ClusterMapCache Map { get; }

    /// <summary>Request routing, retries and outcome classification.</summary>
    public RpcInvoker Rpc { get; }

    /// <summary>Object uploads and verified reads.</summary>
    public DataPlane Data { get; }

    /// <summary>Routing to the control-group leader.</summary>
    public ControlRouter ControlRouter { get; }

    /// <summary>Routing to any live node.</summary>
    public AnyNodeRouter AnyNodeRouter { get; }

    /// <summary>Cluster the client is bound to (fixed by the first successful handshake).</summary>
    public Guid ClusterId { get; private set; }

    /// <summary>Cluster name reported by the bound cluster.</summary>
    public string ClusterName { get; private set; } = string.Empty;

    /// <summary>Authenticated principal.</summary>
    public string Principal { get; private set; } = string.Empty;

    /// <summary>Cluster-level permissions of the principal.</summary>
    public DfsPermissions ClusterPermissions { get; private set; }

    /// <summary>Capabilities advertised by the cluster.</summary>
    public DfsServerCapabilities Capabilities { get; private set; } = new();

    /// <summary>True when the client is connected to a read-only backup host (BAK-015).</summary>
    public bool IsBackupHost { get; private set; }

    /// <summary>Endpoint of the backup host (all requests of a backup-host client go there).</summary>
    public string? BackupHostEndpoint { get; private set; }

    /// <summary>The cluster id every new connection must report (the bound one, or the configured expectation while connecting).</summary>
    public Guid ExpectedClusterIdForConnections => ClusterId != Guid.Empty ? ClusterId : Options.ExpectedClusterId ?? Guid.Empty;

    /// <summary>Cancelled when in-flight work must stop (shutdown deadline passed, client abandoned).</summary>
    public CancellationToken KillToken => _kill.Token;

    /// <summary>True once disposal started.</summary>
    public bool IsDisposed => Volatile.Read(ref _state) != Running;

    /// <summary>Connects a new client: validates nothing (the caller did), contacts the seeds in order and binds the cluster.</summary>
    public static async Task<ClientRuntime> ConnectAsync(DfsClientOptions options, DfsClientOptionsInternal internals, CancellationToken ct)
    {
        var runtime = new ClientRuntime(options, internals);
        try
        {
            await runtime.ConnectSeedsAsync(ct).ConfigureAwait(false);
            return runtime;
        }
        catch
        {
            await runtime.ReleaseResourcesAsync().ConfigureAwait(false);
            throw;
        }
    }

    // ------------------------------------------------------------------------------------------------ operations

    /// <summary>
    /// Starts a public operation: rejects new work after disposal (<see cref="ObjectDisposedException"/>) and a cancelled
    /// token (<see cref="OperationCanceledException"/>), and registers the operation as in flight.
    /// </summary>
    public OperationScope BeginOperation(DfsOperationKind kind, string? volume, string? path, CancellationToken ct, TimeSpan? timeout = null)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_state != Running, typeof(DfsClient));
            ct.ThrowIfCancellationRequested();
            _inFlight++;
        }

        try
        {
            return new OperationScope(this, kind, volume, path, ct, timeout ?? Options.OperationTimeout, tracked: true);
        }
        catch
        {
            EndOperation();
            throw;
        }
    }

    /// <summary>Starts SDK-internal background work (keep-alive, best-effort cleanup); not counted as in-flight work.</summary>
    public OperationScope BeginBackground(DfsOperationKind kind, string? volume, TimeSpan timeout, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _state) == Disposed, typeof(DfsClient));
        return new OperationScope(this, kind, volume, null, ct, timeout, tracked: false);
    }

    /// <summary>Ends an in-flight operation (called by <see cref="OperationScope.Dispose"/>).</summary>
    public void EndOperation()
    {
        TaskCompletionSource? drained = null;
        lock (_sync)
        {
            _inFlight--;
            if (_inFlight == 0 && _drained is not null)
            {
                drained = _drained;
                _drained = null;
            }
        }

        if (drained is not null)
            ContextFree.Run(static d => d.TrySetResult(), drained);
    }

    /// <summary>Throws <see cref="ObjectDisposedException"/> after disposal started.</summary>
    public void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(IsDisposed, typeof(DfsClient));

    /// <summary>The per-client context of volume <paramref name="name"/> (created on first use; no I/O).</summary>
    public VolumeContext GetVolumeContext(string name)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_state != Running, typeof(DfsClient));
            if (!_volumes.TryGetValue(name, out VolumeContext? context))
            {
                context = new VolumeContext(this, name);
                _volumes[name] = context;
            }

            return context;
        }
    }

    /// <summary>Client keys for a principal verifier's salt and iteration count (PBKDF2 is derived once per pair).</summary>
    public ClientKeys GetClientKeys(ReadOnlySpan<byte> salt, int iterations)
    {
        (string, int) key = (Convert.ToHexString(salt), iterations);
        lock (_sync)
        {
            if (_keys.TryGetValue(key, out ClientKeys cached))
                return cached;
        }

        ClientKeys derived = HandshakeAuth.DeriveClientKeys(Options.Credentials.Secret, salt, iterations);
        lock (_sync)
        {
            _keys.TryAdd(key, derived);
            return _keys[key];
        }
    }

    /// <summary>A jittered exponential backoff for retry <paramref name="attempt"/> (1-based) from the injected randomness.</summary>
    public TimeSpan Backoff(int attempt)
    {
        long baseTicks = Math.Max(Options.RetryBaseDelay.Ticks, TimeSpan.FromMilliseconds(1).Ticks);
        long maxTicks = Math.Max(Options.RetryMaxDelay.Ticks, baseTicks);
        int shift = Math.Clamp(attempt - 1, 0, 30);
        long ceiling = baseTicks << shift;
        if (ceiling <= 0 || ceiling > maxTicks)
            ceiling = maxTicks;
        long floor = ceiling / 2;
        long jitter = (long)Random.NextUInt64((ulong)Math.Max(1, ceiling - floor + 1));
        return TimeSpan.FromTicks(floor + jitter);
    }

    // ------------------------------------------------------------------------------------------------ cluster map

    /// <summary>Refreshes the cluster map from any live node (<c>GetClusterMap</c>); returns true when a map was received.</summary>
    public async Task<bool> RefreshMapAsync(OperationScope scope)
    {
        IReadOnlyList<string> endpoints = IsBackupHost && BackupHostEndpoint is { } backup ? [backup] : Map.DiscoveryEndpoints();
        foreach (string endpoint in endpoints)
        {
            if (scope.Token.IsCancellationRequested)
                return false;
            try
            {
                IWireMessage reply = await Rpc.SendOnceAsync(endpoint, new GetClusterMapMessage(0), scope, Rpc.DefaultAttemptTimeout(scope)).ConfigureAwait(false);
                if (reply is ClusterMapResultMessage { Map: { } map } && map.ClusterId == ClusterId)
                {
                    Map.Update(map);
                    Map.MarkGood(endpoint);
                    Capabilities = map.Capabilities ?? Capabilities;
                    return true;
                }
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex) when (RpcInvoker.IsTransient(ex) || ex is DfsProtocolException or DfsClusterIdentityException or DfsException)
            {
                Emit("map.refresh-failed", $"Refreshing the cluster map from {endpoint} failed ({ex.GetType().Name}).", exception: ex);
            }
        }

        return false;
    }

    /// <summary>Refreshes the map when it is older than <paramref name="placementEpoch"/> (uploads place by the staging epoch).</summary>
    public async Task EnsureMapEpochAsync(ulong placementEpoch, OperationScope scope)
    {
        ClusterMapData? map = Map.Current;
        if (map is not null && map.PlacementEpoch >= placementEpoch)
            return;
        await RefreshMapAsync(scope).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------------------------------------ diagnostics

    /// <summary>Reports a diagnostic event (never payloads or secrets; a failing sink is ignored).</summary>
    public void Emit(string name, string message, DfsOperationId? operationId = null, DfsTransactionId? transactionId = null, string? volume = null,
        CommitToken? token = null, Exception? exception = null)
    {
        IDfsDiagnostics? sink = Options.Diagnostics;
        if (sink is null)
            return;
        try
        {
            sink.OnEvent(new DfsDiagnosticEvent(name, operationId, transactionId, volume, token, message, exception));
        }
#pragma warning disable CA1031 // A diagnostics sink must never break the operation that reports to it.
        catch (Exception)
#pragma warning restore CA1031
        {
            // Ignored by contract.
        }
    }

    // ------------------------------------------------------------------------------------------------ lifecycle

    /// <summary>
    /// Disposes the client (FSD §5.1): rejects new work, lets in-flight work finish within
    /// <see cref="DfsClientOptions.ShutdownTimeout"/>, then cancels what remains (uncertain outcomes keep their identity),
    /// stops background work and closes every connection. Never commits or flushes anything. Idempotent; concurrent
    /// callers share one shutdown.
    /// </summary>
    public Task ShutdownAsync()
    {
        lock (_sync)
        {
            if (_shutdown is not null)
                return _shutdown;
            _state = Disposing;
            _shutdown = ShutdownCoreAsync();
            return _shutdown;
        }
    }

    /// <summary>Simulates process death: everything stops immediately, no cleanup traffic is sent.</summary>
    public void Abandon()
    {
        lock (_sync)
        {
            _state = Disposed;
            _shutdown ??= Task.CompletedTask;
        }

        ContextFree.Cancel(_kill);
        StopVolumes();
        Pool.CloseAll();
        ValueTask dispose = DisposeTransportQuietly();
        if (!dispose.IsCompleted)
            Waits.ObserveQuietly(dispose.AsTask());
    }

    private async Task ShutdownCoreAsync()
    {
        StopVolumes();
        Task drained;
        lock (_sync)
        {
            if (_inFlight == 0)
            {
                drained = Task.CompletedTask;
            }
            else
            {
                _drained ??= new TaskCompletionSource(TaskCreationOptions.None);
                drained = _drained.Task;
            }
        }

        bool finished = await Waits.CompletesWithinAsync(drained, Options.ShutdownTimeout, Time).ConfigureAwait(false);
        if (!finished)
        {
            Emit("client.shutdown-cancel", $"Cancelling in-flight work after the {Options.ShutdownTimeout} shutdown deadline.");
            ContextFree.Cancel(_kill);

            // Cancellation resolves every operation promptly (uncertain mutations report their identity); allow a short
            // grace for their completion handlers to run before the connections are closed underneath them.
            await Waits.CompletesWithinAsync(drained, TimeSpan.FromMilliseconds(250), Time).ConfigureAwait(false);
        }

        lock (_sync)
            _state = Disposed;
        ContextFree.Cancel(_kill);
        await ReleaseResourcesAsync().ConfigureAwait(false);
    }

    private void StopVolumes()
    {
        VolumeContext[] volumes;
        lock (_sync)
            volumes = [.. _volumes.Values];
        foreach (VolumeContext volume in volumes)
            volume.Stop();
    }

    private async Task ReleaseResourcesAsync()
    {
        lock (_sync)
        {
            if (_state == Running)
                _state = Disposed;
        }

        ContextFree.Cancel(_kill);
        StopVolumes();
        Pool.CloseAll();
        await DisposeTransportQuietly().ConfigureAwait(false);
    }

    private async ValueTask DisposeTransportQuietly()
    {
        try
        {
            await Transport.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException or RpcConnectionException)
        {
            Emit("client.transport-close-failed", "Closing the transport failed.", exception: ex);
        }
    }

    // ------------------------------------------------------------------------------------------------ connect

    private async Task ConnectSeedsAsync(CancellationToken ct)
    {
        using OperationScope scope = BeginOperation(DfsOperationKind.Connect, null, null, ct, Options.ConnectTimeout);
        await scope.HitAsync(FaultStage.F00, FaultSites.ClientOperationStart).ConfigureAwait(false);
        await scope.HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
        scope.ThrowIfCancelled();

        IReadOnlyList<string> seeds = Options.SeedEndpoints;
        var failures = new List<Exception>();
        DfsClusterIdentityException? mismatch = null;
        for (int i = 0; i < seeds.Count; i++)
        {
            string seed = seeds[i];
            TimeSpan remaining = scope.Remaining;
            if (remaining <= TimeSpan.Zero || scope.Token.IsCancellationRequested)
                break;
            TimeSpan perSeed = TimeSpan.FromTicks(Math.Max(remaining.Ticks / (seeds.Count - i), Math.Min(remaining.Ticks, TimeSpan.FromMilliseconds(250).Ticks)));
            try
            {
                ClientConnection connection = await Pool.GetAsync(seed, scope, perSeed).ConfigureAwait(false);
                ClusterMapData? map = await FetchMapAsync(connection, scope, perSeed).ConfigureAwait(false);
                Bind(connection, map);
                Emit("connect.connected", $"Connected to cluster {ClusterName} ({ClusterId}) through seed {seed}.");
                return;
            }
            catch (DfsAuthenticationException)
            {
                throw;
            }
            catch (OperationCanceledException) when (scope.IsAbortedByCaller)
            {
                throw scope.NotTransmittedFailure(null);
            }
            catch (DfsClusterIdentityException ex)
            {
                mismatch = ex;
                failures.Add(ex);
                Emit("connect.seed-foreign-cluster", $"Seed {seed} belongs to another cluster.", exception: ex);
            }
            catch (Exception ex) when (IsSeedFailure(ex))
            {
                failures.Add(ex);
                Emit("connect.seed-failed", $"Seed {seed} could not be used: {ex.GetType().Name}.", exception: ex);
            }
        }

        if (scope.IsAbortedByCaller)
            throw scope.NotTransmittedFailure(null);
        if (mismatch is not null)
        {
            throw new DfsClusterIdentityException(
                $"No seed endpoint belongs to the expected cluster {Options.ExpectedClusterId}: {mismatch.Message}", mismatch);
        }

        throw new DfsUnavailableException(DfsErrorCode.Unavailable,
            $"None of the {seeds.Count} seed endpoint(s) could be reached and authenticated within the {Options.ConnectTimeout} connect timeout.",
            failures.Count switch { 0 => null, 1 => failures[0], _ => new AggregateException(failures) });
    }

    private static bool IsSeedFailure(Exception ex) =>
        ex is TimeoutException or IOException or OperationCanceledException or DfsUnavailableException or DfsProtocolException
            or NotSupportedException or UnauthorizedAccessException or DfsException;

    private static async Task<ClusterMapData?> FetchMapAsync(ClientConnection connection, OperationScope scope, TimeSpan timeout)
    {
        await scope.HitAsync(FaultStage.F03, FaultSites.ClientRequestSend, DfsOperationKind.Discover, detail: connection.Endpoint).ConfigureAwait(false);
        IWireMessage reply = await connection.CallAsync(MessageCodec.Encode(new GetClusterMapMessage(0)), Positive(timeout), scope.Token).ConfigureAwait(false);
        await scope.HitAsync(FaultStage.F14, FaultSites.ClientReplyReceived, DfsOperationKind.Discover, detail: connection.Endpoint).ConfigureAwait(false);
        return reply switch
        {
            ClusterMapResultMessage result => result.Map,
            ErrorReplyMessage error => throw error.ToException(),
            _ => throw new DfsProtocolException($"Expected ClusterMapResult, received {reply.Type}."),
        };
    }

    private void Bind(ClientConnection connection, ClusterMapData? map)
    {
        HelloChallengeMessage challenge = connection.Challenge;
        HelloResultMessage result = connection.Result;
        ClusterId = challenge.ClusterId;
        ClusterName = challenge.ClusterName;
        Principal = result.Principal;
        ClusterPermissions = result.ClusterPermissions;
        IsBackupHost = connection.IsBackupHost;
        BackupHostEndpoint = connection.IsBackupHost ? connection.Endpoint : null;
        if (map is not null)
            Map.Update(map);
        Map.MarkGood(connection.Endpoint);
        Capabilities = map?.Capabilities ?? result.Capabilities ?? new DfsServerCapabilities();
    }

    /// <summary>Clamps a timeout to a positive value accepted by transports.</summary>
    public static TimeSpan Positive(TimeSpan timeout) => timeout > TimeSpan.FromMilliseconds(1) ? timeout : TimeSpan.FromMilliseconds(1);

    private static IRpcTransport CreateTransport(DfsClientOptions options, DfsClientOptionsInternal internals, string networkName)
    {
        RpcAddress first = RpcAddress.Parse(options.SeedEndpoints[0]);
        string listen = first.IsMemory ? "mem://" + SanitizeMemoryName(networkName) : "quic://127.0.0.1:0";
        IRpcTransportFactory factory = internals.TransportFactory ?? CreateDefaultFactory(options, first);
        return factory.Create(networkName, listen);
    }

    private static IRpcTransportFactory CreateDefaultFactory(DfsClientOptions options, RpcAddress first)
    {
        if (!first.IsQuic)
        {
            throw new NotSupportedException(
                $"Seed endpoint scheme '{first.Scheme}' needs an injected transport; the public SDK connects over QUIC ('quic://host:port').");
        }

        DfsTransportSecurity security = options.Security;
        var quic = new QuiclyTransportOptions(null!, security.PinnedSpkiSha256)
        {
            TimeProvider = options.TimeProvider,
            ConnectTimeout = options.ConnectTimeout,
            ValidateServerCertificate = security.ValidateServerCertificate,
            AllowUntrustedForDevelopment = security.AllowUntrustedForDevelopment,
        };
        return new QuiclyTransportFactory(quic);
    }

    private static string SanitizeMemoryName(string name)
    {
        var chars = name.ToCharArray();
        for (int i = 0; i < chars.Length; i++)
        {
            char c = chars[i];
            if (char.IsControl(c) || char.IsWhiteSpace(c) || c is '/' or '\\' or '?' or '#')
                chars[i] = '-';
        }

        return chars.Length == 0 ? "client" : new string(chars);
    }
}
