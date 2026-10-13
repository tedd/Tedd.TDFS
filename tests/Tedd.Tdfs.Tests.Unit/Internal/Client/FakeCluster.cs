using System.Collections.Concurrent;
using System.Collections.Immutable;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Client;

/// <summary>
/// A scripted single-node "cluster" on the in-memory transport for SDK internal tests: it authenticates clients with the
/// real handshake, serves the cluster map and volume route, and implements a small but faithful volume model (sessions
/// with stored outcomes and fencing, handles with pinned views, staging, object store with unit checksums, CommitFile /
/// WriteAll / Append publication algebra, namespace operations). It is not a server implementation: no Raft, no
/// durability, no permissions — just enough to exercise the client's protocol behaviour deterministically.
/// </summary>
internal sealed class FakeCluster : IRpcHandler, IAsyncDisposable
{
    public const string NodeName = "fake-node";
    public const string Endpoint = "mem://" + NodeName;
    public const string VolumeName = "vol";
    public const string Principal = "tester";
    public const string Secret = "tester-secret";
    public const int InlineThreshold = 4096;
    public const int UnitSize = 4096;

    private readonly System.Threading.Lock _sync = new();
    private readonly IRpcTransport _transport;
    private readonly PrincipalVerifier _verifier;
    private readonly ConcurrentDictionary<IRpcConnection, byte[]> _hellos = new();
    private readonly ConcurrentDictionary<IRpcConnection, byte[]> _challenges = new();
    private readonly Dictionary<string, Inode> _paths = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, SessionState> _sessions = [];
    private readonly Dictionary<ulong, HandleState> _handles = [];
    private readonly HashSet<ulong> _expiredHandles = [];
    private readonly Dictionary<ulong, StagingState> _staging = [];
    private readonly Dictionary<DfsTransactionId, TxState> _transactions = [];
    private readonly Dictionary<ObjectId, (byte[] Data, UInt128 Checksum, StagingId Staging)> _objects = [];
    private ulong _index = 10;
    private ulong _nextSession = 1;
    private ulong _nextHandle = 100;
    private ulong _nextStaging = 1;
    private ulong _nextInode = 2;

    public FakeCluster(ScriptedLinkPolicy? policy = null)
    {
        Policy = policy ?? new ScriptedLinkPolicy();
        Network = new InMemoryNetwork(Policy);
        ClusterId = Guid.NewGuid();
        NodeId = new NodeId(Guid.NewGuid());
        VolumeId = new VolumeId(Guid.NewGuid());
        byte[] salt = new byte[16];
        new Random(7).NextBytes(salt);
        _verifier = HandshakeAuth.CreateVerifier(Secret, salt, 1000);
        _paths["/"] = new Inode(1, isDirectory: true, _index);
        _transport = Network.Factory.Create(NodeName, Endpoint);
    }

    public InMemoryNetwork Network { get; }

    public ScriptedLinkPolicy Policy { get; }

    public Guid ClusterId { get; }

    public NodeId NodeId { get; }

    /// <summary>Id of the volume currently named <see cref="VolumeName"/> (changes when it is re-created).</summary>
    public VolumeId VolumeId { get; private set; }

    /// <summary>Authority epoch of the current volume.</summary>
    public ulong Epoch { get; private set; } = 1;

    /// <summary>Every volume request header received (binding, session and watermark assertions).</summary>
    public ConcurrentQueue<(MessageType Type, RequestHeader Header)> Headers { get; } = new();

    /// <summary>
    /// Called (under the cluster lock) for every request after the handshake; a non-null result is sent as the reply
    /// instead of dispatching the request.
    /// </summary>
    public Func<IWireMessage, IWireMessage?>? Intercept { get; set; }

    /// <summary>Called (under the cluster lock) after a request was dispatched, with the reply about to be sent.</summary>
    public Action<IWireMessage, IWireMessage>? AfterDispatch { get; set; }

    /// <summary>When set, this many further PutObject requests succeed and every later one fails (until reset to null).</summary>
    public int? PutsBeforeFailure { get; set; }

    /// <summary>Handle lease duration issued with new sessions (the SDK renews every third of it).</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Highest acknowledgement watermark any request carried for <paramref name="session"/>.</summary>
    public ulong MaxWatermark(ulong session)
    {
        lock (_sync)
            return _sessions.TryGetValue(session, out SessionState? s) ? s.MaxWatermark : 0;
    }

    /// <summary>Ids of the sessions issued so far (in the current volume).</summary>
    public IReadOnlyList<ulong> SessionIds
    {
        get
        {
            lock (_sync)
                return [.. _sessions.Keys.Order()];
        }
    }

    /// <summary>Handle ids currently open.</summary>
    public IReadOnlyList<ulong> HandleIds
    {
        get
        {
            lock (_sync)
                return [.. _handles.Keys.Order()];
        }
    }

    /// <summary>State of a transaction known to the volume.</summary>
    public string? TransactionState(DfsTransactionId id)
    {
        lock (_sync)
            return _transactions.TryGetValue(id, out TxState? tx) ? tx.State : null;
    }

    /// <summary>Creates (or replaces) a file directly in the committed state.</summary>
    public void SeedFile(string path, byte[] content)
    {
        lock (_sync)
        {
            Inode inode = _paths.TryGetValue(path, out Inode? existing) ? existing : CreateFile(path, null);
            inode.Content = content.ToArray();
            inode.Revision = ++_index;
        }
    }

    /// <summary>
    /// The leader expires a session (as after missed keep-alives): its handles expire, its staging sessions close, its
    /// operation ids become expired.
    /// </summary>
    public void ExpireSession(ulong session)
    {
        lock (_sync)
        {
            if (!_sessions.TryGetValue(session, out SessionState? state))
                return;
            state.Expired = true;
            foreach ((ulong id, HandleState handle) in _handles.ToArray())
            {
                if (handle.Session == session)
                    ExpireHandleCore(id);
            }

            foreach (StagingState staging in _staging.Values)
            {
                if (staging.Session == session)
                    staging.Closed = true;
            }
        }
    }

    /// <summary>Opens a live read handle on <paramref name="path"/> owned by <paramref name="session"/> (as if that session had opened it).</summary>
    public ulong OpenHandleDirect(string path, ulong session)
    {
        lock (_sync)
        {
            Inode inode = _paths[path];
            ulong id = _nextHandle++;
            _handles[id] = new HandleState(inode, false, null, 0, 0, session);
            return id;
        }
    }

    /// <summary>The leader expires one handle's lease (its session stays alive).</summary>
    public void ExpireHandle(ulong handle)
    {
        lock (_sync)
            ExpireHandleCore(handle);
    }

    private void ExpireHandleCore(ulong handle)
    {
        if (!_handles.Remove(handle))
            return;
        _expiredHandles.Add(handle);
        foreach (StagingState staging in _staging.Values)
        {
            if (staging.Handle == handle)
                staging.Closed = true;
        }
    }

    /// <summary>
    /// Deletes the volume and creates a new, unrelated one under the same name (new id, epoch 1, empty namespace, session
    /// and handle numbering restarting like a fresh state machine).
    /// </summary>
    public void RecreateVolume()
    {
        lock (_sync)
        {
            VolumeId = new VolumeId(Guid.NewGuid());
            Epoch = 1;
            _paths.Clear();
            _paths["/"] = new Inode(1, isDirectory: true, ++_index);
            _sessions.Clear();
            _handles.Clear();
            _expiredHandles.Clear();
            _staging.Clear();
            _transactions.Clear();
            _nextSession = 1;
            _nextHandle = 100;
            _nextStaging = 1;
        }
    }

    /// <summary>Default storage policy: one copy in one zone (a single storage node can satisfy it).</summary>
    public static DfsStoragePolicy SingleCopy { get; } = new()
    {
        TargetCopies = 1,
        TargetDistinctZones = 1,
        DurableCopiesBeforeAck = 1,
        DurableZonesBeforeAck = 1,
    };

    /// <summary>Requests received, by type (for assertions about traffic).</summary>
    public ConcurrentDictionary<MessageType, int> Received { get; } = new();

    /// <summary>Number of mutations actually applied, by request type.</summary>
    public ConcurrentDictionary<MessageType, int> Applied { get; } = new();

    public async Task StartAsync() => await _transport.StartAsync(this).ConfigureAwait(false);

    public DfsClientOptions ClientOptions(TimeSpan? operationTimeout = null) => new()
    {
        SeedEndpoints = [Endpoint],
        Credentials = new DfsCredentials(Principal, Secret),
        ClientName = "c0",
        OperationTimeout = operationTimeout ?? TimeSpan.FromSeconds(10),
        ConnectTimeout = TimeSpan.FromSeconds(5),
        RetryBaseDelay = TimeSpan.FromMilliseconds(5),
        RetryMaxDelay = TimeSpan.FromMilliseconds(50),
        ShutdownTimeout = TimeSpan.FromSeconds(2),
    };

    public Task<DfsClient> ConnectAsync(TimeSpan? operationTimeout = null, DfsClientOptions? options = null) =>
        DfsClient.ConnectAsync(options ?? ClientOptions(operationTimeout), new DfsClientOptionsInternal { TransportFactory = Network.Factory },
            CancellationToken.None);

    /// <summary>Committed content of a file (null when absent).</summary>
    public byte[]? Content(string path)
    {
        lock (_sync)
            return _paths.TryGetValue(path, out Inode? inode) && !inode.IsDirectory ? inode.Content.ToArray() : null;
    }

    /// <summary>Committed revision of a file.</summary>
    public ulong Revision(string path)
    {
        lock (_sync)
            return _paths[path].Revision;
    }

    /// <summary>Number of objects currently stored.</summary>
    public int ObjectCount
    {
        get
        {
            lock (_sync)
                return _objects.Count;
        }
    }

    /// <summary>Number of open handles.</summary>
    public int OpenHandles
    {
        get
        {
            lock (_sync)
                return _handles.Count;
        }
    }

    public async ValueTask DisposeAsync() => await _transport.DisposeAsync().ConfigureAwait(false);

    // ------------------------------------------------------------------------------------------------ dispatch

    public ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
    {
        IWireMessage message = MessageCodec.Decode(request.Span);
        Received.AddOrUpdate(message.Type, 1, (_, n) => n + 1);
        IWireMessage reply;
        try
        {
            reply = message switch
            {
                HelloMessage hello => Hello(connection, hello, request),
                HelloProofMessage proof => Proof(connection, proof),
                _ => Dispatch(message),
            };
        }
        catch (DfsException ex)
        {
            reply = new ErrorReplyMessage(ex.ErrorCode, ex.Message, null, ex.IsRetryable);
        }

        ReadOnlyMemory<byte>? frame = MessageCodec.Encode(reply);
        return ValueTask.FromResult(frame);
    }

    private IWireMessage Hello(IRpcConnection connection, HelloMessage hello, ReadOnlyMemory<byte> frame)
    {
        _hellos[connection] = frame.ToArray();
        byte[] nonce = new byte[ProtocolInfo.NonceSize];
        Random.Shared.NextBytes(nonce);
        var challenge = new HelloChallengeMessage(ClusterId, "fake", ProtocolInfo.CurrentVersion, ImmutableSortedSet.Create<string>(StringComparer.Ordinal),
            NodeName, PeerRole.Node, NodeId, 1, nonce, _verifier.Salt, _verifier.Iterations);
        byte[] challengeFrame = MessageCodec.Encode(challenge);
        _challenges[connection] = challengeFrame;
        return challenge;
    }

    private IWireMessage Proof(IRpcConnection connection, HelloProofMessage proof)
    {
        byte[] transcript = HandshakeAuth.ComputeTranscript(_hellos[connection], _challenges[connection]);
        if (!HandshakeAuth.VerifyClientProof(_verifier, transcript, proof.Proof.Span))
            return new HelloResultMessage(false, DfsErrorCode.AuthenticationFailed, "bad proof", string.Empty, DfsPermissions.None, null, ReadOnlyMemory<byte>.Empty);
        return new HelloResultMessage(true, DfsErrorCode.None, string.Empty, Principal, DfsPermissions.ClusterAdmin, Capabilities,
            HandshakeAuth.ComputeServerMac(_verifier, transcript));
    }

    private static DfsServerCapabilities Capabilities => new() { InlineThreshold = InlineThreshold, ChunkSize = 64 * 1024, MaxObjectSize = 64 * 1024 };

    private IWireMessage Dispatch(IWireMessage message)
    {
        lock (_sync)
        {
            if (Intercept?.Invoke(message) is { } intercepted)
                return intercepted;
            IWireMessage reply;
            try
            {
                reply = DispatchCore(message);
            }
            catch (DfsException ex)
            {
                reply = new ErrorReplyMessage(ex.ErrorCode, ex.Message, null, ex.IsRetryable);
            }

            AfterDispatch?.Invoke(message, reply);
            return reply;
        }
    }

    private IWireMessage DispatchCore(IWireMessage message)
    {
        if (message is IVolumeRequest volumeRequest)
            CheckHeader(message.Type, volumeRequest.Header);
        return message switch
        {
            GetClusterMapMessage => new ClusterMapResultMessage(1, Map()),
            GetVolumeRouteMessage route => route.Volume == VolumeName
                ? new VolumeRouteResultMessage(Volume(), NodeId, Endpoint, 1, Token(_index))
                : throw new DfsException(DfsErrorCode.VolumeNotFound, $"No volume '{route.Volume}'."),
            OpenSessionMessage => OpenSession(),
            ReserveOperationIdsMessage reserve => Reserve(reserve),
            KeepAliveMessage keep => new KeepAliveResultMessage([], [], !_sessions.TryGetValue(keep.Header.Session, out SessionState? live) || live.Expired,
                LeaseDuration, DateTimeOffset.UtcNow),
            QueryOperationMessage query => Query(query),
            PutObjectMessage put => Put(put),
            GetObjectMessage get => Get(get),
            CreateStagingMessage staging => staging.Renew.IsEmpty
                ? Mutate(staging.Header, staging, () => CreateStaging(staging))
                : CreateStaging(staging),
            OpenHandleMessage open => Mutate(open.Header, open, () => Open(open)),
            CloseHandleMessage close => Close(close),
            ReadRangeMessage read => Read(read),
            GetInfoMessage info => GetInfo(info),
            CommitFileMessage commit => Mutate(commit.Header, commit, () => Commit(commit)),
            WriteAllMessage write => Mutate(write.Header, write, () => WriteAll(write)),
            AppendMessage append => Mutate(append.Header, append, () => Append(append)),
            DeleteMessage delete => Mutate(delete.Header, delete, () => Delete(delete)),
            CreateDirectoryMessage create => Mutate(create.Header, create, () => CreateDirectory(create)),
            ListDirectoryMessage list => List(list),
            TxBeginMessage begin => Mutate(begin.Header, begin, () => TxBegin(begin)),
            TxCommitMessage commit => Mutate(commit.Header, commit, () => TxCommit(commit)),
            TxAbortMessage abort => TxAbort(abort),
            CloseSnapshotMessage => new CloseSnapshotResultMessage(true),
            _ => throw new DfsException(DfsErrorCode.Unsupported, $"The fake cluster does not serve {message.Type}."),
        };
    }

    /// <summary>The volume binding check of every volume request (PROTOCOL.md §4.1) and watermark bookkeeping.</summary>
    private void CheckHeader(MessageType type, RequestHeader header)
    {
        Headers.Enqueue((type, header));
        if ((header.ExpectedVolumeId != default && header.ExpectedVolumeId != VolumeId) || (header.ExpectedVolumeEpoch != 0 && header.ExpectedVolumeEpoch != Epoch))
            throw new DfsException(DfsErrorCode.EpochMismatch, $"Volume '{VolumeName}' is now {VolumeId} epoch {Epoch}.");
        if (header.Session != 0 && _sessions.TryGetValue(header.Session, out SessionState? session) && header.AckWatermark > session.MaxWatermark)
            session.MaxWatermark = header.AckWatermark;
    }

    private ClusterMapData Map()
    {
        var labels = new DfsTopologyLabels("r1", "z1", "rack1", "host1", "dev1");
        var node = new NodeEntry(NodeId, NodeName, labels, [Endpoint], WireNodeStatus.Active, 1, false, true, true, true, 1L << 40, 0);
        return new ClusterMapData(ClusterId, "fake", 1, 1, [node], [Volume()], NodeId, Capabilities);
    }

    private VolumeEntry Volume() =>
        new(VolumeId, VolumeName, Epoch, WireVolumeState.Active, [NodeId], [], NodeId, new DfsMetadataPolicy { Voters = 1 }, SingleCopy,
            new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal), InlineThreshold, new DfsVolumeLimits());

    private CommitToken Token(ulong index) => new(ClusterId, VolumeId, Epoch, index);

    // ------------------------------------------------------------------------------------------------ sessions

    private OpenSessionResultMessage OpenSession()
    {
        ulong id = _nextSession++;
        _sessions[id] = new SessionState();
        return new OpenSessionResultMessage(id, 1, LeaseDuration, LeaseDuration * 3, DateTimeOffset.UtcNow, 1000, TimeSpan.FromMinutes(5),
            Token(++_index));
    }

    private ReserveOperationIdsResultMessage Reserve(ReserveOperationIdsMessage reserve)
    {
        SessionState session = RequireSession(reserve.Header.Session);
        ulong first = session.Highest + 1;
        session.Highest += (ulong)reserve.Count;
        return new ReserveOperationIdsResultMessage(reserve.Header.Session, first, reserve.Count);
    }

    private SessionState RequireSession(ulong id) =>
        _sessions.TryGetValue(id, out SessionState? session) && !session.Expired
            ? session
            : throw new DfsException(DfsErrorCode.LeaseLost, $"Session {id} expired.");

    /// <summary>
    /// The operation gate: a stored outcome is replayed; an id of an expired (or unknown) session is OperationExpired and
    /// never executed; otherwise the request executes once.
    /// </summary>
    private IWireMessage Mutate(RequestHeader header, IWireMessage request, Func<IWireMessage> execute)
    {
        DfsOperationId op = header.Operation;
        if (op.IsEmpty)
            return execute();
        if (!_sessions.TryGetValue(op.Session, out SessionState? session) || session.Expired)
            throw new DfsException(DfsErrorCode.OperationExpired, $"Operation {op} belongs to an expired session.");
        if (session.Outcomes.TryGetValue(op.Sequence, out Outcome? stored))
        {
            if (stored.Kind != request.Type && !stored.Fenced)
                throw new DfsException(DfsErrorCode.OperationIdMismatch, $"Operation {op} was used for another request.");
            return MessageCodec.Decode(stored.Frame);
        }

        IWireMessage reply;
        try
        {
            reply = execute();
            Applied.AddOrUpdate(request.Type, 1, (_, n) => n + 1);
        }
        catch (DfsException ex)
        {
            reply = new ErrorReplyMessage(ex.ErrorCode, ex.Message, op, ex.IsRetryable);
        }

        session.Outcomes[op.Sequence] = new Outcome(request.Type, MessageCodec.Encode(reply), reply is not ErrorReplyMessage, false);
        session.Highest = Math.Max(session.Highest, op.Sequence);
        return reply;
    }

    private QueryOperationResultMessage Query(QueryOperationMessage query)
    {
        DfsOperationId target = query.Target;
        if (query.IsTransaction)
            return QueryTransaction(query);
        if (!_sessions.TryGetValue(target.Session, out SessionState? session))
            return new QueryOperationResultMessage(target, WireOutcomeStatus.NotFound, CommitToken.None, DfsErrorCode.None, null, DfsOperationKind.Unknown, default);
        if (session.Outcomes.TryGetValue(target.Sequence, out Outcome? outcome))
        {
            if (outcome.Success)
            {
                return new QueryOperationResultMessage(target, WireOutcomeStatus.Committed, Token(_index), DfsErrorCode.None, null, DfsOperationKind.Unknown,
                    outcome.Frame);
            }

            var error = (ErrorReplyMessage)MessageCodec.Decode(outcome.Frame);
            return new QueryOperationResultMessage(target, WireOutcomeStatus.Aborted, CommitToken.None, error.Code, error.Message, DfsOperationKind.Unknown,
                default);
        }

        if (!query.Resolve)
            return new QueryOperationResultMessage(target, WireOutcomeStatus.NotFound, CommitToken.None, DfsErrorCode.None, null, DfsOperationKind.Unknown, default);
        var fence = new ErrorReplyMessage(DfsErrorCode.Cancelled, "fenced", target);
        session.Outcomes[target.Sequence] = new Outcome(MessageType.ErrorReply, MessageCodec.Encode(fence), false, true);
        return new QueryOperationResultMessage(target, WireOutcomeStatus.Aborted, CommitToken.None, DfsErrorCode.Cancelled, "fenced", DfsOperationKind.Unknown,
            default);
    }

    // ------------------------------------------------------------------------------------------------ transactions

    private TxBeginResultMessage TxBegin(TxBeginMessage begin)
    {
        RequireSession(begin.Header.Session);
        var id = new DfsTransactionId(begin.Header.Operation);
        var staging = new StagingId(_nextStaging++);
        _staging[staging.Value] = new StagingState(begin.Header.Session, 0);
        _transactions[id] = new TxState { Staging = staging.Value };
        return new TxBeginResultMessage(id, Token(++_index), staging, new byte[] { 4, 5, 6 }, DateTimeOffset.UtcNow.AddHours(1), DateTimeOffset.UtcNow.AddHours(1),
            1, new DfsVolumeLimits());
    }

    private TxCommitResultMessage TxCommit(TxCommitMessage commit)
    {
        TxState tx = RequireTransaction(commit.Header);
        if (tx.State != "Active")
            throw new DfsException(DfsErrorCode.TransactionAborted, "The transaction is not active.");
        tx.State = "Committed";
        tx.Token = Token(++_index);
        CloseStaging(tx.Staging);
        return new TxCommitResultMessage(tx.Token, null);
    }

    private TxAbortResultMessage TxAbort(TxAbortMessage abort)
    {
        TxState tx = RequireTransaction(abort.Header);
        if (tx.State == "Committed")
            return new TxAbortResultMessage(WireOutcomeStatus.Committed, tx.Token);
        tx.State = "Aborted";
        CloseStaging(tx.Staging);
        return new TxAbortResultMessage(WireOutcomeStatus.Aborted, Token(++_index));
    }

    private QueryOperationResultMessage QueryTransaction(QueryOperationMessage query)
    {
        DfsOperationId target = query.Target;
        if (!_transactions.TryGetValue(new DfsTransactionId(target), out TxState? tx))
        {
            return new QueryOperationResultMessage(target, query.Resolve ? WireOutcomeStatus.Aborted : WireOutcomeStatus.NotFound, CommitToken.None,
                query.Resolve ? DfsErrorCode.Cancelled : DfsErrorCode.None, null, DfsOperationKind.TxCommit, default);
        }

        if (tx.State == "Active" && query.Resolve)
        {
            tx.State = "Aborted";
            CloseStaging(tx.Staging);
        }

        return tx.State switch
        {
            "Committed" => new QueryOperationResultMessage(target, WireOutcomeStatus.Committed, tx.Token, DfsErrorCode.None, null, DfsOperationKind.TxCommit,
                MessageCodec.Encode(new TxCommitResultMessage(tx.Token, null))),
            "Aborted" => new QueryOperationResultMessage(target, WireOutcomeStatus.Aborted, CommitToken.None, DfsErrorCode.TransactionAborted, "aborted",
                DfsOperationKind.TxCommit, default),
            _ => new QueryOperationResultMessage(target, WireOutcomeStatus.Pending, CommitToken.None, DfsErrorCode.None, null, DfsOperationKind.TxCommit, default),
        };
    }

    private TxState RequireTransaction(RequestHeader header) =>
        header.Transaction is { } id && _transactions.TryGetValue(id, out TxState? tx)
            ? tx
            : throw new DfsException(DfsErrorCode.TransactionAborted, "Unknown transaction.");

    private void CloseStaging(ulong staging)
    {
        if (_staging.TryGetValue(staging, out StagingState? state))
            state.Closed = true;
    }

    // ------------------------------------------------------------------------------------------------ data plane

    private CreateStagingResultMessage CreateStaging(CreateStagingMessage message)
    {
        if (message.Renew.Value != 0)
        {
            if (!_staging.TryGetValue(message.Renew.Value, out StagingState? existing) || existing.Closed)
                throw new DfsException(DfsErrorCode.LeaseLost, $"Staging session {message.Renew} is closed.");
            return new CreateStagingResultMessage(message.Renew, new byte[] { 1, 2, 3 }, DateTimeOffset.UtcNow.AddHours(1), 1, CommitToken.None);
        }

        RequireSession(message.Header.Session);
        if (!message.Handle.IsEmpty && !_handles.ContainsKey(message.Handle.Value))
            throw new DfsException(DfsErrorCode.LeaseLost, $"Handle {message.Handle} is not active.");
        var id = new StagingId(_nextStaging++);
        _staging[id.Value] = new StagingState(message.Header.Session, message.Handle.Value);
        return new CreateStagingResultMessage(id, new byte[] { 1, 2, 3 }, DateTimeOffset.UtcNow.AddHours(1), 1, Token(++_index));
    }

    /// <summary>Publications referring to a closed staging session are rejected (nothing proposed).</summary>
    private void RequireOpenStaging(StagingId staging)
    {
        if (!staging.IsEmpty && (!_staging.TryGetValue(staging.Value, out StagingState? state) || state.Closed))
            throw new DfsException(DfsErrorCode.LeaseLost, $"Staging session {staging} is closed; its uploads can no longer be published.");
    }

    private PutObjectResultMessage Put(PutObjectMessage put)
    {
        if (ObjectChecksum.Compute(put.Data.Span) != put.Checksum || put.Data.Length != put.Length)
            throw new DfsException(DfsErrorCode.Integrity, "Checksum mismatch.");
        if (PutsBeforeFailure is { } remaining)
        {
            if (remaining <= 0)
                throw new DfsException(DfsErrorCode.CapacityExhausted, "The storage node is full.");
            PutsBeforeFailure = remaining - 1;
        }

        bool present = _objects.ContainsKey(put.Object);
        _objects[put.Object] = (put.Data.ToArray(), put.Checksum, put.Staging);
        var receipt = new ObjectReceipt(put.Object, put.Volume, put.Staging, put.Checksum, put.Length, ObjectEncodings.Plain, NodeId, 1, put.PlacementEpoch,
            DateTimeOffset.UtcNow.Ticks, new byte[32]);
        return new PutObjectResultMessage(receipt, present);
    }

    private GetObjectResultMessage Get(GetObjectMessage get)
    {
        if (_objectsLost || !_objects.TryGetValue(get.Object, out (byte[] Data, UInt128 Checksum, StagingId Staging) stored))
            throw new DfsException(DfsErrorCode.Integrity, $"Object {get.Object} is missing.");
        long length = stored.Data.Length;
        if (get.Offset >= length)
            return new GetObjectResultMessage(length, stored.Checksum, ObjectEncodings.Plain, get.Offset, ReadOnlyMemory<byte>.Empty, 0, []);
        long end = Math.Min(length, get.Offset + get.Length);
        long start = get.Offset / UnitSize * UnitSize;
        long alignedEnd = Math.Min(length, (end + UnitSize - 1) / UnitSize * UnitSize);
        byte[] data = stored.Data.AsSpan((int)start, (int)(alignedEnd - start)).ToArray();
        var checksums = new uint[(data.Length + UnitSize - 1) / UnitSize];
        for (int i = 0; i < checksums.Length; i++)
            checksums[i] = ObjectChecksum.Crc32C(data.AsSpan(i * UnitSize, Math.Min(UnitSize, data.Length - (i * UnitSize))));
        if (CorruptReads > 0)
        {
            CorruptReads--;
            data[0] ^= 0xFF;
        }

        return new GetObjectResultMessage(length, stored.Checksum, ObjectEncodings.Plain, start, data, UnitSize, checksums);
    }

    /// <summary>Number of following object reads returned corrupted (first byte flipped).</summary>
    public int CorruptReads { get; set; }

    /// <summary>Removes every stored object (simulates data loss on all replicas).</summary>
    public void LoseAllObjects()
    {
        lock (_sync)
        {
            _objects.Clear();
            _objectsLost = true;
        }
    }

    private bool _objectsLost;

    // ------------------------------------------------------------------------------------------------ files

    private OpenHandleResultMessage Open(OpenHandleMessage open)
    {
        bool exists = _paths.TryGetValue(open.Path, out Inode? inode) && !inode.IsDirectory;
        if (inode is { IsDirectory: true })
            throw new DfsException(DfsErrorCode.IsADirectory, "Directory.");
        bool created = false;
        bool truncated = false;
        switch (open.Mode)
        {
            case FileMode.CreateNew when exists:
                throw new DfsException(DfsErrorCode.AlreadyExists, "Exists.");
            case FileMode.Open or FileMode.Truncate when !exists:
                throw new DfsException(DfsErrorCode.FileNotFound, "Missing.");
        }

        if (!exists)
        {
            inode = CreateFile(open.Path, open.StoragePolicy);
            created = true;
        }
        else if (open.Mode is FileMode.Create or FileMode.Truncate)
        {
            inode!.Content = [];
            inode.Revision = ++_index;
            truncated = true;
        }

        RequireSession(open.Header.Session);
        bool writable = (open.Access & FileAccess.Write) != 0;
        if (writable && _handles.Values.Any(h => h.Inode == inode && h.Writable))
            throw new DfsException(DfsErrorCode.WriterConflict, "Another writer.");
        ulong generation = writable ? ++inode!.WriterGeneration : 0;
        var handle = new HandleState(inode!, writable, open.VersionPinned ? inode!.Content.ToArray() : null, open.VersionPinned ? inode!.Revision : 0,
            generation, open.Header.Session);
        ulong id = _nextHandle++;
        _handles[id] = handle;
        return new OpenHandleResultMessage(new HandleId(id), Describe(open.Path, inode!), generation, inode!.Revision, TimeSpan.FromSeconds(30), created,
            truncated, Token(++_index));
    }

    private Inode CreateFile(string path, string? policy)
    {
        string parent = DfsPath.GetParent(DfsStreamPath.GetFilePath(path));
        if (!_paths.TryGetValue(parent, out Inode? directory) || !directory.IsDirectory)
            throw new DfsException(DfsErrorCode.DirectoryNotFound, $"Parent of {path} is missing.");
        var inode = new Inode(_nextInode++, isDirectory: false, ++_index) { Policy = policy ?? string.Empty };
        _paths[path] = inode;
        return inode;
    }

    private CloseHandleResultMessage Close(CloseHandleMessage close)
    {
        bool removed = _handles.Remove(close.Handle.Value);
        return new CloseHandleResultMessage(removed, removed ? Token(++_index) : CommitToken.None);
    }

    /// <summary>A handle of the caller: LeaseLost once expired, InvalidHandle when foreign or never issued.</summary>
    private HandleState RequireHandle(RequestHeader header, HandleId id)
    {
        if (_expiredHandles.Contains(id.Value))
            throw new DfsException(DfsErrorCode.LeaseLost, $"Handle {id} is no longer active (closed or its lease expired).");
        if (!_handles.TryGetValue(id.Value, out HandleState? handle) || handle.Session != header.Session)
            throw new DfsException(DfsErrorCode.InvalidHandle, $"Handle {id} is not a handle of this session.");
        return handle;
    }

    private ReadRangeResultMessage Read(ReadRangeMessage read)
    {
        HandleState handle = RequireHandle(read.Header, read.Handle);
        byte[] view = handle.Pinned ?? handle.Inode.Content;
        ulong revision = handle.Pinned is not null ? handle.PinnedRevision : handle.Inode.Revision;
        long length = Math.Max(0, Math.Min(read.Length, Math.Min(MaxReadResult, view.Length - read.Offset)));
        if (length <= 0)
            return new ReadRangeResultMessage(revision, view.Length, read.Offset, 0, true, ReadOnlyMemory<byte>.Empty, [], default, default, Token(_index));
        if (view.Length <= InlineThreshold)
        {
            return new ReadRangeResultMessage(revision, view.Length, read.Offset, length, true, view.AsMemory((int)read.Offset, (int)length).ToArray(), [],
                default, default, Token(_index));
        }

        // Large files are served as one extent over a synthesized object holding the revision's bytes.
        var obj = new ObjectId(new UInt128(revision, (ulong)handle.Inode.Id));
        UInt128 checksum = ObjectChecksum.Compute(view);
        _objects[obj] = (view, checksum, default);
        var extent = new ExtentData(0, view.Length, obj, 0, view.Length, checksum, ObjectEncodings.Plain, 1, [NodeId]);
        return new ReadRangeResultMessage(revision, view.Length, read.Offset, length, false, ReadOnlyMemory<byte>.Empty, [extent], new byte[] { 9 },
            DateTimeOffset.UtcNow.AddHours(1), Token(_index));
    }

    /// <summary>Largest covered range of one ReadRange result (short results exercise the client's read loop).</summary>
    public long MaxReadResult { get; set; } = long.MaxValue;

    private GetInfoResultMessage GetInfo(GetInfoMessage info)
    {
        if (info.Handle.Value != 0)
        {
            HandleState handle = RequireHandle(info.Header, info.Handle);
            FileInfoData data = Describe(string.Empty, handle.Inode);
            if (handle.Pinned is not null)
                data = data with { Length = handle.Pinned.Length, Revision = handle.PinnedRevision };
            return new GetInfoResultMessage(true, data, Token(_index));
        }

        return _paths.TryGetValue(info.Path, out Inode? inode)
            ? new GetInfoResultMessage(true, Describe(info.Path, inode), Token(_index))
            : new GetInfoResultMessage(false, null, Token(_index));
    }

    private FileInfoData Describe(string path, Inode inode) =>
        new(path, inode.IsDirectory ? WireEntryKind.Directory : WireEntryKind.File, new InodeId(inode.Id), inode.Revision, inode.Content.Length,
            DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch, Principal, false, false, inode.Policy, inode.Content.Length <= InlineThreshold, false,
            inode.Content.Length <= InlineThreshold ? 0 : 1, inode.IsDirectory ? _paths.Keys.Count(k => !DfsPath.IsRoot(k) && k != path && DfsPath.GetParent(k) == path) : 0,
            inode.WriterGeneration);

    private CommitFileResultMessage Commit(CommitFileMessage commit)
    {
        if (!_handles.TryGetValue(commit.Handle.Value, out HandleState? handle) || !handle.Writable || handle.Generation != commit.Generation)
            throw new DfsException(DfsErrorCode.LeaseLost, "Stale handle.");
        RequireOpenStaging(commit.Staging);
        Inode inode = handle.Inode;
        byte[] content = inode.Content;
        if (commit.TruncateTo >= 0 && commit.TruncateTo < content.Length)
            content = content.AsSpan(0, (int)commit.TruncateTo).ToArray();
        content = Apply(content, commit.Changes, 0);
        content = Resize(content, commit.NewLength);
        inode.Content = content;
        inode.Revision = ++_index;
        LastChanges = commit.Changes;
        return new CommitFileResultMessage(new InodeId(inode.Id), inode.Revision, content.Length, Token(inode.Revision), null);
    }

    /// <summary>Changes of the most recent publication (representation assertions).</summary>
    public IReadOnlyList<RangeChange> LastChanges { get; private set; } = [];

    private WriteAllResultMessage WriteAll(WriteAllMessage write)
    {
        RequireOpenStaging(write.Staging);
        bool created = false;
        if (!_paths.TryGetValue(write.Path, out Inode? inode))
        {
            inode = CreateFile(write.Path, write.StoragePolicy);
            created = true;
        }

        inode.Content = Resize(Apply([], write.Changes, 0), write.Length);
        inode.Revision = ++_index;
        LastChanges = write.Changes;
        return new WriteAllResultMessage(new InodeId(inode.Id), inode.Revision, inode.Content.Length, created, Token(inode.Revision), null);
    }

    private AppendResultMessage Append(AppendMessage append)
    {
        RequireOpenStaging(append.Staging);
        bool created = false;
        if (!_paths.TryGetValue(append.Path, out Inode? inode))
        {
            inode = CreateFile(append.Path, append.StoragePolicy);
            created = true;
        }

        long old = inode.Content.Length;
        if (old + append.AppendLength > InlineThreshold && append.Changes.Any(c => c.Kind == RangeChangeKind.Inline))
            throw new DfsException(DfsErrorCode.InvalidArgument, "Inline change for an extent-based result.");
        inode.Content = Resize(Apply(inode.Content, append.Changes, old), old + append.AppendLength);
        inode.Revision = ++_index;
        LastChanges = append.Changes;
        return new AppendResultMessage(new InodeId(inode.Id), inode.Revision, old, inode.Content.Length, created, Token(inode.Revision), null);
    }

    private DeleteResultMessage Delete(DeleteMessage delete)
    {
        if (!_paths.TryGetValue(delete.Path, out Inode? inode))
        {
            return delete.ExpectDirectory
                ? throw new DfsException(DfsErrorCode.DirectoryNotFound, "Missing.")
                : new DeleteResultMessage(false, default, CommitToken.None, null);
        }

        foreach (string path in _paths.Keys.Where(p => p == delete.Path || DfsPath.IsAncestorOrSelf(delete.Path, p)).ToArray())
            _paths.Remove(path);
        return new DeleteResultMessage(true, new InodeId(inode.Id), Token(++_index), null);
    }

    private CreateDirectoryResultMessage CreateDirectory(CreateDirectoryMessage create)
    {
        string path = create.Path;
        bool created = false;
        var chain = new List<string>();
        for (string p = path; !DfsPath.IsRoot(p); p = DfsPath.GetParent(p))
            chain.Insert(0, p);
        foreach (string p in chain)
        {
            if (_paths.TryGetValue(p, out Inode? existing))
            {
                if (!existing.IsDirectory)
                    throw new DfsException(DfsErrorCode.NotADirectory, $"{p} is a file.");
                continue;
            }

            _paths[p] = new Inode(_nextInode++, isDirectory: true, ++_index);
            created = true;
        }

        return new CreateDirectoryResultMessage(Describe(path, _paths[path]), created, Token(_index), null);
    }

    private ListDirectoryResultMessage List(ListDirectoryMessage list)
    {
        if (!_paths.TryGetValue(list.Path, out Inode? directory) || !directory.IsDirectory)
            throw new DfsException(DfsErrorCode.DirectoryNotFound, "Missing.");
        string[] names = _paths.Keys.Where(k => !DfsPath.IsRoot(k) && k != list.Path && DfsPath.GetParent(k) == list.Path).Select(DfsPath.GetName).Order(StringComparer.Ordinal)
            .ToArray();
        int start = list.Continuation is null ? 0 : int.Parse(list.Continuation, System.Globalization.CultureInfo.InvariantCulture);
        string[] page = names.Skip(start).Take(list.PageSize).ToArray();
        string? next = start + page.Length < names.Length ? (start + page.Length).ToString(System.Globalization.CultureInfo.InvariantCulture) : null;
        DirectoryEntryData[] entries = page.Select(n =>
        {
            Inode i = _paths[DfsPath.Combine(list.Path, n)];
            return new DirectoryEntryData(n, i.IsDirectory ? WireEntryKind.Directory : WireEntryKind.File, new InodeId(i.Id), i.Revision, i.Content.Length,
                DateTimeOffset.UnixEpoch);
        }).ToArray();
        return new ListDirectoryResultMessage(entries, next, Token(_index), next is null ? 0UL : 77UL, new InodeId(directory.Id), directory.Revision);
    }

    private byte[] Apply(byte[] content, IReadOnlyList<RangeChange> changes, long relativeTo)
    {
        foreach (RangeChange change in changes)
        {
            long offset = relativeTo + change.Offset;
            content = Resize(content, Math.Max(content.Length, offset + change.Length));
            switch (change.Kind)
            {
                case RangeChangeKind.Inline:
                    change.InlineData.Span.CopyTo(content.AsSpan((int)offset));
                    break;
                case RangeChangeKind.Zero:
                    content.AsSpan((int)offset, (int)change.Length).Clear();
                    break;
                case RangeChangeKind.Data:
                    byte[] data = _objects.TryGetValue(change.Object, out var stored) ? stored.Data : throw new DfsException(DfsErrorCode.PlacementUnavailable, "No receipt.");
                    data.AsSpan((int)change.ObjectOffset, (int)change.Length).CopyTo(content.AsSpan((int)offset));
                    break;
            }
        }

        return content;
    }

    private static byte[] Resize(byte[] content, long length)
    {
        if (content.Length == length)
            return content;
        var resized = new byte[length];
        content.AsSpan(0, (int)Math.Min(content.Length, length)).CopyTo(resized);
        return resized;
    }

    private sealed class Inode(ulong id, bool isDirectory, ulong revision)
    {
        public ulong Id { get; } = id;

        public bool IsDirectory { get; } = isDirectory;

        public ulong Revision { get; set; } = revision;

        public byte[] Content { get; set; } = [];

        public ulong WriterGeneration { get; set; }

        public string Policy { get; set; } = string.Empty;
    }

    private sealed record HandleState(Inode Inode, bool Writable, byte[]? Pinned, ulong PinnedRevision, ulong Generation, ulong Session);

    private sealed class SessionState
    {
        public ulong Highest { get; set; }

        public bool Expired { get; set; }

        public ulong MaxWatermark { get; set; }

        public Dictionary<ulong, Outcome> Outcomes { get; } = [];
    }

    private sealed class StagingState(ulong session, ulong handle)
    {
        public ulong Session { get; } = session;

        public ulong Handle { get; } = handle;

        public bool Closed { get; set; }
    }

    private sealed class TxState
    {
        public string State { get; set; } = "Active";

        public CommitToken Token { get; set; }

        public ulong Staging { get; init; }
    }

    private sealed record Outcome(MessageType Kind, byte[] Frame, bool Success, bool Fenced);
}

/// <summary>A link policy whose rules drop (or black-hole) the next N messages of a type in a direction.</summary>
internal sealed class ScriptedLinkPolicy : ILinkPolicy
{
    private readonly System.Threading.Lock _sync = new();
    private readonly List<Rule> _rules = [];

    /// <summary>Drops the next <paramref name="count"/> messages of <paramref name="type"/> travelling in <paramref name="direction"/>.</summary>
    public void Drop(MessageType type, LinkDirection direction, int count = 1)
    {
        lock (_sync)
            _rules.Add(new Rule(type, direction, count));
    }

    /// <summary>Removes every rule.</summary>
    public void Clear()
    {
        lock (_sync)
            _rules.Clear();
    }

    /// <summary>Messages dropped so far.</summary>
    public int Dropped { get; private set; }

    public LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix)
    {
        if (!MessageCodec.TryReadType(payloadPrefix, out MessageType type))
            return LinkDecision.Deliver;
        lock (_sync)
        {
            foreach (Rule rule in _rules)
            {
                if (rule.Remaining > 0 && rule.Type == type && rule.Direction == direction)
                {
                    rule.Remaining--;
                    Dropped++;
                    return LinkDecision.Drop;
                }
            }
        }

        return LinkDecision.Deliver;
    }

    public bool CanConnect(string fromName, string toName) => true;

    private sealed class Rule(MessageType type, LinkDirection direction, int remaining)
    {
        public MessageType Type { get; } = type;

        public LinkDirection Direction { get; } = direction;

        public int Remaining { get; set; } = remaining;
    }
}
