using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Admin;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Data;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Security;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Loop-side composition of a live node: start-up (superblock, incarnation bump before serving, keys, object store,
/// control group, transport), seed join, incarnation registration, background jobs, graceful stop and crash.
/// </summary>
internal sealed class NodeRuntime
{
    private const string SuperblockFile = Superblock.DefaultFileName;

    private IRpcTransport? _transport;
    private ControlService _controlService = null!;
    private int _killed;
    private bool _joined;

    private NodeRuntime(NodeServices services)
    {
        Services = services;
    }

    /// <summary>The node's services.</summary>
    public NodeServices Services { get; }

    /// <summary>The node's transport (null before start-up created it).</summary>
    public IRpcTransport? Transport => _transport;

    /// <summary>Starts a node; must run on <paramref name="loop"/>.</summary>
    public static async Task<NodeRuntime> StartAsync(NodeOptions options, NodeEnvironment environment, INodeLoop loop, CancellationToken ct)
    {
        var runtime = new NodeRuntime(new NodeServices(options, environment, loop));
        try
        {
            await runtime.StartCoreAsync(ct);
            return runtime;
        }
        catch
        {
            await runtime.AbortStartAsync();
            throw;
        }
    }

    private async Task StartCoreAsync(CancellationToken ct)
    {
        NodeServices s = Services;
        NodeOptions options = s.Options;
        IStorageMedium medium = s.Env.Medium;
        ValidateOptions(options);
        s.Keys = new KeyHierarchy(options.ClusterSecret.Span);
        Span<byte> boot = stackalloc byte[16];
        s.Random.Fill(boot);
        s.BootId = System.Buffers.Binary.BinaryPrimitives.ReadUInt128LittleEndian(boot);
        s.VolumeStateMachines = s.Env.VolumeStateMachines ?? VolumeHost.ResolveDefaultFactory();

        s.Rpc = new RpcDispatcher(s);
        s.Peers = new PeerConnections(s);
        s.Raft = new RaftGroupHost(s);
        s.Control = new ControlHost(s);
        s.Volumes = new VolumeRegistry(s);
        s.Membership = new MembershipManager(s);
        s.Admin = new AdminService(s);
        s.Jobs = new BackgroundTasks(s);
        _controlService = new ControlService(s);
        RegisterHandlers();

        await medium.CreateDirectoryDurableAsync(options.DataDirectory, ct);
        string superblockPath = s.DataPath(SuperblockFile);
        Superblock? superblock = await Superblock.LoadAsync(medium, superblockPath, ct);
        IReadOnlyList<RaftEntry>? controlBootstrap = null;
        bool seedJoin = false;
        if (superblock is null)
        {
            if (options.Bootstrap is { } bootstrap)
            {
                BootstrapMember self = ControlBootstrap.Validate(bootstrap, options.Name);
                controlBootstrap = ControlBootstrap.BuildEntries(bootstrap, s.Keys);
                superblock = await Superblock.CreateAsync(medium, superblockPath, bootstrap.ClusterId, self.Id, options.Name, ct);
                s.Info("node", NodeServices.Inv($"Bootstrapping cluster {bootstrap.ClusterName} ({bootstrap.ClusterId}) as {self.Id}."));
            }
            else if (options.JoinSeeds.Count > 0)
            {
                seedJoin = true;
            }
            else
            {
                throw new InvalidOperationException(
                    $"Node {options.Name} has no durable state, no bootstrap description and no join seeds: it cannot start.");
            }
        }
        else
        {
            if (!string.Equals(superblock.NodeName, options.Name, StringComparison.Ordinal))
                throw new InvalidOperationException($"The data directory belongs to node '{superblock.NodeName}', not '{options.Name}'.");
            if (options.Bootstrap is { } bootstrap && bootstrap.ClusterId != superblock.ClusterId)
            {
                throw new DfsClusterIdentityException(
                    $"The data directory belongs to cluster {superblock.ClusterId}; the bootstrap describes cluster {bootstrap.ClusterId}.");
            }
        }

        // Listen before joining (the leader must be able to reach a joining node); every request is refused until serving.
        _transport = s.Env.TransportFactory.Create(options.Name, options.ListenEndpoint);
        s.Peers.Attach(_transport);
        await _transport.StartAsync(s.Rpc, ct);
        s.Endpoint = _transport.LocalEndpoint;

        if (seedJoin)
            superblock = await DiscoverAndCreateSuperblockAsync(superblockPath, ct);

        // The incarnation is durably incremented before the node serves anything (ARCHITECTURE §2.5, RCV-02).
        superblock = await Superblock.BumpIncarnationAsync(medium, superblockPath, ct);
        s.Superblock = superblock;
        s.NodeId = superblock!.NodeId;
        s.ClusterId = superblock.ClusterId;
        s.Incarnation = superblock.Incarnation;

        IRecordCipher? objectCipher = null;
        if (options.EncryptAtRest)
        {
            RecordCipher cipher = s.Keys.CreateObjectCipher(s.Random.Fork("cipher/objects"));
            s.Disposables.Add(cipher);
            objectCipher = cipher;
        }

        s.ObjectStore = await ObjectStore.OpenAsync(medium, s.DataPath(NodeServices.ObjectsDirectory),
            new ObjectStoreOptions(ReservedBytes: options.ReservedBytes, MaxObjectSize: options.Timings.MaxObjectSize, Cipher: objectCipher)
            {
                Mutations = options.Mutations,
                NodeName = options.Name,
            }, s.Faults, ct);
        s.Metrics.SetStored(s.ObjectStore.ObjectCount, s.ObjectStore.LiveBytes);
        s.Objects = new ObjectService(s, new Capabilities(s.Keys, () => s.ClusterId, s.Time));
        s.Objects.Register(s.Rpc);

        await s.Control.OpenAsync(controlBootstrap, ct);
        s.Volumes.Start();
        s.Raft.StartTicking();
        s.IsServing = true;
        s.Info("node", NodeServices.Inv($"Node {options.Name} ({s.NodeId}) serving at {s.Endpoint}, incarnation {s.Incarnation}."));
        StartJobs();

        if (seedJoin)
            await JoinAsync(ct);
    }

    private void RegisterHandlers()
    {
        NodeServices s = Services;
        _controlService.Register(s.Rpc);
        s.Admin.Register(s.Rpc);
        s.Rpc.Register<RaftBatchMessage>(HandleRaftBatchAsync, RpcAccess.Node | RpcAccess.JoiningNode);
        s.Rpc.Register<RaftSnapshotChunkMessage>(HandleSnapshotChunkAsync, RpcAccess.Node | RpcAccess.JoiningNode);
    }

    private ValueTask<IWireMessage> HandleRaftBatchAsync(RpcContext context, IWireMessage request)
    {
        var batch = (RaftBatchMessage)request;
        if (context.PeerNode != batch.From || context.Identity.Incarnation != batch.Incarnation)
            throw new UnauthorizedAccessException("The Raft batch does not come from the authenticated node incarnation.");
        return new ValueTask<IWireMessage>(Services.Raft.HandleBatch(batch, batch.From));
    }

    private async ValueTask<IWireMessage> HandleSnapshotChunkAsync(RpcContext context, IWireMessage request)
    {
        var chunk = (RaftSnapshotChunkMessage)request;
        if (context.PeerNode != chunk.From || context.Identity.Incarnation != chunk.Incarnation)
            throw new UnauthorizedAccessException("The snapshot chunk does not come from the authenticated node incarnation.");
        return await Services.Raft.HandleSnapshotChunkAsync(chunk, chunk.From, context.Cancellation);
    }

    private void StartJobs()
    {
        NodeServices s = Services;
        TimeSpan tick = s.Timings.Tick;
        s.Jobs.StartPeriodic("register-incarnation", tick * 4, RegisterIncarnationOnceAsync, tick);
        s.Jobs.StartPeriodic("membership", tick * 10, s.Membership.RunOnceAsync);
        s.Jobs.StartPeriodic("volume-membership", tick * 10, s.Volumes.ReconcileMembershipAsync);
        s.Jobs.StartPeriodic("volume-registry", tick * 40, () =>
        {
            s.Volumes.Schedule();
            return Task.CompletedTask;
        });
        s.Jobs.StartPeriodic("rejoin", tick * 40, RejoinIfNeededAsync, tick * 20);
    }

    // ------------------------------------------------------------------ seed join

    private async Task<Superblock> DiscoverAndCreateSuperblockAsync(string superblockPath, CancellationToken ct)
    {
        NodeServices s = Services;
        s.NodeId = NodeId.New(s.Random);
        var credentials = new NodeCredentials(Guid.Empty, s.NodeName, s.NodeId, 0, s.BootId, s.Keys.NodeAuthKey, ProtocolFeatures.Supported);
        Exception? last = null;
        foreach (string seed in s.Options.JoinSeeds)
        {
            try
            {
                (IRpcConnection connection, HandshakeOutcome outcome) = await s.Peers.ConnectUnpooledAsync(seed, credentials, ct);
                await connection.DisposeAsync();
                s.Info("node", NodeServices.Inv($"Seed {seed} belongs to cluster {outcome.Challenge.ClusterName} ({outcome.Challenge.ClusterId})."));
                return await Superblock.CreateAsync(s.Env.Medium, superblockPath, outcome.Challenge.ClusterId, s.NodeId, s.NodeName, ct);
            }
            catch (Exception ex) when (ex is DfsAuthenticationException or DfsClusterIdentityException
                || DfsErrors.ToCode(ex) is DfsErrorCode.AuthenticationFailed or DfsErrorCode.AccessDenied)
            {
                throw;
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException)
            {
                last = ex;
            }
        }

        throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"No join seed of {s.NodeName} could be reached: {last?.Message}", last);
    }

    private async Task JoinAsync(CancellationToken ct)
    {
        NodeServices s = Services;
        TimeSpan budget = TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(30).Ticks, (s.Timings.Tick * s.Timings.ElectionTicksMax * 100).Ticks));
        long start = s.Time.GetTimestamp();
        var candidates = new List<string>(s.Options.JoinSeeds);
        Exception? last = null;
        while (s.Time.GetElapsedTime(start) < budget && !s.IsStopping)
        {
            ct.ThrowIfCancellationRequested();
            if (await TryJoinOnceAsync(candidates, ct) is { } failure)
                last = failure;
            else
                return;
            await s.Jobs.DelayAsync(s.Timings.Tick * s.Timings.ElectionTicksMin);
        }

        throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"{s.NodeName} could not join the cluster: {last?.Message}", last);
    }

    /// <summary>One pass over the candidate endpoints; null when the join was accepted, else the last transient failure.</summary>
    private async Task<Exception?> TryJoinOnceAsync(List<string> candidates, CancellationToken ct)
    {
        NodeServices s = Services;
        var message = new JoinClusterMessage(s.NodeId, s.NodeName, s.Options.Labels, [s.Endpoint], s.Incarnation, s.Options.CapacityBytes,
            ProtocolInfo.CurrentVersion, ProtocolFeatures.Supported, s.BootId);
        Exception? last = null;
        for (int i = 0; i < candidates.Count; i++)
        {
            string endpoint = candidates[i];
            try
            {
                IWireMessage reply = await s.Peers.CallEndpointAsync(endpoint, message, ct: ct);
                if (reply is JoinClusterResultMessage { Accepted: true } accepted)
                {
                    if (accepted.Map is { } map)
                        s.Peers.LearnEndpoints(map);
                    _joined = true;
                    s.Info("node", NodeServices.Inv($"{s.NodeName} joined the cluster through {endpoint}."));
                    return null;
                }

                if (reply is JoinClusterResultMessage { LeaderEndpoint: { } hint } && !candidates.Contains(hint, StringComparer.Ordinal))
                    candidates.Insert(i + 1, hint);
            }
            catch (DfsUnavailableException ex) when (ex.ErrorCode == DfsErrorCode.NotLeader)
            {
                last = ex;
                if (ex.LeaderHint is { } hint && !candidates.Contains(hint, StringComparer.Ordinal))
                    candidates.Insert(i + 1, hint);
            }
            catch (Exception ex) when (DfsErrors.ToCode(ex) is DfsErrorCode.AuthenticationFailed or DfsErrorCode.AccessDenied or DfsErrorCode.ClusterMismatch
                or DfsErrorCode.StaleIncarnation)
            {
                throw;
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException)
            {
                last = ex;
            }
        }

        return last ?? new DfsUnavailableException(DfsErrorCode.Unavailable, "No join candidate accepted the join.");
    }

    /// <summary>A node whose (empty or lagging) control view does not list it re-runs the idempotent join.</summary>
    private async Task RejoinIfNeededAsync()
    {
        NodeServices s = Services;
        if (_joined || s.Options.JoinSeeds.Count == 0 || s.Control.Self is not null || s.Control.Group?.Leader is not null)
            return;
        var candidates = new List<string>(s.Options.JoinSeeds);
        try
        {
            await TryJoinOnceAsync(candidates, CancellationToken.None);
        }
        catch (Exception ex) when (ex is DfsException or UnauthorizedAccessException)
        {
            s.Warn("node", NodeServices.Inv($"Re-joining failed: {ex.Message}"));
        }
    }

    // ------------------------------------------------------------------ incarnation registration

    private async Task RegisterIncarnationOnceAsync()
    {
        NodeServices s = Services;
        ControlHost control = s.Control;
        if (control.IsSelfRegistered || !control.State.IsBootstrapped || control.Self is not { IsLive: true } self)
            return;
        await s.HitAsync(FaultStage.F21, FaultSites.Reconfiguration, operation: DfsOperationKind.MembershipChange, detail: "register-incarnation");
        if (control.IsLeader)
        {
            await _controlService.RegisterAsync(s.NodeId, s.Incarnation, s.BootId, s.Options.Labels, [s.Endpoint], s.Options.CapacityBytes, CancellationToken.None);
        }
        else if (control.Leader is { } leader && leader != s.NodeId)
        {
            var message = new RegisterIncarnationMessage(s.NodeId, s.Incarnation, s.NodeName, s.Options.Labels, [s.Endpoint], s.Options.CapacityBytes, s.BootId);
            await s.Peers.CallAsync(leader, message);
        }
        else
        {
            return;
        }

        if (control.Group is { } group && !control.IsSelfRegistered)
        {
            Task applied = WaitRegisteredAsync(group);
            await Task.WhenAny(applied, Task.Delay(s.Timings.Tick * s.Timings.ElectionTicksMax, s.Time));
        }

        if (control.IsSelfRegistered)
            s.Info("node", NodeServices.Inv($"Incarnation {s.Incarnation} of {s.NodeName} registered (was {self.Incarnation})."));
    }

    private async Task WaitRegisteredAsync(GroupRuntime group)
    {
        while (!Services.Control.IsSelfRegistered && !group.IsClosed)
            await group.WaitForAppliedAsync(group.AppliedIndex + 1);
    }

    // ------------------------------------------------------------------ stop and crash

    /// <summary>Graceful stop: refuse new requests, drain in-flight ones, close groups and stores (flushing), close transport.</summary>
    public async Task StopAsync(TimeSpan drainTimeout)
    {
        NodeServices s = Services;
        if (s.IsStopping)
            return;
        s.IsStopping = true;
        s.Jobs?.Stop();
        s.Raft?.Stop();
        if (s.Rpc is { } rpc)
            await Task.WhenAny(rpc.WaitForDrainAsync(), Task.Delay(drainTimeout, s.Time));
        if (s.Volumes is not null)
            await s.Volumes.CloseAllAsync();
        if (s.Raft is not null)
            await s.Raft.CloseAllAsync();
        if (s.ObjectStore is not null)
            await s.ObjectStore.DisposeAsync();
        if (s.Peers is not null)
            await s.Peers.CloseAllAsync();
        if (s.Rpc is not null)
            await s.Rpc.CloseAllAsync();
        if (_transport is not null)
            await _transport.DisposeAsync();
        DisposeResources();
        s.Info("node", NodeServices.Inv($"Node {s.NodeName} stopped."));
    }

    /// <summary>Crash: the transport is torn down without any flush (called after the loop was stopped).</summary>
    public void Kill()
    {
        if (Interlocked.Exchange(ref _killed, 1) != 0)
            return;
        Services.IsStopping = true;
        if (_transport is { } transport)
            _ = transport.DisposeAsync().AsTask();
    }

    private async Task AbortStartAsync()
    {
        NodeServices s = Services;
        s.IsStopping = true;
        s.Jobs?.Stop();
        s.Raft?.Stop();
        try
        {
            if (s.Raft is not null)
                await s.Raft.CloseAllAsync();
            if (s.ObjectStore is not null)
                await s.ObjectStore.DisposeAsync();
            if (s.Peers is not null)
                await s.Peers.CloseAllAsync();
        }
        catch (Exception ex) when (ex is IOException or DfsException or ObjectDisposedException)
        {
            s.Warn("node", "Cleanup after a failed start failed: " + ex.Message);
        }
        finally
        {
            if (_transport is not null)
                await _transport.DisposeAsync();
            DisposeResources();
        }
    }

    private void DisposeResources()
    {
        foreach (IDisposable disposable in Services.Disposables)
            disposable.Dispose();
        Services.Disposables.Clear();
        Services.Metrics.Dispose();
    }

    private static void ValidateOptions(NodeOptions options)
    {
        ArgumentException.ThrowIfNullOrEmpty(options.Name);
        ArgumentNullException.ThrowIfNull(options.Labels);
        ArgumentException.ThrowIfNullOrEmpty(options.ListenEndpoint);
        ArgumentNullException.ThrowIfNull(options.DataDirectory);
        if (options.ClusterSecret.IsEmpty)
            throw new ArgumentException("The cluster secret must not be empty.", nameof(options));
        ArgumentNullException.ThrowIfNull(options.Timings);
        options.Timings.Validate();
        ArgumentNullException.ThrowIfNull(options.Mutations);
        ArgumentNullException.ThrowIfNull(options.JoinSeeds);
        if (options.ReservedBytes < 0)
            throw new ArgumentException("ReservedBytes must not be negative.", nameof(options));
    }
}
