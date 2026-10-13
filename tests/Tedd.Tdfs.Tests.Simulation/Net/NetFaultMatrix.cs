using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Con.ConHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Net;

/// <summary>Fault actions of the NET-020 single-fault campaign (test plan §8.3, one representative per family).</summary>
internal enum MatrixFault
{
    /// <summary>Abrupt crash of the node where the fault hits (process family).</summary>
    Crash,

    /// <summary>Pause of the node loop for one second, then resume (process family).</summary>
    Pause,

    /// <summary>EIO at the boundary (storage family).</summary>
    IoError,

    /// <summary>500 ms stall at the boundary (delivery/storage latency).</summary>
    Delay,

    /// <summary>The server executes but its reply is lost (application delivery family).</summary>
    DropReply,

    /// <summary>The SDK caller's token is cancelled at the boundary (client control family).</summary>
    CancelCaller,
}

/// <summary>One generated campaign case: operation × stage site × fault, with its applicability decision.</summary>
internal sealed record MatrixCase(DfsOperationKind Operation, string Site, FaultStage Stage, MatrixFault Fault, string? ExclusionReason)
{
    public bool Applicable => ExclusionReason is null;

    public string Id => $"{Operation}@{Site}/{Fault}";

    public override string ToString() => Id;
}

/// <summary>Result of running one applicable case.</summary>
internal sealed record MatrixCaseResult(MatrixCase Case, bool Reached, bool Completed, string? Failure)
{
    public bool Passed => Reached && Completed && Failure is null;
}

/// <summary>Per-case state shared by a driver's setup, invocation and verification.</summary>
internal sealed class MatrixContext
{
    public required TestCluster Cluster { get; init; }

    public required DfsClient Client { get; init; }

    public required DfsVolume Volume { get; init; }

    public required MatrixCase Case { get; init; }

    public DfsOperationId OperationId { get; set; }

    public Dictionary<string, object> State { get; } = new(StringComparer.Ordinal);

    public T Get<T>(string key) => (T)State[key];

    public bool TryGet<T>(string key, out T value)
    {
        if (State.TryGetValue(key, out object? boxed) && boxed is T typed)
        {
            value = typed;
            return true;
        }

        value = default!;
        return false;
    }
}

/// <summary>
/// Registry entry of one operation family: its published stage graph (the fault sites it passes through, test plan §8:
/// "each operation publishes its real stage graph"), the topology it needs, and how to set up, invoke and verify it.
/// </summary>
internal sealed record OperationDriver(
    DfsOperationKind Operation,
    string Description,
    IReadOnlyCollection<string> Sites,
    Func<TestClusterOptions> Options,
    Func<MatrixContext, Task> SetupAsync,
    Func<MatrixContext, CancellationToken, Task> InvokeAsync,
    Func<MatrixContext, Exception?, Task> VerifyAsync);

/// <summary>
/// The NET-020 operation × stage × fault matrix (test plan §8): generation with explicit applicability predicates and
/// reviewed exclusions, and execution of each applicable case with data, namespace, outcome, pin/reservation and
/// recovery invariants.
/// </summary>
/// <remarks>
/// Conventions of the stage graph: the "operation" dimension is the protocol operation kind (<see cref="DfsOperationKind"/>,
/// the value of <c>MessageTypes.OperationOf</c> and of <see cref="FaultContext.Operation"/>). Client stages F00/F01 exist only
/// for operations started by a public SDK call; request-path stages (F03-F15) carry the operation in the fault context and
/// are matched on it; background stages (F16-F21) are matched on the site alone while the driver is the only activity
/// that reaches them.
/// </remarks>
internal static class NetFaultMatrix
{
    private static readonly string[] ClientStart = [FaultSites.ClientOperationStart, FaultSites.ClientValidated];
    private static readonly string[] ClientWire = [FaultSites.ClientRequestSend, FaultSites.ClientReplyReceived];
    private static readonly string[] ServerWire = [FaultSites.RpcRequestAccepted, FaultSites.RpcReplySend];
    private static readonly string[] Propose =
        [FaultSites.RaftBeforePropose, FaultSites.RaftAfterLocalAppend, FaultSites.RaftAfterCommitBeforeApply, FaultSites.ApplyAfterBeforeReply];
    private static readonly string[] ObjectIo = [FaultSites.ObjectWrite, FaultSites.ObjectBeforeFlush, FaultSites.ObjectAfterFlushBeforeReceipt];

    /// <summary>Boundaries at which local storage I/O happens (where an injected EIO is meaningful).</summary>
    private static readonly HashSet<string> IoSites = new(StringComparer.Ordinal)
    {
        FaultSites.ObjectWrite, FaultSites.ObjectBeforeFlush, FaultSites.RecoveryJournalReplay, FaultSites.RecoveryCheckpointInstall,
        FaultSites.RaftSnapshotInstall, FaultSites.BackupObjectReceived, FaultSites.BackupBeforePointer, FaultSites.GcSweep,
        FaultSites.CompactionCopy, FaultSites.CompactionPublish, FaultSites.CompactionDelete, FaultSites.ReplicaDelete,
    };

    /// <summary>Operation families with no applicable case at all, and why (reviewed exclusions).</summary>
    public static readonly IReadOnlyDictionary<DfsOperationKind, string> OperationExclusions = new Dictionary<DfsOperationKind, string>
    {
        [DfsOperationKind.Unknown] = "not an operation: frames of unregistered message types map to Unknown",
        [DfsOperationKind.RaftVote] = "no registered fault site lies on the vote path (term/vote persistence has no FaultSites boundary); covered by CON-001 (bounded model with vote-before-persist check) and CON-004",
        [DfsOperationKind.Compaction] = "object-store segment compaction is scheduled by the storage engine's garbage ratio and cannot be forced by a bounded SDK case; covered by DSK compaction tests that call ObjectStore.CompactAsync",
        [DfsOperationKind.Scrub] = "no registered fault site lies on the scrub read path; scrub detection is covered by DSK-016",
    };

    /// <summary>Sites that are reviewed as having no applicable case in this campaign.</summary>
    public static readonly IReadOnlySet<string> SitesWithoutApplicableCase = new HashSet<string>(StringComparer.Ordinal)
    {
        FaultSites.CompactionCopy, FaultSites.CompactionPublish, FaultSites.CompactionDelete,
    };

    private static readonly Lazy<IReadOnlyDictionary<DfsOperationKind, OperationDriver>> LazyDrivers = new(CreateDrivers);

    public static IReadOnlyDictionary<DfsOperationKind, OperationDriver> Drivers => LazyDrivers.Value;

    /// <summary>Generates the complete matrix: every operation kind × every registered site × every fault.</summary>
    public static IReadOnlyList<MatrixCase> Generate()
    {
        var cases = new List<MatrixCase>();
        foreach (DfsOperationKind operation in Enum.GetValues<DfsOperationKind>())
        {
            foreach (string site in FaultSites.All)
            {
                foreach (MatrixFault fault in Enum.GetValues<MatrixFault>())
                    cases.Add(new MatrixCase(operation, site, FaultSites.StageOf(site), fault, Exclusion(operation, site, fault)));
            }
        }

        return cases;
    }

    /// <summary>The applicability predicate: null when applicable, otherwise the specific exclusion reason.</summary>
    public static string? Exclusion(DfsOperationKind operation, string site, MatrixFault fault)
    {
        if (OperationExclusions.TryGetValue(operation, out string? operationReason))
            return operationReason;
        if (!Drivers.TryGetValue(operation, out OperationDriver? driver))
            return $"no driver registered for {operation}";
        if (!driver.Sites.Contains(site))
            return SiteExclusion(operation, site);
        return FaultExclusion(site, fault);
    }

    /// <summary>
    /// Operation kinds that exist only at the API level and travel inside another operation's message
    /// (<c>MessageTypes.OperationOf</c> maps no message type to them), so their wire and server stages carry the other kind.
    /// </summary>
    private static readonly IReadOnlyDictionary<DfsOperationKind, string> CarriedBy = new Dictionary<DfsOperationKind, string>
    {
        [DfsOperationKind.Exists] = "GetInfo",
    };

    private static string SiteExclusion(DfsOperationKind operation, string site) => FaultSites.StageOf(site) switch
    {
        FaultStage.F03 or FaultStage.F04 or FaultStage.F13 or FaultStage.F14 when CarriedBy.TryGetValue(operation, out string? carrier) =>
            $"{operation} has no message of its own (it travels as a {carrier} request, see MessageTypes.OperationOf); its wire and server stages are the {carrier} cases",
        FaultStage.F00 or FaultStage.F01 => $"{operation} is not started by a public SDK call (internal protocol or background work), so client op.start/validated stages do not exist for it",
        FaultStage.F02 => $"{operation} runs over an established pooled connection; connection establishment is the Connect operation's stage (stale pooled reuse is NET-016)",
        FaultStage.F03 when site == FaultSites.ClientUploadChunk => $"{operation} uploads no object payload chunks (payload chunks are PutObject requests)",
        FaultStage.F03 or FaultStage.F14 => $"{operation} is not sent by the SDK client (server-to-server or background traffic)",
        FaultStage.F04 or FaultStage.F13 => $"{operation} has no request/response exchange of its own",
        FaultStage.F05 => $"{operation} registers no staging session",
        FaultStage.F06 or FaultStage.F07 or FaultStage.F08 => $"{operation} writes no object (object writes are PutObject, Repair and RestoreSnapshot copy requests)",
        FaultStage.F09 => $"{operation} publishes no data receipts",
        FaultStage.F10 or FaultStage.F11 or FaultStage.F12 => $"{operation} proposes no metadata log entry (read-only operation or not replicated through the volume log)",
        FaultStage.F15 => $"{operation} releases no reservation, pin or reference after its decision",
        FaultStage.F16 => $"{operation} performs no restart, journal replay or checkpoint install",
        FaultStage.F17 => $"{operation} establishes no snapshot/transfer pin or backup inventory",
        FaultStage.F18 or FaultStage.F19 => $"{operation} is not part of the backup transfer/publication pipeline",
        FaultStage.F20 => $"{operation} performs no retention expiry, GC sweep, compaction or replica deletion",
        FaultStage.F21 => $"{operation} performs no membership, placement, policy, key or epoch transition",
        _ => $"{operation} does not reach {site}",
    };

    private static string? FaultExclusion(string site, MatrixFault fault)
    {
        FaultStage stage = FaultSites.StageOf(site);
        bool clientSite = stage is FaultStage.F00 or FaultStage.F01 or FaultStage.F02 or FaultStage.F03 or FaultStage.F14;
        return fault switch
        {
            MatrixFault.CancelCaller => clientSite ? null : "CancelCaller cancels the SDK caller's token and exists only at client sites; server-side cancellation races are NET-017",
            MatrixFault.Crash => clientSite ? "CrashNode crashes a server node; client process death is covered by KillClient scenarios (NET-002, OUT)" : null,
            MatrixFault.Pause => clientSite ? "PauseNode pauses a server node loop; client-side stalls are covered by Delay" : null,
            MatrixFault.DropReply => (stage is >= FaultStage.F04 and <= FaultStage.F13) || site == FaultSites.BackupAck
                ? null
                : "DropReply discards a server reply; this boundary is not on a request/reply path",
            MatrixFault.IoError => IoSites.Contains(site) ? null : "no local storage I/O happens at this boundary",
            MatrixFault.Delay => null,
            _ => "unknown fault",
        };
    }

    public static FaultAction ActionFor(MatrixFault fault) => fault switch
    {
        MatrixFault.Crash => FaultAction.CrashNode(CrashMode.LoseUnflushed),
        MatrixFault.Pause => FaultAction.PauseNode(),
        MatrixFault.IoError => FaultAction.IoError(),
        MatrixFault.Delay => FaultAction.Delay(TimeSpan.FromMilliseconds(500)),
        MatrixFault.DropReply => FaultAction.DropReply(),
        MatrixFault.CancelCaller => FaultAction.CancelCaller(),
        _ => throw new ArgumentOutOfRangeException(nameof(fault)),
    };

    /// <summary>Request-path stages carry the operation in the fault context; background stages are matched on the site.</summary>
    public static DfsOperationKind? OperationFilter(DfsOperationKind operation, string site) =>
        FaultSites.StageOf(site) <= FaultStage.F15 ? operation : null;

    /// <summary>Runs one applicable case in a fresh deterministic cluster and records reached/completed/failure.</summary>
    public static async Task<MatrixCaseResult> RunCaseAsync(MatrixCase matrixCase, OperationDriver driver)
    {
        FaultArm? arm = null;
        bool completed = false;
        string? failure = null;
        try
        {
            await ClusterTest.RunAsync(driver.Options(), async cluster =>
            {
                DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(8) }, Ct);
                try
                {
                    var context = new MatrixContext { Cluster = cluster, Client = client, Volume = client.GetVolume(Vol), Case = matrixCase };
                    await driver.SetupAsync(context);

                    arm = cluster.Faults.Arm(new FaultMatch(matrixCase.Site, Operation: OperationFilter(matrixCase.Operation, matrixCase.Site)),
                        ActionFor(matrixCase.Fault));
                    using var caller = CancellationTokenSource.CreateLinkedTokenSource(Ct);
                    Task<Exception?> invocation = CaptureAsync(() => driver.InvokeAsync(context, caller.Token));

                    // Liveness: the operation terminates (or reaches the pause) within a bound.
                    await cluster.WaitUntilAsync(() => Task.FromResult(invocation.IsCompleted || (matrixCase.Fault == MatrixFault.Pause && arm.Triggered)),
                        TimeSpan.FromSeconds(60), $"{matrixCase.Id}: the operation must terminate", Ct);
                    if (matrixCase.Fault == MatrixFault.Pause)
                    {
                        await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                        arm.Release();
                    }

                    await cluster.WaitUntilAsync(() => Task.FromResult(invocation.IsCompleted), TimeSpan.FromSeconds(60),
                        $"{matrixCase.Id}: the operation must terminate after the fault is released", Ct);
                    Exception? error = await invocation;
                    cluster.Faults.DisarmAll();

                    // Recovery invariants: restart crashed nodes, heal, stable, converged, no split brain.
                    await RecoverAndConvergeAsync(cluster);
                    if (arm.Triggered)
                        Assert.True(cluster.Faults.Coverage.WasReached(matrixCase.Site), $"{matrixCase.Id}: the coverage registry must record the hit");

                    // Outcome invariants common to every operation.
                    if (error is not null)
                    {
                        NetHelpers.AssertTypedFailure(error, matrixCase.Id);
                        if (error is OperationCanceledException)
                            Assert.True(matrixCase.Fault == MatrixFault.CancelCaller, $"{matrixCase.Id}: cancellation reported although the caller never cancelled");
                    }

                    if (arm.Triggered && matrixCase.Fault == MatrixFault.CancelCaller && matrixCase.Stage is FaultStage.F00 or FaultStage.F01)
                        Assert.True(error is OperationCanceledException, $"{matrixCase.Id}: cancellation before transmission must surface as OperationCanceledException, got {error?.GetType().Name ?? "success"}");

                    // Operation-specific data, namespace and outcome invariants, then reservations and pins: writer
                    // reservations and transactions are released (or expire) while the client is connected; once the
                    // client is gone, its session expires and every staging session it opened is closed.
                    await driver.VerifyAsync(context, error);
                    await AssertNoLeakedReservationsAsync(cluster, includeStaging: false);
                    await client.DisposeAsync();
                    await AssertNoLeakedReservationsAsync(cluster, includeStaging: true);
                    completed = true;
                }
                finally
                {
                    await CaptureAsync(async () => await client.DisposeAsync());
                }
            }, Ct);
        }
        catch (Exception ex) when (!Ct.IsCancellationRequested)
        {
            failure = ex.GetType().Name + ": " + FirstLine(ex.Message);
        }

        // A case whose fault point was reached but which then failed counts as reached (and failed), not as unreached.
        bool reached = arm?.Triggered ?? false;
        if (!reached && failure is null)
            failure = "fault point not reached";
        return new MatrixCaseResult(matrixCase, reached, completed, failure);
    }

    /// <summary>
    /// Restarts a node; when an armed fault crashes it during that very restart (recovery boundaries F16), the node is
    /// restarted once more (the fault fires once), so the driver continues with a running node. An injected I/O error
    /// during recovery (journal replay, checkpoint install) must make the start fail cleanly: the node never serves with
    /// partially recovered state (a durable record it could not read is never skipped, ARCHITECTURE §4) and is not running
    /// afterwards; the next restart, without the fault, recovers it.
    /// </summary>
    private static async Task RestartToleratingInjectedCrashAsync(TestNode node)
    {
        Exception? error = await CaptureAsync(() => node.RestartAsync());
        if (error is null && node.State != TestNodeState.Crashed)
            return;
        bool injectedIoError = error is IOException { HResult: StorageErrors.IoDeviceHResult };
        if (injectedIoError)
            Assert.True(node.State == TestNodeState.Stopped, $"{node.Name}: a start that failed with an injected I/O error must leave the node down, not {node.State}");
        else
            Assert.True(node.State == TestNodeState.Crashed, $"{node.Name}: restart failed without an injected crash or I/O error: {error}");
        await node.RestartAsync();
    }

    private static string FirstLine(string text)
    {
        int newline = text.IndexOfAny(['\r', '\n']);
        return newline < 0 ? text : text[..newline];
    }

    /// <summary>
    /// Writer reservations and transactions are released (or expire) after a case; with <paramref name="includeStaging"/>
    /// also every staging session (closed at the latest when the owning client's session expires, ARCHITECTURE §7).
    /// </summary>
    public static Task AssertNoLeakedReservationsAsync(TestCluster cluster, bool includeStaging) =>
        cluster.WaitUntilAsync(async () =>
        {
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            return state.Handles.All(h => !h.IsWriter || h.Expired) && state.ActiveTransactions == 0
                && (!includeStaging || state.Staging.All(s => !s.Open));
        }, includeStaging ? NodeTimings.Fast.SessionTimeout + (Lease * 4) : Lease * 4,
            includeStaging
                ? "staging sessions must be closed once their clients' sessions have expired"
                : "writer reservations and transactions must be released or expire after the case", Ct);

    // ------------------------------------------------------------------------------------------------------------
    // Drivers
    // ------------------------------------------------------------------------------------------------------------

    private static TestClusterOptions DefaultOptions() => Det(TestClusterOptions.ThreeZones) with { ScenarioTimeout = TimeSpan.FromSeconds(300) };

    private static string[] Sites(params IEnumerable<string>[] groups) => groups.SelectMany(g => g).Distinct(StringComparer.Ordinal).ToArray();

    private static IReadOnlyDictionary<DfsOperationKind, OperationDriver> CreateDrivers()
    {
        var drivers = new List<OperationDriver>
        {
            ConnectDriver(DfsOperationKind.Connect, Sites(ClientStart, [FaultSites.ClientConnect])),
            ConnectDriver(DfsOperationKind.Discover, Sites(ClientWire, ServerWire)),
            ConnectDriver(DfsOperationKind.Authenticate, Sites(ClientWire, ServerWire)),
            OpenSessionDriver(),
            KeepAliveDriver(),
            OpenDriver(),
            CloseHandleDriver(),
            ReadDriver(DfsOperationKind.Read, Sites(ClientStart, ClientWire, ServerWire)),
            ReadDriver(DfsOperationKind.ReadObject, Sites(ClientWire, ServerWire)),
            FromMutation(DfsOperationKind.PutObject, "stream-commit", Sites(ClientWire, [FaultSites.ClientUploadChunk], ServerWire, ObjectIo)),
            FromMutation(DfsOperationKind.CreateStaging, "stream-commit", Sites(ClientWire, ServerWire, Propose, [FaultSites.StagingRegistered])),
            FromMutation(DfsOperationKind.CommitFile, "stream-commit", Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.CommitReceiptValidated])),
            FromMutation(DfsOperationKind.WriteAll, "write-all", Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.CommitReceiptValidated, FaultSites.CleanupRelease])),
            FromMutation(DfsOperationKind.Append, "append", Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.CommitReceiptValidated])),
            FromMutation(DfsOperationKind.Copy, "copy", Sites(ClientStart, ClientWire, ServerWire, Propose)),
            FromMutation(DfsOperationKind.Move, "move", Sites(ClientStart, ClientWire, ServerWire, Propose)),
            FromMutation(DfsOperationKind.Delete, "delete", Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.CleanupRelease])),
            FromMutation(DfsOperationKind.CreateDirectory, "create-directory", Sites(ClientStart, ClientWire, ServerWire, Propose)),
            EnumerateDriver(),
            GetInfoDriver(),
            ExistsDriver(),
            FromMutation(DfsOperationKind.TxBegin, "tx-commit", Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.StagingRegistered])),
            FromMutation(DfsOperationKind.TxOp, "tx-commit", Sites(ClientStart, ClientWire, ServerWire, Propose)),
            FromMutation(DfsOperationKind.TxCommit, "tx-commit", Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.CommitReceiptValidated, FaultSites.CleanupRelease])),
            TxAbortDriver(),
            FromMutation(DfsOperationKind.CreateSnapshot, "create-snapshot", Sites(ClientStart, ClientWire, ServerWire, Propose)),
            DeleteSnapshotDriver(),
            OpenSnapshotDriver(),
            RestoreSnapshotDriver(),
            QueryOperationDriver(),
            BackupDriver(DfsOperationKind.BackupTransfer, Sites(ServerWire, [FaultSites.SnapshotPin, FaultSites.BackupInventory, FaultSites.BackupObjectReceived])),
            BackupDriver(DfsOperationKind.BackupPublish, Sites(ServerWire, [FaultSites.BackupBeforeClosureVerify, FaultSites.BackupBeforePointer, FaultSites.BackupAfterPointer, FaultSites.BackupAck])),
            BackupDriver(DfsOperationKind.BackupAwait, Sites(ClientStart, ClientWire, ServerWire)),
            GcDriver(),
            RepairDriver(),
            RaftAppendDriver(),
            RaftSnapshotDriver(),
            MembershipChangeDriver(),
            PolicyChangeDriver(),
            KeyRotationDriver(),
            AdminDriver(),
        };
        return drivers.ToDictionary(d => d.Operation);
    }

    private static OperationDriver FromMutation(DfsOperationKind operation, string mutation, IReadOnlyCollection<string> sites) => new(
        operation,
        $"mutation case '{mutation}' with an explicit operation identity",
        sites,
        DefaultOptions,
        async context =>
        {
            MutationCase m = NetHelpers.CreateMutation(mutation);
            context.State["m"] = m;
            await m.SetupAsync(context.Volume);
            context.OperationId = await context.Volume.NewOperationIdAsync(Ct);
        },
        async (context, ct) => context.State["token"] = await context.Get<MutationCase>("m").InvokeAsync(context.Volume, context.OperationId, ct),
        async (context, error) =>
        {
            MutationCase m = context.Get<MutationCase>("m");
            CommitToken? token = context.TryGet("token", out CommitToken t) ? t : null;
            await NetHelpers.AssertExactlyOnceAsync(m, context.Volume, context.OperationId, token, error);
        });

    private static OperationDriver ConnectDriver(DfsOperationKind operation, IReadOnlyCollection<string> sites) => new(
        operation,
        "DfsClient.ConnectAsync of a new client (handshake, authentication, discovery)",
        sites,
        DefaultOptions,
        _ => Task.CompletedTask,
        async (context, ct) =>
        {
            await using DfsClient probe = await context.Cluster.ConnectAsync(new TestClientOptions
            {
                Name = "probe",
                ConnectTimeout = TimeSpan.FromSeconds(5),
                OperationTimeout = TimeSpan.FromSeconds(5),
            }, ct);
            context.State["cluster"] = probe.Cluster.ClusterId;
        },
        async (context, error) =>
        {
            if (error is null)
                Assert.Equal(context.Cluster.ClusterId, context.Get<Guid>("cluster"));
            await using DfsClient again = await context.Cluster.ConnectAsync(new TestClientOptions { Name = "probe2" }, Ct);
            Assert.Equal(context.Cluster.ClusterId, again.Cluster.ClusterId);
        });

    private static OperationDriver OpenSessionDriver()
    {
        byte[] bytes = TestData.Bytes(0x6001, 300);
        return new OperationDriver(
            DfsOperationKind.OpenSession,
            "first mutation of a new client (opens its volume session lazily)",
            Sites(ClientWire, ServerWire, Propose),
            DefaultOptions,
            _ => Task.CompletedTask,
            async (context, ct) =>
            {
                await using DfsClient probe = await context.Cluster.ConnectAsync(new TestClientOptions { Name = "probe", OperationTimeout = TimeSpan.FromSeconds(8) }, ct);
                await probe.GetVolume(Vol).Files.WriteAllBytesAsync("/os", bytes, ct);
            },
            async (context, error) =>
            {
                if (error is null)
                    await AssertContentAsync(context.Volume, "/os", bytes, "write through the new session");
                else if (await context.Volume.Files.ExistsAsync("/os", Ct))
                    await AssertContentAsync(context.Volume, "/os", bytes, "all-or-nothing");
                await context.Volume.Files.WriteAllBytesAsync("/os", bytes, Ct);
            });
    }

    private static OperationDriver KeepAliveDriver()
    {
        byte[] bytes = TestData.Bytes(0x6011, 5000);
        return new OperationDriver(
            DfsOperationKind.KeepAlive,
            "an open read handle renewed by keep-alives for one lease period",
            Sites(ClientWire, ServerWire, Propose),
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/ka", bytes, Ct);
                context.State["handle"] = await context.Volume.Files.OpenAsync("/ka", new DfsOpenOptions { Share = FileShare.ReadWrite }, Ct);
            },
            async (context, ct) =>
            {
                await context.Cluster.DelayAsync(Lease, ct);
                DfsStream handle = context.Get<DfsStream>("handle");
                handle.Seek(0, SeekOrigin.Begin);
                (byte[] all, _) = await NetHelpers.ReadAllCallsAsync(handle, 4096, ct);
                context.State["read"] = all;
            },
            async (context, error) =>
            {
                if (error is null)
                    AssertEx.BytesEqual(bytes, context.Get<byte[]>("read"), "read through a renewed handle");
                else
                    Assert.True(error is DfsLeaseLostException or DfsPinExpiredException || IsUnavailability(error) || error is IOException, $"handle failure must be typed: {error}");
                // Closing a read handle whose lease may have lapsed during the fault is best effort.
                await CaptureAsync(async () => await context.Get<DfsStream>("handle").DisposeAsync());
                await AssertContentAsync(context.Volume, "/ka", bytes, "file untouched");
            });
    }

    private static OperationDriver OpenDriver()
    {
        byte[] bytes = TestData.Bytes(0x6021, 100);
        return new OperationDriver(
            DfsOperationKind.Open,
            "creating open (CreateNew) with an operation identity, write, dispose",
            Sites(ClientStart, ClientWire, ServerWire, Propose),
            DefaultOptions,
            async context => context.OperationId = await context.Volume.NewOperationIdAsync(Ct),
            async (context, ct) =>
            {
                DfsStream stream = await context.Volume.Files.OpenAsync("/op-new", new DfsOpenOptions
                {
                    Mode = FileMode.CreateNew,
                    Access = FileAccess.Write,
                    Share = FileShare.None,
                    OperationId = context.OperationId,
                }, ct);
                await using (stream)
                    await stream.WriteAsync(bytes, ct);
            },
            async (context, error) =>
            {
                DfsOperationOutcome outcome = await context.Volume.QueryOperationAsync(context.OperationId, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted);
                if (outcome.Status == DfsOutcomeStatus.Aborted)
                {
                    Assert.False(await context.Volume.Files.ExistsAsync("/op-new", Ct));
                    Assert.NotNull(error);
                    return;
                }

                byte[] content = await context.Volume.Files.ReadAllBytesAsync("/op-new", Ct);
                if (error is null)
                    AssertEx.BytesEqual(bytes, content, "created and flushed");
                else
                    Assert.True(content.Length == 0 || content.AsSpan().SequenceEqual(bytes), "a created file is empty or holds the complete flush");
            });
    }

    private static OperationDriver CloseHandleDriver()
    {
        byte[] old = TestData.Bytes(0x6031, 3000);
        byte[] updated = TestData.Bytes(0x6032, 90_000);
        return new OperationDriver(
            DfsOperationKind.CloseHandle,
            "dispose of a writable handle with pending changes (flush + close)",
            Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.CleanupRelease]),
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/ch", old, Ct);
                DfsStream stream = await context.Volume.Files.OpenAsync("/ch", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
                await stream.WriteAsync(updated, Ct);
                context.State["handle"] = stream;
            },
            async (context, _) => await context.Get<DfsStream>("handle").DisposeAsync(),
            async (context, error) =>
            {
                byte[] content = await context.Volume.Files.ReadAllBytesAsync("/ch", Ct);
                if (error is null)
                    AssertEx.BytesEqual(updated, content, "dispose published the pending changes");
                else
                    Assert.True(content.AsSpan().SequenceEqual(old) || content.AsSpan().SequenceEqual(updated), "publication is all-or-nothing");
                await AssertNoLeakedReservationsAsync(context.Cluster, includeStaging: false);
                await using DfsStream again = await context.Volume.Files.OpenWriteAsync("/ch", Ct);
                Assert.True(again.CanWrite, "the reservation must be released so a new writer can open");
            });
    }

    private static OperationDriver ReadDriver(DfsOperationKind operation, IReadOnlyCollection<string> sites)
    {
        byte[] bytes = TestData.Bytes(0x6041, 150_000);
        return new OperationDriver(
            operation,
            "live stream read of a multi-extent file",
            sites,
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/rd", bytes, Ct);
                context.State["handle"] = await context.Volume.Files.OpenAsync("/rd", new DfsOpenOptions { Share = FileShare.ReadWrite }, Ct);
            },
            async (context, ct) =>
            {
                (byte[] all, _) = await NetHelpers.ReadAllCallsAsync(context.Get<DfsStream>("handle"), 64 * 1024, ct);
                context.State["read"] = all;
            },
            async (context, error) =>
            {
                if (error is null)
                    AssertEx.BytesEqual(bytes, context.Get<byte[]>("read"), "verified bytes of the selected revision");
                // Closing a read handle whose lease may have lapsed during the fault is best effort.
                await CaptureAsync(async () => await context.Get<DfsStream>("handle").DisposeAsync());
                await AssertContentAsync(context.Volume, "/rd", bytes, "file intact");
            });
    }

    private static OperationDriver EnumerateDriver()
    {
        string[] expected = Enumerable.Range(0, 30).Select(i => "f" + i.ToString("D2", CultureInfo.InvariantCulture)).ToArray();
        return new OperationDriver(
            DfsOperationKind.Enumerate,
            "paged enumeration (page size 7) of a 30-entry directory",
            Sites(ClientStart, ClientWire, ServerWire),
            DefaultOptions,
            async context =>
            {
                await context.Volume.Directories.CreateDirectoryAsync("/en", Ct);
                foreach (string name in expected)
                    await context.Volume.Files.WriteAllBytesAsync("/en/" + name, TestData.Bytes(0x6050, 10), Ct);
            },
            async (context, ct) =>
            {
                var names = new List<string>();
                await foreach (DfsEntryInfo entry in context.Volume.Directories.EnumerateAsync("/en", new DfsEnumerationOptions { PageSize = 7 }, ct))
                    names.Add(entry.Name);
                context.State["names"] = names;
            },
            (context, error) =>
            {
                if (error is null)
                    Assert.Equal(expected, context.Get<List<string>>("names").Order(StringComparer.Ordinal).ToArray());
                else
                    Assert.False(error is DirectoryNotFoundException, "a fault must never surface as false absence");
                return Task.CompletedTask;
            });
    }

    private static OperationDriver GetInfoDriver()
    {
        byte[] bytes = TestData.Bytes(0x6061, 7777);
        return new OperationDriver(
            DfsOperationKind.GetInfo,
            "file metadata retrieval",
            Sites(ClientStart, ClientWire, ServerWire),
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/gi", bytes, Ct);
                context.State["inode"] = (await context.Volume.Files.GetInfoAsync("/gi", Ct)).InodeId;
            },
            async (context, ct) => context.State["info"] = await context.Volume.Files.GetInfoAsync("/gi", ct),
            (context, error) =>
            {
                if (error is null)
                {
                    DfsFileInfo info = context.Get<DfsFileInfo>("info");
                    Assert.Equal(bytes.Length, info.Length);
                    Assert.Equal(context.Get<InodeId>("inode"), info.InodeId);
                }
                else
                {
                    Assert.False(error is FileNotFoundException, "a fault must never surface as false absence");
                }

                return Task.CompletedTask;
            });
    }

    private static OperationDriver ExistsDriver() => new(
        DfsOperationKind.Exists,
        "existence check of a present file (API-level stages; the request itself is a GetInfo)",
        Sites(ClientStart),
        DefaultOptions,
        context => context.Volume.Files.WriteAllBytesAsync("/ex", TestData.Bytes(0x6071, 10), Ct),
        async (context, ct) => context.State["exists"] = await context.Volume.Files.ExistsAsync("/ex", ct),
        (context, error) =>
        {
            if (error is null)
                Assert.True(context.Get<bool>("exists"), "Exists returned false for a present file (false absence)");
            return Task.CompletedTask;
        });

    private static OperationDriver TxAbortDriver()
    {
        byte[] a0 = TestData.Bytes(0x6081, 400);
        return new OperationDriver(
            DfsOperationKind.TxAbort,
            "transaction with a staged write, explicitly aborted",
            Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.CleanupRelease]),
            DefaultOptions,
            context => context.Volume.Files.WriteAllBytesAsync("/ta", a0, Ct),
            async (context, ct) =>
            {
                await using DfsTransaction tx = await context.Volume.Transactions.BeginAsync(null, ct);
                await tx.Files.WriteAllBytesAsync("/ta", TestData.Bytes(0x6082, 70_000), ct);
                await tx.AbortAsync(ct);
            },
            (context, _) => AssertContentAsync(context.Volume, "/ta", a0, "an aborted transaction never publishes"));
    }

    private static OperationDriver DeleteSnapshotDriver() => new(
        DfsOperationKind.DeleteSnapshot,
        "snapshot deletion",
        Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.CleanupRelease]),
        DefaultOptions,
        async context =>
        {
            await context.Volume.Files.WriteAllBytesAsync("/sd", TestData.Bytes(0x6091, 70_000), Ct);
            await context.Volume.Snapshots.CreateAsync("del-snap", null, Ct);
        },
        (context, ct) => context.Volume.Snapshots.DeleteAsync("del-snap", ct),
        async (context, error) =>
        {
            bool present = (await context.Volume.Snapshots.ListAsync(Ct)).Any(s => s.Name == "del-snap");
            if (error is null)
                Assert.False(present, "a deleted snapshot must no longer be listed");
            await AssertContentAsync(context.Volume, "/sd", TestData.Bytes(0x6091, 70_000), "live data is never affected by snapshot deletion");
        });

    private static OperationDriver OpenSnapshotDriver()
    {
        byte[] v1 = TestData.Bytes(0x60A1, 70_000);
        byte[] v2 = TestData.Bytes(0x60A2, 70_000);
        return new OperationDriver(
            DfsOperationKind.OpenSnapshot,
            "open a snapshot view (reader pin) and read a file that changed since",
            Sites(ClientStart, ClientWire, ServerWire, Propose, [FaultSites.SnapshotPin]),
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/so", v1, Ct);
                await context.Volume.Snapshots.CreateAsync("open-snap", null, Ct);
                await context.Volume.Files.WriteAllBytesAsync("/so", v2, Ct);
            },
            async (context, ct) =>
            {
                await using DfsSnapshotView view = await context.Volume.Snapshots.OpenAsync("open-snap", ct);
                context.State["read"] = await view.Files.ReadAllBytesAsync("/so", ct);
            },
            async (context, error) =>
            {
                if (error is null)
                    AssertEx.BytesEqual(v1, context.Get<byte[]>("read"), "snapshot view content");
                await AssertContentAsync(context.Volume, "/so", v2, "live content");
            });
    }

    private static OperationDriver RestoreSnapshotDriver()
    {
        byte[] bytes = TestData.Bytes(0x60B1, 100_000);
        return new OperationDriver(
            DfsOperationKind.RestoreSnapshot,
            "restore a snapshot into a new volume",
            Sites(ClientStart, ClientWire, ServerWire, Propose, ObjectIo),
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/sr", bytes, Ct);
                await context.Volume.Snapshots.CreateAsync("rs", null, Ct);
            },
            async (context, ct) => context.State["restored"] = await context.Volume.Snapshots.RestoreAsync("rs", "restored", ct),
            async (context, error) =>
            {
                bool exists = (await context.Cluster.Admin.ListVolumesAsync(Ct)).Contains("restored");
                if (error is null)
                    Assert.True(exists, "a successful restore must create the volume");
                if (exists)
                {
                    DfsVolume restored = context.Client.GetVolume("restored");
                    await context.Cluster.WaitUntilAsync(async () => await CaptureAsync(() => restored.Files.GetInfoAsync("/sr", Ct)) is null,
                        ElectionBound, "the restored volume must become available", Ct);
                    await AssertContentAsync(restored, "/sr", bytes, "a restored volume is complete");
                }

                await AssertContentAsync(context.Volume, "/sr", bytes, "source volume unchanged");
            });
    }

    private static OperationDriver QueryOperationDriver()
    {
        byte[] bytes = TestData.Bytes(0x60C1, 5000);
        byte[] neverBytes = TestData.Bytes(0x60C2, 300);
        return new OperationDriver(
            DfsOperationKind.QueryOperation,
            "resolve (fence) an identity that was never used, then resolve an operation whose reply was lost",
            Sites(ClientStart, ClientWire, ServerWire, Propose),
            DefaultOptions,
            async context =>
            {
                // The never-used identity has no outcome, so resolving it must propose a fence (the Propose stages); the
                // lost-reply identity has a committed outcome that must be reported as such.
                context.State["never"] = await context.Volume.NewOperationIdAsync(Ct);
                context.OperationId = await context.Volume.NewOperationIdAsync(Ct);
                FaultArm drop = context.Cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.WriteAll, Count: 1000), FaultAction.DropReply());
                await CaptureAsync(() => context.Volume.Files.WriteAllBytesAsync("/qo", bytes, WithId(context.OperationId), Ct));
                context.Cluster.Faults.AssertReached(drop);
                drop.Disarm();
            },
            async (context, ct) =>
            {
                context.State["fenced"] = await context.Volume.QueryOperationAsync(context.Get<DfsOperationId>("never"), resolve: true, ct);
                context.State["outcome"] = await context.Volume.QueryOperationAsync(context.OperationId, resolve: true, ct);
            },
            async (context, error) =>
            {
                DfsOperationId never = context.Get<DfsOperationId>("never");
                DfsOperationOutcome fenced = await context.Volume.QueryOperationAsync(never, resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Aborted, fenced.Status);
                DfsOperationOutcome final = await context.Volume.QueryOperationAsync(context.OperationId, resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, final.Status);
                if (error is null)
                {
                    Assert.Equal(DfsOutcomeStatus.Aborted, context.Get<DfsOperationOutcome>("fenced").Status);
                    DfsOperationOutcome first = context.Get<DfsOperationOutcome>("outcome");
                    Assert.Equal(DfsOutcomeStatus.Committed, first.Status);
                    Assert.Equal(final.Token, first.Token);
                }

                // A fenced identity can never execute later (a delayed or retried copy is rejected, not applied).
                Exception? late = await CaptureAsync(() => context.Volume.Files.WriteAllBytesAsync("/qo-never", neverBytes, WithId(never), Ct));
                Assert.True(late is not null, "a write with a fenced identity must not succeed");
                Assert.False(await context.Volume.Files.ExistsAsync("/qo-never", Ct), "a fenced identity must never take effect");
                await AssertContentAsync(context.Volume, "/qo", bytes, "the executed write");
            });
    }

    private static OperationDriver BackupDriver(DfsOperationKind operation, IReadOnlyCollection<string> sites)
    {
        byte[] bytes = TestData.Bytes(0x60D1, 120_000);
        return new OperationDriver(
            operation,
            "synchronise and await an exact snapshot on a late-sync backup target",
            sites,
            () => DefaultOptions() with { BackupNodeCount = 1 },
            async context =>
            {
                TestBackupNode backup = context.Cluster.BackupNodes[0];
                await backup.RegisterAsync(Vol);
                await context.Volume.Files.WriteAllBytesAsync("/bk", bytes, Ct);
                await context.Volume.Snapshots.CreateAsync("bk1", null, Ct);
            },
            async (context, ct) =>
            {
                string target = context.Cluster.BackupNodes[0].TargetName;
                await context.Volume.Backups.RequestSyncAsync(target, "bk1", ct);
                context.State["checkpoint"] = await context.Volume.Backups.AwaitBackupSnapshotAsync(target, "bk1", TimeSpan.FromSeconds(20), ct);
            },
            async (context, error) =>
            {
                TestBackupNode backup = context.Cluster.BackupNodes[0];
                if (error is null)
                {
                    DfsBackupCheckpoint checkpoint = context.Get<DfsBackupCheckpoint>("checkpoint");
                    Assert.Equal("bk1", checkpoint.SnapshotName);
                    Assert.True(checkpoint.IsExactSnapshot);
                }

                // Recovery: the backup eventually publishes the complete snapshot, readable without the source.
                await context.Volume.Backups.RequestSyncAsync(backup.TargetName, "bk1", Ct);
                await context.Volume.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "bk1", TimeSpan.FromSeconds(60), Ct);
                Assert.Contains("bk1", await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
                await using DfsClient reader = await context.Cluster.ConnectBackupAsync(0, null, Ct);
                await using DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("bk1", Ct);
                await AssertBytesAsync(bytes, view.Files.ReadAllBytesAsync("/bk", Ct), "published backup snapshot");
                await AssertContentAsync(context.Volume, "/bk", bytes, "live data unaffected by backup faults");
            });
    }

    private static OperationDriver GcDriver()
    {
        byte[] old = TestData.Bytes(0x60E1, 200_000);
        byte[] updated = TestData.Bytes(0x60E2, 200_000);
        return new OperationDriver(
            DfsOperationKind.Gc,
            "overwrite a multi-extent file and wait for its unreferenced objects to be collected",
            [FaultSites.GcSweep],
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/gc", old, Ct);
                ObjectId[] oldObjects = (await NetHelpers.ExtentsAsync(context.Cluster, "/gc")).Select(e => e.Object).ToArray();
                await context.Volume.Files.WriteAllBytesAsync("/gc", updated, Ct);
                context.State["old"] = oldObjects;
            },
            (context, ct) => context.Cluster.WaitUntilAsync(async () =>
            {
                IReadOnlySet<ObjectId> stored = await context.Cluster.Inspect.GetAllStoredObjectsAsync(Vol);
                return !context.Get<ObjectId[]>("old").Any(stored.Contains);
            }, TimeSpan.FromSeconds(20), "unreferenced objects of the old revision must be collected", ct),
            async (context, _) =>
            {
                await AssertContentAsync(context.Volume, "/gc", updated, "GC never removes referenced data");
                await NetHelpers.AssertDurablyPlacedAsync(context.Cluster, "/gc", "after GC");
            });
    }

    private static OperationDriver RepairDriver()
    {
        byte[] bytes = TestData.Bytes(0x60F1, 200_000);
        return new OperationDriver(
            DfsOperationKind.Repair,
            "lose one replica of an object and repair it",
            Sites(ServerWire, ObjectIo),
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/rp", bytes, Ct);
                await context.Cluster.WaitForRepairAsync(Vol, ElectionBound, Ct);
                ObjectId target = (await NetHelpers.ExtentsAsync(context.Cluster, "/rp"))[0].Object;
                string holder = (await context.Cluster.Inspect.GetObjectHoldersAsync(target))[0];
                await context.Cluster.Node(holder).Disk.DeleteObjectAsync(target);
                context.State["object"] = target;
            },
            async (context, ct) =>
            {
                await context.Cluster.Admin.StartRepairAsync(Vol, ct);
                await context.Cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(20), ct);
            },
            async (context, _) =>
            {
                await context.Cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
                int copies = (await context.Cluster.Inspect.CountDurableDomainsAsync(context.Get<ObjectId>("object"))).Copies;
                Assert.True(copies >= context.Cluster.Options.GetEffectiveDefaultPolicy().TargetCopies, $"repair restored only {copies} copies");
                await AssertContentAsync(context.Volume, "/rp", bytes, "repaired data");
            });
    }

    private static OperationDriver RaftAppendDriver() => new(
        DfsOperationKind.RaftAppend,
        "replication traffic plus a follower restart (journal replay)",
        Sites(ServerWire, [FaultSites.RecoveryJournalReplay]),
        DefaultOptions,
        async context =>
        {
            for (int i = 0; i < 20; i++)
                await context.Volume.Files.WriteAllBytesAsync("/ra" + i.ToString(CultureInfo.InvariantCulture), TestData.Bytes(0x6100 + (ulong)i, 300), Ct);
        },
        async (context, ct) =>
        {
            TestNode follower = Others(context.Cluster, await context.Cluster.GetLeaderAsync(Vol, ct))[0];
            await follower.StopAsync();
            await RestartToleratingInjectedCrashAsync(follower);
            await context.Volume.Files.WriteAllBytesAsync("/ra-after", TestData.Bytes(0x6120, 300), ct);
            await context.Cluster.WaitForReplicationAsync(Vol, ct);
        },
        async (context, _) =>
        {
            for (int i = 0; i < 20; i += 7)
                await AssertContentAsync(context.Volume, "/ra" + i.ToString(CultureInfo.InvariantCulture), TestData.Bytes(0x6100 + (ulong)i, 300), "replicated data");
        });

    private static OperationDriver RaftSnapshotDriver() => new(
        DfsOperationKind.RaftSnapshot,
        "a lagging follower catches up by snapshot install, then restarts from its checkpoint",
        Sites(ServerWire, [FaultSites.RaftSnapshotInstall, FaultSites.RecoveryCheckpointInstall]),
        DefaultOptions,
        async context =>
        {
            TestNode follower = Others(context.Cluster, await context.Cluster.GetLeaderAsync(Vol, Ct))[0];
            context.State["follower"] = follower;
            context.Cluster.Network.Isolate(follower.Name);
            for (int i = 0; i < 250; i++)
                await context.Volume.Files.WriteAllBytesAsync("/rs" + (i % 10).ToString(CultureInfo.InvariantCulture), TestData.Bytes(0x6200 + (ulong)i, 64), Ct);
        },
        async (context, ct) =>
        {
            TestNode follower = context.Get<TestNode>("follower");
            context.Cluster.Network.Heal();

            // An injected crash may stop the follower during the install; it is then brought back like any crashed node.
            await context.Cluster.WaitForReplicationAsync(Vol, ct);
            if (follower.State is TestNodeState.Crashed or TestNodeState.Stopped)
                await RestartToleratingInjectedCrashAsync(follower);
            await context.Cluster.WaitForReplicationAsync(Vol, ct);
            await follower.CrashAsync(CrashMode.LoseUnflushed);
            await RestartToleratingInjectedCrashAsync(follower);
            await context.Cluster.WaitForReplicationAsync(Vol, ct);
        },
        async (context, _) =>
        {
            for (int i = 240; i < 250; i++)
                await AssertContentAsync(context.Volume, "/rs" + (i % 10).ToString(CultureInfo.InvariantCulture), TestData.Bytes(0x6200 + (ulong)i, 64), "latest revisions");
        });

    private static OperationDriver MembershipChangeDriver()
    {
        byte[] bytes = TestData.Bytes(0x6301, 150_000);
        return new OperationDriver(
            DfsOperationKind.MembershipChange,
            "drain a volume voter (voter replacement and old-replica deletion)",
            [FaultSites.Reconfiguration, FaultSites.ReplicaDelete],
            () => Det(new TestClusterOptions
            {
                NodeCount = 4,
                ControlVoters = 3,
                Volumes =
                [
                    new TestVolumeSpec(Vol)
                    {
                        Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = ["n0", "n1", "n2"] },
                    },
                ],
                ScenarioTimeout = TimeSpan.FromSeconds(300),
            }),
            context => context.Volume.Files.WriteAllBytesAsync("/mc", bytes, Ct),
            async (context, ct) =>
            {
                // Draining ends when the node has lost its voter role and its replicas have been moved off it (new copies
                // verified first, then the old ones deleted: the replica-deletion boundary).
                await context.Cluster.Admin.DrainNodeAsync("n2", ct);
                await context.Cluster.WaitUntilAsync(async () =>
                {
                    if ((await context.Cluster.Inspect.GetVolumeGroupAsync(Vol)).Voters.Contains("n2"))
                        return false;
                    DfsNodeInfo? info = (await context.Cluster.Admin.ListNodesAsync(ct)).FirstOrDefault(n => n.Name == "n2");
                    if (info is null || info.Status != DfsNodeStatus.Drained)
                        return false;
                    TestNode drained = context.Cluster.Node("n2");
                    return drained.State != TestNodeState.Running || (await drained.Inspect.ListObjectsAsync(Vol)).Count == 0;
                }, TimeSpan.FromSeconds(60), "the drained voter must be replaced and its replicas moved off", ct);
            },
            async (context, _) =>
            {
                await context.Cluster.WaitUntilAsync(async () =>
                {
                    GroupInspection group = await context.Cluster.Inspect.GetVolumeGroupAsync(Vol);
                    return group.Voters.Count == 3;
                }, TimeSpan.FromSeconds(60), "the voter set must return to three voters", Ct);
                await AssertContentAsync(context.Volume, "/mc", bytes, "data through a membership change");
                await NetHelpers.AssertDurablyPlacedAsync(context.Cluster, "/mc", "after the membership change");
            });
    }

    private static OperationDriver PolicyChangeDriver()
    {
        byte[] bytes = TestData.Bytes(0x6311, 100_000);
        return new OperationDriver(
            DfsOperationKind.PolicyChange,
            "change a file's named storage policy",
            Sites(ClientStart, ClientWire, ServerWire, Propose),
            DefaultOptions,
            async context =>
            {
                await context.Cluster.Admin.DefinePolicyAsync(Vol, "two", new DfsStoragePolicy
                {
                    TargetCopies = 2,
                    TargetDistinctZones = 2,
                    DurableCopiesBeforeAck = 2,
                    DurableZonesBeforeAck = 2,
                }, Ct);
                await context.Volume.Files.WriteAllBytesAsync("/pc", bytes, Ct);
                context.State["before"] = (await context.Volume.Files.GetInfoAsync("/pc", Ct)).StoragePolicy;
            },
            (context, ct) => context.Volume.Files.SetStoragePolicyAsync("/pc", "two", ct),
            async (context, error) =>
            {
                string policy = (await context.Volume.Files.GetInfoAsync("/pc", Ct)).StoragePolicy;
                if (error is null)
                    Assert.Equal("two", policy);
                else
                    Assert.True(policy == "two" || policy == context.Get<string>("before"), $"unexpected policy {policy}");
                await AssertContentAsync(context.Volume, "/pc", bytes, "policy changes never alter content");
            });
    }

    private static OperationDriver KeyRotationDriver()
    {
        byte[] before = TestData.Bytes(0x6321, 100_000);
        byte[] after = TestData.Bytes(0x6322, 100_000);
        return new OperationDriver(
            DfsOperationKind.KeyRotation,
            "rotate the volume data key, then write under the new key",
            [FaultSites.KeyRotation],
            DefaultOptions,
            context => context.Volume.Files.WriteAllBytesAsync("/kr", before, Ct),
            async (context, ct) =>
            {
                await context.Cluster.Admin.RotateDataKeyAsync(Vol, ct);
                await context.Volume.Files.WriteAllBytesAsync("/kr2", after, ct);
            },
            async (context, error) =>
            {
                await AssertContentAsync(context.Volume, "/kr", before, "data encrypted under the previous key stays readable");
                if (error is null)
                    await AssertContentAsync(context.Volume, "/kr2", after, "data written after rotation");
                await context.Volume.Files.WriteAllBytesAsync("/kr3", after, Ct);
                await AssertContentAsync(context.Volume, "/kr3", after, "writes work after recovery");
            });
    }

    private static OperationDriver AdminDriver()
    {
        byte[] bytes = TestData.Bytes(0x6331, 3000);
        var updated = new DfsInodeAccess(OwnerOnly: false, ReadOnlyForOthers: true);
        return new OperationDriver(
            DfsOperationKind.Admin,
            "change a file's access metadata",
            Sites(ClientStart, ClientWire, ServerWire, Propose),
            DefaultOptions,
            async context =>
            {
                await context.Volume.Files.WriteAllBytesAsync("/ad", bytes, Ct);
                context.State["before"] = (await context.Volume.Files.GetInfoAsync("/ad", Ct)).Access;
            },
            (context, ct) => context.Volume.Files.SetAccessAsync("/ad", updated, ct),
            async (context, error) =>
            {
                DfsInodeAccess access = (await context.Volume.Files.GetInfoAsync("/ad", Ct)).Access;
                if (error is null)
                    Assert.Equal(updated, access);
                else
                    Assert.True(access == updated || access == context.Get<DfsInodeAccess>("before"), $"unexpected access {access}");
                await AssertContentAsync(context.Volume, "/ad", bytes, "metadata changes never alter content");
            });
    }
}
