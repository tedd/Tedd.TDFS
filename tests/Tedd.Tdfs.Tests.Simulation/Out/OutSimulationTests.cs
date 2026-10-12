using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Out.OutHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Out;

/// <summary>
/// OUT — idempotency, cancellation and uncertain outcomes, in deterministic whole-cluster simulation
/// (FSD §13, §6.4; INV-01, INV-02, INV-06, INV-08).
/// </summary>
public sealed class OutSimulationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ================================================================================================================
    // OUT-002 SameIdSamePayloadSameOutcome
    // ================================================================================================================

    public enum RetryTiming
    {
        /// <summary>The original request is held in the network; the retry is delivered and executed first.</summary>
        BeforeOriginalDelivered,

        /// <summary>The original is accepted and blocked after the leader's local log append; the retry arrives meanwhile.</summary>
        WhileOriginalInFlight,

        /// <summary>The original completed and its reply was received; the retry follows.</summary>
        AfterOriginalCompleted,

        /// <summary>The original committed but its reply was lost; the retry follows.</summary>
        AfterOriginalReplyLost,
    }

    public static TheoryData<RetryOp, RetryTiming> RetryTimings
    {
        get
        {
            var data = new TheoryData<RetryOp, RetryTiming>();
            foreach (RetryOp op in Enum.GetValues<RetryOp>())
            {
                foreach (RetryTiming timing in Enum.GetValues<RetryTiming>())
                    data.Add(op, timing);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-002: a retry with the same id and payload before, during and after the original completes resolves to one
    /// durable outcome (identical token / result inode) and the effect exists exactly once.
    /// </summary>
    [Theory]
    [MemberData(nameof(RetryTimings))]
    [Catalog("OUT-002", "S", "INV-06", "INV-02", "INV-01")]
    public Task OUT_002_SameIdSamePayloadSameOutcome(RetryOp op, RetryTiming timing) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            RetryScenario scenario = Scenario(op);
            await scenario.Setup(v0, Ct);
            DfsOperationId id = await v0.NewOperationIdAsync(Ct);

            Attempt<Fingerprint> original;
            Fingerprint retry;
            switch (timing)
            {
                case RetryTiming.BeforeOriginalDelivered:
                {
                    using MessageHold hold = cluster.Network.Hold(new MessageFilter(From: ClientName("c0"), Type: scenario.RequestType, Direction: LinkDirection.Request));
                    Task<Fingerprint> originalTask = scenario.Invoke(v0, id, Ct);
                    await hold.WaitForAsync(1, Bounded);
                    retry = await scenario.Invoke(v1, id, Ct);
                    await scenario.AssertAppliedOnce(v1, Ct);   // the retry executed while the original was undelivered
                    hold.ReleaseAll();
                    original = await TryAsync(originalTask);
                    break;
                }

                case RetryTiming.WhileOriginalInFlight:
                {
                    TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
                    var accepted = new List<FaultContext>();
                    Capture(cluster, new FaultMatch(FaultSites.RpcRequestAccepted, Node: leader.Name, Operation: scenario.Operation), accepted);
                    FaultArm block = cluster.Faults.Arm(
                        new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: leader.Name, Operation: scenario.Operation), FaultAction.Block());
                    Task<Fingerprint> originalTask = scenario.Invoke(v0, id, Ct);
                    await block.WaitTriggeredAsync(Bounded);
                    Task<Fingerprint> retryTask = scenario.Invoke(v1, id, Ct);
                    // Release as soon as the leader has accepted the retry while the original is still in flight (a
                    // condition, not a fixed sleep: holding the leader's append longer than needed could force an election).
                    await cluster.WaitUntilAsync(() => Task.FromResult(Hits(accepted).Count(h => h.OperationId == id) >= 2), Bounded,
                        "the leader accepts the same-id retry while the original is in flight", Ct);
                    block.Release();
                    cluster.Faults.AssertReached(block);
                    retry = await retryTask;
                    original = await TryAsync(originalTask);
                    break;
                }

                case RetryTiming.AfterOriginalCompleted:
                    original = await TryAsync(() => scenario.Invoke(v0, id, Ct));
                    Assert.True(original.Succeeded, $"the undisturbed original must succeed: {original}");
                    retry = await scenario.Invoke(v1, id, Ct);
                    break;

                case RetryTiming.AfterOriginalReplyLost:
                {
                    MessageRule drop = cluster.Network.DropReplies(new MessageFilter(From: ClientName("c0"), Type: scenario.RequestType), count: 1);
                    original = await TryAsync(() => scenario.Invoke(v0, id, Ct));
                    Assert.Equal(1, drop.Matched);
                    retry = await scenario.Invoke(v1, id, Ct);
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(timing));
            }

            // The original either reports the same durable outcome or is uncertain; it never reports a different success
            // or a definite failure for an operation that committed.
            if (original.Succeeded)
                Assert.Equal(retry, original.Value);
            else
                Assert.Equal(OpOutcome.Unknown, original.Classification);

            Assert.False(retry.Token.IsNone);
            await AssertSingleCommittedEffectAsync(scenario, v1, id, retry.Token, Ct);

            // A further retry after everything settled still returns the same outcome.
            Assert.Equal(retry, await scenario.Invoke(v1, id, Ct));
            await AssertSingleCommittedEffectAsync(scenario, v1, id, retry.Token, Ct);
        }, Ct);

    // ================================================================================================================
    // OUT-003 SameIdDifferentPayloadRejected
    // ================================================================================================================

    public enum PayloadMismatch
    {
        AppendDifferentBytes,
        AppendDifferentPath,
        WriteAllDifferentBytes,
        WriteAllDifferentPath,
        WriteAllDifferentStoragePolicyOption,
        WriteAllDifferentCommitSnapshotOption,
        CopyDifferentDestination,
        CopyDifferentOverwriteOption,
        MoveDifferentDestination,
        DeleteDifferentPath,
        CreateDirectoryDifferentPath,
        DifferentOperationKindSamePath,
        DifferentClientDifferentBytes,
        TransactionDifferentDescriptor,
    }

    public static TheoryData<PayloadMismatch> PayloadMismatches => new(Enum.GetValues<PayloadMismatch>());

    /// <summary>
    /// OUT-003: reusing an accepted id with different bytes, path, options, operation kind or transaction descriptor is
    /// rejected with <see cref="DfsOperationIdMismatchException"/>; the original outcome stays resolvable and the
    /// mismatched request has no effect.
    /// </summary>
    [Theory]
    [MemberData(nameof(PayloadMismatches))]
    [Catalog("OUT-003", "S", "INV-06", "INV-08")]
    public Task OUT_003_SameIdDifferentPayloadRejected(PayloadMismatch mismatch) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            const string root = "/m";
            await v.Directories.CreateDirectoryAsync(root, Ct);
            await v.Files.WriteAllBytesAsync(root + "/log", Prefix, Ct);
            await v.Files.WriteAllBytesAsync(root + "/f", ContentA, Ct);
            await v.Files.WriteAllBytesAsync(root + "/g", ContentA, Ct);
            await v.Files.WriteAllBytesAsync(root + "/src", ContentA, Ct);
            await v.Files.WriteAllBytesAsync(root + "/victim", ContentA, Ct);
            await v.Files.WriteAllBytesAsync(root + "/victim2", ContentA, Ct);
            DfsOperationId id = await v.NewOperationIdAsync(Ct);
            var withId = new DfsMutationOptions { OperationId = id };

            Func<Task<CommitToken>> original;
            Func<Task> conflicting;
            Func<Task> assertOriginalOnly;
            switch (mismatch)
            {
                case PayloadMismatch.AppendDifferentBytes:
                    original = () => v.Files.AppendAllBytesAsync(root + "/log", Suffix, withId, Ct);
                    conflicting = () => v.Files.AppendAllBytesAsync(root + "/log", OtherSuffix, withId, Ct);
                    assertOriginalOnly = () => ExpectContentAsync(Concat(Prefix, Suffix), v.Files.ReadAllBytesAsync(root + "/log", Ct), "log");
                    break;
                case PayloadMismatch.AppendDifferentPath:
                    original = () => v.Files.AppendAllBytesAsync(root + "/log", Suffix, withId, Ct);
                    conflicting = () => v.Files.AppendAllBytesAsync(root + "/f", Suffix, withId, Ct);
                    assertOriginalOnly = async () =>
                    {
                        await ExpectContentAsync(Concat(Prefix, Suffix), v.Files.ReadAllBytesAsync(root + "/log", Ct), "log");
                        await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(root + "/f", Ct), "other path untouched");
                    };
                    break;
                case PayloadMismatch.WriteAllDifferentBytes:
                    original = () => v.Files.WriteAllBytesAsync(root + "/f", ContentB, withId, Ct);
                    conflicting = () => v.Files.WriteAllBytesAsync(root + "/f", Suffix, withId, Ct);
                    assertOriginalOnly = () => ExpectContentAsync(ContentB, v.Files.ReadAllBytesAsync(root + "/f", Ct), "f");
                    break;
                case PayloadMismatch.WriteAllDifferentPath:
                    original = () => v.Files.WriteAllBytesAsync(root + "/f", ContentB, withId, Ct);
                    conflicting = () => v.Files.WriteAllBytesAsync(root + "/g", ContentB, withId, Ct);
                    assertOriginalOnly = async () =>
                    {
                        await ExpectContentAsync(ContentB, v.Files.ReadAllBytesAsync(root + "/f", Ct), "f");
                        await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(root + "/g", Ct), "g untouched");
                    };
                    break;
                case PayloadMismatch.WriteAllDifferentStoragePolicyOption:
                    original = () => v.Files.WriteAllBytesAsync(root + "/new", ContentB, withId, Ct);
                    conflicting = () => v.Files.WriteAllBytesAsync(root + "/new", ContentB, withId with { StoragePolicy = AlternatePolicy }, Ct);
                    assertOriginalOnly = async () =>
                    {
                        await ExpectContentAsync(ContentB, v.Files.ReadAllBytesAsync(root + "/new", Ct), "new");
                        Assert.NotEqual(AlternatePolicy, (await v.Files.GetInfoAsync(root + "/new", Ct)).StoragePolicy);
                    };
                    break;
                case PayloadMismatch.WriteAllDifferentCommitSnapshotOption:
                    original = () => v.Files.WriteAllBytesAsync(root + "/f", ContentB, withId with { CommitSnapshotName = "s-original" }, Ct);
                    conflicting = () => v.Files.WriteAllBytesAsync(root + "/f", ContentB, withId with { CommitSnapshotName = "s-other" }, Ct);
                    assertOriginalOnly = async () =>
                    {
                        IReadOnlyList<DfsSnapshotInfo> snaps = await v.Snapshots.ListAsync(Ct);
                        Assert.Contains(snaps, s => s.Name == "s-original");
                        Assert.DoesNotContain(snaps, s => s.Name == "s-other");
                    };
                    break;
                case PayloadMismatch.CopyDifferentDestination:
                    original = () => v.Files.CopyAsync(root + "/src", root + "/dst1", false, withId, Ct);
                    conflicting = () => v.Files.CopyAsync(root + "/src", root + "/dst2", false, withId, Ct);
                    assertOriginalOnly = async () =>
                    {
                        await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(root + "/dst1", Ct), "dst1");
                        Assert.False(await v.Files.ExistsAsync(root + "/dst2", Ct));
                    };
                    break;
                case PayloadMismatch.CopyDifferentOverwriteOption:
                    original = () => v.Files.CopyAsync(root + "/src", root + "/dst1", false, withId, Ct);
                    conflicting = () => v.Files.CopyAsync(root + "/src", root + "/dst1", true, withId, Ct);
                    assertOriginalOnly = () => ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(root + "/dst1", Ct), "dst1");
                    break;
                case PayloadMismatch.MoveDifferentDestination:
                    original = () => v.Files.MoveAsync(root + "/f", root + "/moved1", false, withId, Ct);
                    conflicting = () => v.Files.MoveAsync(root + "/g", root + "/moved2", false, withId, Ct);
                    assertOriginalOnly = async () =>
                    {
                        Assert.True(await v.Files.ExistsAsync(root + "/moved1", Ct));
                        Assert.True(await v.Files.ExistsAsync(root + "/g", Ct));
                        Assert.False(await v.Files.ExistsAsync(root + "/moved2", Ct));
                    };
                    break;
                case PayloadMismatch.DeleteDifferentPath:
                    original = () => v.Files.DeleteAsync(root + "/victim", withId, Ct);
                    conflicting = () => v.Files.DeleteAsync(root + "/victim2", withId, Ct);
                    assertOriginalOnly = async () =>
                    {
                        Assert.False(await v.Files.ExistsAsync(root + "/victim", Ct));
                        await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(root + "/victim2", Ct), "victim2 survives");
                    };
                    break;
                case PayloadMismatch.CreateDirectoryDifferentPath:
                    original = async () =>
                    {
                        // CreateDirectory returns metadata (ObservedAt is "read at or after"), so the token is the outcome's.
                        DfsDirectoryInfo info = await v.Directories.CreateDirectoryAsync(root + "/d1", withId, Ct);
                        CommitToken created = (await v.QueryOperationAsync(id, resolve: false, Ct)).Token;
                        Assert.True(info.ObservedAt.IsAtOrAfter(created));
                        return created;
                    };
                    conflicting = () => v.Directories.CreateDirectoryAsync(root + "/d2", withId, Ct);
                    assertOriginalOnly = async () =>
                    {
                        Assert.True(await v.Directories.ExistsAsync(root + "/d1", Ct));
                        Assert.False(await v.Directories.ExistsAsync(root + "/d2", Ct));
                    };
                    break;
                case PayloadMismatch.DifferentOperationKindSamePath:
                    original = () => v.Files.AppendAllBytesAsync(root + "/log", Suffix, withId, Ct);
                    conflicting = () => v.Files.WriteAllBytesAsync(root + "/log", Suffix, withId, Ct);
                    assertOriginalOnly = () => ExpectContentAsync(Concat(Prefix, Suffix), v.Files.ReadAllBytesAsync(root + "/log", Ct), "log");
                    break;
                case PayloadMismatch.DifferentClientDifferentBytes:
                    original = () => v.Files.AppendAllBytesAsync(root + "/log", Suffix, withId, Ct);
                    conflicting = () => v1.Files.AppendAllBytesAsync(root + "/log", OtherSuffix, withId, Ct);
                    assertOriginalOnly = () => ExpectContentAsync(Concat(Prefix, Suffix), v.Files.ReadAllBytesAsync(root + "/log", Ct), "log");
                    break;
                case PayloadMismatch.TransactionDifferentDescriptor:
                {
                    var txOptions = new DfsTransactionOptions { OperationId = id, Timeout = TimeSpan.FromSeconds(30) };
                    original = async () =>
                    {
                        await using DfsTransaction tx = await v.Transactions.BeginAsync(txOptions, Ct);
                        Assert.Equal(id, tx.Id.Operation);
                        await tx.Files.WriteAllBytesAsync(root + "/tx-a", ContentA, Ct);
                        return await tx.CommitAsync(Ct);
                    };
                    conflicting = async () =>
                    {
                        await using DfsTransaction tx = await v.Transactions.BeginAsync(txOptions with { Timeout = TimeSpan.FromSeconds(45) }, Ct);
                        await tx.Files.WriteAllBytesAsync(root + "/tx-b", ContentB, Ct);
                        await tx.CommitAsync(Ct);
                    };
                    assertOriginalOnly = async () =>
                    {
                        await ExpectContentAsync(ContentA, v.Files.ReadAllBytesAsync(root + "/tx-a", Ct), "tx-a");
                        Assert.False(await v.Files.ExistsAsync(root + "/tx-b", Ct));
                        DfsOperationOutcome txOutcome = await v.Transactions.QueryOutcomeAsync(new DfsTransactionId(id), resolve: false, Ct);
                        Assert.Equal(DfsOutcomeStatus.Committed, txOutcome.Status);
                    };
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(mismatch));
            }

            CommitToken token = await original();
            Assert.False(token.IsNone);
            await assertOriginalOnly();

            DfsOperationIdMismatchException rejected = await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(conflicting, DfsErrorCode.OperationIdMismatch);
            Assert.False(rejected.IsRetryable, "an id/payload mismatch can never succeed on retry");

            // The original outcome is unchanged and still resolvable; the rejected request had no effect.
            await assertOriginalOnly();
            if (mismatch != PayloadMismatch.TransactionDifferentDescriptor)
            {
                DfsOperationOutcome outcome = await v1.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.Equal(token, outcome.Token);
            }

            // Rejection is stable: a second mismatched attempt is rejected the same way.
            await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(conflicting, DfsErrorCode.OperationIdMismatch);
            await assertOriginalOnly();
        }, Ct);

    // ================================================================================================================
    // OUT-004 SameIdDifferentPrincipalRejected
    // ================================================================================================================

    public enum PrincipalReuse
    {
        OtherWriterSamePayload,
        OtherWriterDifferentPayload,
        ReadOnlyPrincipalSamePayload,
        OtherWriterQueriesOutcome,
        OtherWriterResolvesOutcome,
        SamePrincipalOtherVolume,
    }

    public static TheoryData<PrincipalReuse> PrincipalReuses => new(Enum.GetValues<PrincipalReuse>());

    /// <summary>
    /// OUT-004: an id accepted for one principal cannot be replayed, re-bound or queried by another principal (nor on
    /// another volume): the attempt is rejected without disclosing the outcome and the original effect is unchanged.
    /// </summary>
    [Theory]
    [MemberData(nameof(PrincipalReuses))]
    [Catalog("OUT-004", "S", "INV-06", "INV-08")]
    public Task OUT_004_SameIdDifferentPrincipalRejected(PrincipalReuse reuse) =>
        ClusterTest.RunAsync(Sim(3, null, 0, "vol2"), async cluster =>
        {
            var writer = new Dictionary<string, DfsPermissions>(StringComparer.Ordinal)
            {
                [Volume] = DfsPermissions.Read | DfsPermissions.Write,
                ["vol2"] = DfsPermissions.Read | DfsPermissions.Write,
            };
            TestPrincipal alice = await cluster.CreatePrincipalAsync("alice", writer);
            TestPrincipal bob = await cluster.CreatePrincipalAsync("bob", writer);
            TestPrincipal carol = await cluster.CreatePrincipalAsync("carol",
                new Dictionary<string, DfsPermissions>(StringComparer.Ordinal) { [Volume] = DfsPermissions.Read });

            (DfsClient ca, DfsVolume va) = await ConnectAsync(cluster, "alice", Ct, alice.Credentials);
            await using var own0 = ca;
            await va.Files.WriteAllBytesAsync("/log", Prefix, Ct);
            DfsVolume va2 = ca.GetVolume("vol2");
            await va2.Files.WriteAllBytesAsync("/log", Prefix, Ct);

            DfsOperationId id = await va.NewOperationIdAsync(Ct);
            CommitToken token = await va.Files.AppendAllBytesAsync("/log", Suffix, new DfsMutationOptions { OperationId = id }, Ct);

            async Task AssertAliceStateUnchanged()
            {
                await ExpectContentAsync(Concat(Prefix, Suffix), va.Files.ReadAllBytesAsync("/log", Ct), "alice's log has her suffix exactly once");
                await ExpectContentAsync(Prefix, va2.Files.ReadAllBytesAsync("/log", Ct), "vol2 log untouched");
                DfsOperationOutcome own = await va.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, own.Status);
                Assert.Equal(token, own.Token);
            }

            switch (reuse)
            {
                case PrincipalReuse.OtherWriterSamePayload:
                case PrincipalReuse.OtherWriterDifferentPayload:
                {
                    (DfsClient cb, DfsVolume vb) = await ConnectAsync(cluster, "bob", Ct, bob.Credentials);
                    await using var own2 = cb;
                    byte[] payload = reuse == PrincipalReuse.OtherWriterSamePayload ? Suffix : OtherSuffix;
                    Attempt<CommitToken> replay = await TryAsync(() => vb.Files.AppendAllBytesAsync("/log", payload, new DfsMutationOptions { OperationId = id }, Ct));
                    Assert.False(replay.Succeeded, $"bob must not obtain alice's outcome or replay her id: {replay}");
                    Assert.True(IsCrossPrincipalRejection(replay.Error!), $"expected an authorization or id-mismatch rejection, got {replay.Error}");
                    break;
                }

                case PrincipalReuse.ReadOnlyPrincipalSamePayload:
                {
                    (DfsClient cc, DfsVolume vc) = await ConnectAsync(cluster, "carol", Ct, carol.Credentials);
                    await using var own2 = cc;
                    Attempt<CommitToken> replay = await TryAsync(() => vc.Files.AppendAllBytesAsync("/log", Suffix, new DfsMutationOptions { OperationId = id }, Ct));
                    Assert.False(replay.Succeeded, "a read-only principal must not obtain a write outcome by replaying an id");
                    Assert.True(replay.Error is UnauthorizedAccessException || IsCrossPrincipalRejection(replay.Error!),
                        $"expected UnauthorizedAccessException, got {replay.Error}");
                    break;
                }

                case PrincipalReuse.OtherWriterQueriesOutcome:
                case PrincipalReuse.OtherWriterResolvesOutcome:
                {
                    (DfsClient cb, DfsVolume vb) = await ConnectAsync(cluster, "bob", Ct, bob.Credentials);
                    await using var own2 = cb;
                    bool resolve = reuse == PrincipalReuse.OtherWriterResolvesOutcome;
                    Attempt<DfsOperationOutcome> query = await TryAsync(() => vb.QueryOperationAsync(id, resolve, Ct));
                    if (query.Succeeded)
                    {
                        // No disclosure: bob may only learn that nothing of his exists under this id.
                        Assert.Equal(DfsOutcomeStatus.NotFound, query.Value!.Status);
                        Assert.True(query.Value.Token.IsNone, "the outcome token of another principal's operation must not be disclosed");
                    }
                    else
                    {
                        Assert.True(IsCrossPrincipalRejection(query.Error!), $"expected an authorization rejection, got {query.Error}");
                    }

                    break;
                }

                case PrincipalReuse.SamePrincipalOtherVolume:
                {
                    Attempt<CommitToken> replay = await TryAsync(() => va2.Files.AppendAllBytesAsync("/log", Suffix, new DfsMutationOptions { OperationId = id }, Ct));
                    Assert.False(replay.Succeeded, "an id is bound to its volume and must not be replayed on another volume");
                    Assert.True(IsCrossPrincipalRejection(replay.Error!) || replay.Error is DfsOperationExpiredException or ArgumentException
                        || replay.Error is DfsException { ErrorCode: DfsErrorCode.InvalidArgument or DfsErrorCode.OperationExpired },
                        $"expected a namespace-binding rejection, got {replay.Error}");
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(reuse));
            }

            await AssertAliceStateUnchanged();
        }, Ct);

    // ================================================================================================================
    // OUT-005 ConcurrentRetriesCoalesce
    // ================================================================================================================

    public static TheoryData<int, RetryOp, bool> ConcurrentRetryCases
    {
        get
        {
            var data = new TheoryData<int, RetryOp, bool>();
            foreach (int nodes in new[] { 3, 5 })
            {
                foreach (RetryOp op in new[] { RetryOp.Append, RetryOp.WriteAll, RetryOp.Move, RetryOp.Copy, RetryOp.CreateDirectory })
                {
                    data.Add(nodes, op, false);
                    data.Add(nodes, op, true);
                }
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-005: the same mutation (same id, same payload) sent concurrently through every node endpoint — optionally with
    /// network duplication of each request — commits at most one logical effect; all definite answers report the same
    /// outcome and the rest are uncertain, never a conflicting success or failure.
    /// </summary>
    [Theory]
    [MemberData(nameof(ConcurrentRetryCases))]
    [Catalog("OUT-005", "S", "INV-06", "INV-02")]
    public Task OUT_005_ConcurrentRetriesCoalesce(int nodeCount, RetryOp op, bool duplicateRequests) =>
        ClusterTest.RunAsync(Sim(nodeCount), async cluster =>
        {
            var clients = new List<(DfsClient Client, DfsVolume Volume)>();
            try
            {
                foreach (TestNode node in cluster.Nodes)
                    clients.Add(await ConnectAsync(cluster, "c-" + node.Name, Ct, seedNodes: [node.Name]));
                RetryScenario scenario = Scenario(op);
                await scenario.Setup(clients[0].Volume, Ct);
                DfsOperationId id = await clients[0].Volume.NewOperationIdAsync(Ct);

                MessageRule? dup = duplicateRequests
                    ? cluster.Network.DuplicateNext(new MessageFilter(Type: scenario.RequestType, Direction: LinkDirection.Request), count: 4 * nodeCount)
                    : null;

                Attempt<Fingerprint>[] results = await Task.WhenAll(clients.Select(c => TryAsync(() => scenario.Invoke(c.Volume, id, Ct))));
                if (dup is not null)
                    Assert.True(dup.Matched > 0, "the duplication rule must have duplicated at least one request");

                Fingerprint[] successes = results.Where(r => r.Succeeded).Select(r => r.Value).ToArray();
                Assert.All(results.Where(r => !r.Succeeded), r => Assert.Equal(OpOutcome.Unknown, r.Classification));
                Assert.NotEmpty(successes);
                Assert.All(successes, s => Assert.Equal(successes[0], s));

                await AssertSingleCommittedEffectAsync(scenario, clients[0].Volume, id, successes[0].Token, Ct);
                await cluster.WaitForReplicationAsync(Volume, Ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            }
            finally
            {
                foreach ((DfsClient client, _) in clients)
                    await client.DisposeAsync();
            }
        }, Ct);

    // ================================================================================================================
    // OUT-006 AppendReplyLossDoesNotDuplicate (test plan §9.2)
    // ================================================================================================================

    public enum AppendReplyLossVariant
    {
        /// <summary>Healthy cluster; replies lost; client process dies; new client queries and retries.</summary>
        Basic,

        /// <summary>The leader crashes right after applying (before replying), forcing a new leader before the retry.</summary>
        LeaderCrashAfterApply,

        /// <summary>Metadata is compacted and a replica restarts from its checkpoint within the retained outcome window.</summary>
        AfterCompactionWithinWindow,

        /// <summary>The dead client's session expires and the outcome retention elapses: explicit expired-id rejection.</summary>
        BeyondRetentionWindow,
    }

    public static TheoryData<AppendReplyLossVariant, bool> AppendReplyLossCases
    {
        get
        {
            var data = new TheoryData<AppendReplyLossVariant, bool>();
            foreach (AppendReplyLossVariant variant in Enum.GetValues<AppendReplyLossVariant>())
            {
                data.Add(variant, false);
                data.Add(variant, true);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-006 / §9.2: append K is committed, every reply to the caller is lost and the caller process dies. A new client
    /// with the persisted K queries and retries: contents are exactly P + S (never P + S + S), query/retry report the
    /// original commit, changed-payload reuse is rejected, and the dead caller's uncertainty never became a rollback.
    /// Repeated after compaction within the window and beyond it (explicit expiry).
    /// </summary>
    [Theory]
    [MemberData(nameof(AppendReplyLossCases))]
    [Catalog("OUT-006", "S", "INV-06", "INV-02", "INV-01")]
    public Task OUT_006_AppendReplyLossDoesNotDuplicate(AppendReplyLossVariant variant, bool multiExtent) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            const string log = "/app/log";
            byte[] p = multiExtent ? TestData.Bytes(0x9200_01, 150 * 1024) : Prefix;
            byte[] s = multiExtent ? TestData.Bytes(0x9200_02, 70 * 1024 + 3) : Suffix;
            byte[] s2 = multiExtent ? TestData.Bytes(0x9200_03, 70 * 1024 + 3) : OtherSuffix;
            var history = new HistoryRecorder();

            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            await v0.Directories.CreateDirectoryAsync("/app", Ct);
            await history.RecordAsync("c0", "append", log, p, () => v0.Files.WriteAllBytesAsync(log, p, Ct));

            // The caller persists K and S before submitting.
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            string persistedK = k.ToString();

            var applied = new List<FaultContext>();
            FaultArm appliedArm = Capture(cluster, new FaultMatch(FaultSites.ApplyAfterBeforeReply, Operation: DfsOperationKind.Append), applied);
            MessageRule dropAll = cluster.Network.DropReplies(new MessageFilter(From: ClientName("c0"), Type: MessageType.Append), count: 10_000);
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            FaultArm? crash = variant == AppendReplyLossVariant.LeaderCrashAfterApply
                ? cluster.Faults.Arm(new FaultMatch(FaultSites.ApplyAfterBeforeReply, Node: leader.Name, Operation: DfsOperationKind.Append), FaultAction.CrashNode())
                : null;

            long appendOp = history.Invoke("c0", "append", log, s, k);
            Task<CommitToken> first = v0.Files.AppendAllBytesAsync(log, s, new DfsMutationOptions { OperationId = k }, Ct);

            // Durable metadata commit finished (the entry was applied on the leader) and its reply was lost (or the leader
            // died before replying) before the caller dies. Waiting for the drop itself avoids racing KillClient's
            // isolation against the reply transmission.
            await cluster.WaitUntilAsync(
                () => Task.FromResult(crash is not null ? crash.Triggered : Hits(applied).Length > 0 && dropAll.Matched > 0), Bounded,
                "append K is applied by the leader and its reply is lost", Ct);
            cluster.Faults.AssertReached(crash ?? appliedArm);
            Assert.True(dropAll.Matched > 0 || crash is not null, "the reply to the caller must have been lost");
            cluster.KillClient(c0);

            if (crash is not null)
            {
                cluster.Faults.AssertReached(crash);
                await cluster.WaitForStableAsync(Bounded, Ct);
                TestNode newLeader = await cluster.GetLeaderAsync(Volume, Ct);
                Assert.NotEqual(leader.Name, newLeader.Name);
                await leader.RestartAsync();
                await cluster.WaitForStableAsync(Bounded, Ct);
            }

            // The dead caller never learned a definite failure: its call is either still pending or uncertain.
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            if (first.IsCompleted)
            {
                Attempt<CommitToken> firstResult = await TryAsync(first);
                Assert.False(firstResult.Succeeded, "every reply to the first client was dropped");
                Assert.True(firstResult.Classification == OpOutcome.Unknown || firstResult.Error is ObjectDisposedException,
                    $"the first client's uncertainty must not be reported as a definite failure: {firstResult}");
            }

            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c1;
            DfsOperationId recoveredK = DfsOperationId.Parse(persistedK);
            Assert.Equal(k, recoveredK);

            if (variant == AppendReplyLossVariant.AfterCompactionWithinWindow)
            {
                DfsOperationOutcome before = await v1.QueryOperationAsync(recoveredK, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, before.Status);
                (DfsClient c2, DfsVolume v2) = await ConnectAsync(cluster, "load", Ct);
                await using var own1 = c2;
                await GenerateLoadAsync(v2, cluster.Options.Timings.RaftSnapshotThreshold + 50, Ct);
                await WaitForCompactionAsync(cluster, before.Token.Sequence, Bounded, Ct);
                // Every replica (whichever becomes leader and answers the query/retry) restarts from a checkpoint taken at or
                // beyond K's entry, so K's outcome must come from checkpointed state. Still within the outcome window.
                await CrashAndRestartAllAsync(cluster, Ct);
            }

            if (variant == AppendReplyLossVariant.BeyondRetentionWindow)
            {
                NodeTimings t = cluster.Options.Timings;
                await cluster.DelayAsync(t.SessionTimeout + t.OutcomeRetention + TimeSpan.FromSeconds(5), Ct);

                DfsOperationOutcome expired = await v1.QueryOperationAsync(recoveredK, resolve: false, Ct);
                Assert.Equal(recoveredK, expired.Id);
                Assert.Equal(DfsOutcomeStatus.Expired, expired.Status);
                await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                    () => v1.Files.AppendAllBytesAsync(log, s, new DfsMutationOptions { OperationId = recoveredK }, Ct), DfsErrorCode.OperationExpired);
                await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                    () => v1.Files.AppendAllBytesAsync(log, s2, new DfsMutationOptions { OperationId = recoveredK }, Ct), DfsErrorCode.OperationExpired);
                // The append itself is still durable (expiry of the id never rolls the commit back).
                history.Resolve(appendOp, OpOutcome.Ok);
            }
            else
            {
                DfsOperationOutcome outcome = await v1.QueryOperationAsync(recoveredK, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.False(outcome.Token.IsNone);
                history.Resolve(appendOp, OpOutcome.Ok);

                CommitToken retried = await v1.Files.AppendAllBytesAsync(log, s, new DfsMutationOptions { OperationId = recoveredK }, Ct);
                Assert.Equal(outcome.Token, retried);
                await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(
                    () => v1.Files.AppendAllBytesAsync(log, s2, new DfsMutationOptions { OperationId = recoveredK }, Ct), DfsErrorCode.OperationIdMismatch);
                Assert.Equal(outcome.Token.Sequence, (await v1.Files.GetInfoAsync(log, Ct)).Revision);
            }

            byte[] final = await history.RecordAsync("c1", "read", log, null, () => v1.Files.ReadAllBytesAsync(log, Ct));
            AssertEx.BytesEqual(Concat(p, s), final, "P + S exactly once, never P + S + S");
            LinearizabilityChecker.Check(history.Operations, new AppendLogModel()).AssertValid();
        }, Ct);

    // ================================================================================================================
    // OUT-007 CreateRetryAfterNameReuse
    // ================================================================================================================

    public enum CreateKind
    {
        WriteAllCreatesFile,
        OpenCreateNew,
        CreateDirectory,
        CopyCreatesDestination,
    }

    public static TheoryData<CreateKind, bool> CreateReuseCases
    {
        get
        {
            var data = new TheoryData<CreateKind, bool>();
            foreach (CreateKind kind in Enum.GetValues<CreateKind>())
            {
                data.Add(kind, false);
                data.Add(kind, true);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-007: create with id K commits (reply received or lost), the name is deleted and recreated as a new inode, then K
    /// is retried: the retry resolves to the original event (same token / original inode) and never modifies or collides
    /// with the new incarnation.
    /// </summary>
    [Theory]
    [MemberData(nameof(CreateReuseCases))]
    [Catalog("OUT-007", "S", "INV-06", "INV-02")]
    public Task OUT_007_CreateRetryAfterNameReuse(CreateKind kind, bool originalReplyLost) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            const string path = "/c/x";
            byte[] original = ContentA;
            byte[] newer = ContentB;
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            await v0.Directories.CreateDirectoryAsync("/c", Ct);
            await v0.Files.WriteAllBytesAsync("/c/src", original, Ct);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            bool isDirectory = kind == CreateKind.CreateDirectory;

            MessageType requestType = kind switch
            {
                CreateKind.WriteAllCreatesFile => MessageType.WriteAll,
                CreateKind.OpenCreateNew => MessageType.OpenHandle,
                CreateKind.CreateDirectory => MessageType.CreateDirectory,
                _ => MessageType.Copy,
            };
            MessageRule? drop = originalReplyLost
                ? cluster.Network.DropReplies(new MessageFilter(From: ClientName("c0"), Type: requestType), count: 10_000)
                : null;

            async Task<(CommitToken Token, InodeId Inode, DfsStream? Stream)> CreateAsync(DfsVolume v)
            {
                switch (kind)
                {
                    case CreateKind.WriteAllCreatesFile:
                    {
                        CommitToken t = await v.Files.WriteAllBytesAsync(path, original, new DfsMutationOptions { OperationId = k }, Ct);
                        return (t, default, null);
                    }

                    case CreateKind.OpenCreateNew:
                    {
                        DfsStream stream = await v.Files.OpenAsync(path, new DfsOpenOptions
                        {
                            Mode = FileMode.CreateNew, Access = FileAccess.ReadWrite, Share = FileShare.Read, OperationId = k,
                        }, Ct);
                        return (CommitToken.None, stream.InodeId, stream);
                    }

                    case CreateKind.CreateDirectory:
                    {
                        DfsDirectoryInfo info = await v.Directories.CreateDirectoryAsync(path, new DfsMutationOptions { OperationId = k }, Ct);
                        return (info.ObservedAt, info.InodeId, null);
                    }

                    default:
                    {
                        CommitToken t = await v.Files.CopyAsync("/c/src", path, false, new DfsMutationOptions { OperationId = k }, Ct);
                        return (t, default, null);
                    }
                }
            }

            Attempt<(CommitToken Token, InodeId Inode, DfsStream? Stream)> first = await TryAsync(() => CreateAsync(v0));
            if (first.Succeeded && first.Value.Stream is not null)
                await first.Value.Stream.DisposeAsync();
            if (originalReplyLost)
            {
                Assert.True(drop!.Matched > 0);
                if (!first.Succeeded)
                    Assert.Equal(OpOutcome.Unknown, first.Classification);
            }
            else
            {
                Assert.True(first.Succeeded, $"undisturbed create must succeed: {first}");
            }

            drop?.Cancel();
            if (kind == CreateKind.OpenCreateNew && !first.Succeeded)
            {
                // The handle created by the uncertain open is unknown to its client and is never renewed: its lease (and
                // its share reservation, which would block the delete below) must expire server-side.
                await cluster.DelayAsync(cluster.Options.Timings.LeaseDuration * 2, Ct);
            }

            DfsOperationOutcome committed = await v1.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, committed.Status);
            InodeId originalInode = isDirectory
                ? (await v1.Directories.GetInfoAsync(path, Ct)).InodeId
                : (await v1.Files.GetInfoAsync(path, Ct)).InodeId;

            // Delete and recreate the name: a new inode J with different contents.
            InodeId recreated;
            ulong recreatedRevision;
            if (isDirectory)
            {
                await v1.Directories.DeleteAsync(path, recursive: true, Ct);
                DfsDirectoryInfo again = await v1.Directories.CreateDirectoryAsync(path, Ct);
                await v1.Files.WriteAllBytesAsync(path + "/child", newer, Ct);
                DfsDirectoryInfo info = await v1.Directories.GetInfoAsync(path, Ct);
                recreated = again.InodeId;
                recreatedRevision = info.Revision;
            }
            else
            {
                await v1.Files.DeleteAsync(path, Ct);
                await v1.Files.WriteAllBytesAsync(path, newer, Ct);
                DfsFileInfo info = await v1.Files.GetInfoAsync(path, Ct);
                recreated = info.InodeId;
                recreatedRevision = info.Revision;
            }

            Assert.NotEqual(originalInode, recreated);

            // Retry the original create id.
            Attempt<(CommitToken Token, InodeId Inode, DfsStream? Stream)> retry = await TryAsync(() => CreateAsync(v1));
            switch (kind)
            {
                case CreateKind.WriteAllCreatesFile:
                case CreateKind.CopyCreatesDestination:
                    Assert.True(retry.Succeeded, $"the retry must resolve to the original commit: {retry}");
                    Assert.Equal(committed.Token, retry.Value.Token);
                    break;
                case CreateKind.CreateDirectory:
                    Assert.True(retry.Succeeded, $"the retry must resolve to the original commit: {retry}");
                    Assert.Equal(originalInode, retry.Value.Inode);
                    break;
                case CreateKind.OpenCreateNew:
                    if (retry.Succeeded)
                    {
                        // A replayed open may only describe the original incarnation, never the new occupant. Its handle is
                        // the original (closed or expired) one, so releasing it may legitimately fail: not asserted.
                        Assert.Equal(originalInode, retry.Value.Inode);
                        await TryAsync(() => retry.Value.Stream!.DisposeAsync().AsTask());
                    }
                    else
                    {
                        Assert.False(retry.Error is DfsAlreadyExistsException,
                            "the retry must resolve the original event, not re-execute CreateNew against the current occupant");
                        Assert.True(retry.Error is DfsException
                            {
                                ErrorCode: DfsErrorCode.InvalidHandle or DfsErrorCode.LeaseLost or DfsErrorCode.PinExpired or DfsErrorCode.OperationExpired,
                            },
                            $"unexpected answer to a replayed create-open: {retry.Error}");
                    }

                    break;
            }

            // The new incarnation is untouched.
            if (isDirectory)
            {
                DfsDirectoryInfo now = await v1.Directories.GetInfoAsync(path, Ct);
                Assert.Equal(recreated, now.InodeId);
                Assert.Equal(recreatedRevision, now.Revision);
                await ExpectContentAsync(newer, v1.Files.ReadAllBytesAsync(path + "/child", Ct), "child of the new directory");
            }
            else
            {
                DfsFileInfo now = await v1.Files.GetInfoAsync(path, Ct);
                Assert.Equal(recreated, now.InodeId);
                Assert.Equal(recreatedRevision, now.Revision);
                await ExpectContentAsync(newer, v1.Files.ReadAllBytesAsync(path, Ct), "new incarnation contents");
            }

            DfsOperationOutcome after = await v1.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, after.Status);
            Assert.Equal(committed.Token, after.Token);
        }, Ct);

    // ================================================================================================================
    // OUT-008 DeleteRetryAfterNameReuse
    // ================================================================================================================

    public enum DeleteKind
    {
        File,
        EmptyDirectory,
        RecursiveDirectory,
    }

    public static TheoryData<DeleteKind, bool> DeleteReuseCases
    {
        get
        {
            var data = new TheoryData<DeleteKind, bool>();
            foreach (DeleteKind kind in Enum.GetValues<DeleteKind>())
            {
                data.Add(kind, false);
                data.Add(kind, true);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-008: delete K commits, a new inode is created at the path, then K is retried: the retry returns the original
    /// delete outcome and the new inode (identity, revision and contents) survives.
    /// </summary>
    [Theory]
    [MemberData(nameof(DeleteReuseCases))]
    [Catalog("OUT-008", "S", "INV-06", "INV-02")]
    public Task OUT_008_DeleteRetryAfterNameReuse(DeleteKind kind, bool originalReplyLost) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            const string path = "/d/x";
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            await v0.Directories.CreateDirectoryAsync("/d", Ct);
            bool isDirectory = kind != DeleteKind.File;
            switch (kind)
            {
                case DeleteKind.File:
                    await v0.Files.WriteAllBytesAsync(path, ContentA, Ct);
                    break;
                case DeleteKind.EmptyDirectory:
                    await v0.Directories.CreateDirectoryAsync(path, Ct);
                    break;
                case DeleteKind.RecursiveDirectory:
                    await v0.Directories.CreateDirectoryAsync(path + "/sub", Ct);
                    await v0.Files.WriteAllBytesAsync(path + "/sub/leaf", ContentA, Ct);
                    break;
            }

            InodeId oldInode = isDirectory ? (await v0.Directories.GetInfoAsync(path, Ct)).InodeId : (await v0.Files.GetInfoAsync(path, Ct)).InodeId;
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            var withK = new DfsMutationOptions { OperationId = k };
            Task<CommitToken> DeleteAsync(DfsVolume v) => isDirectory
                ? v.Directories.DeleteAsync(path, kind == DeleteKind.RecursiveDirectory, withK, Ct)
                : v.Files.DeleteAsync(path, withK, Ct);

            MessageRule? drop = originalReplyLost
                ? cluster.Network.DropReplies(new MessageFilter(From: ClientName("c0"), Type: MessageType.Delete), count: 10_000)
                : null;
            Attempt<CommitToken> first = await TryAsync(() => DeleteAsync(v0));
            if (originalReplyLost)
            {
                Assert.True(drop!.Matched > 0);
                if (!first.Succeeded)
                    Assert.Equal(OpOutcome.Unknown, first.Classification);
                drop.Cancel();
            }
            else
            {
                Assert.True(first.Succeeded, $"undisturbed delete must succeed: {first}");
            }

            DfsOperationOutcome committed = await v1.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, committed.Status);
            if (first.Succeeded)
                Assert.Equal(first.Value, committed.Token);

            // Recreate the name as a new inode.
            InodeId newInode;
            ulong newRevision;
            if (isDirectory)
            {
                newInode = (await v1.Directories.CreateDirectoryAsync(path, Ct)).InodeId;
                await v1.Files.WriteAllBytesAsync(path + "/fresh", ContentB, Ct);
                newRevision = (await v1.Directories.GetInfoAsync(path, Ct)).Revision;
            }
            else
            {
                await v1.Files.WriteAllBytesAsync(path, ContentB, Ct);
                DfsFileInfo info = await v1.Files.GetInfoAsync(path, Ct);
                newInode = info.InodeId;
                newRevision = info.Revision;
            }

            Assert.NotEqual(oldInode, newInode);

            CommitToken retried = await DeleteAsync(v1);
            Assert.Equal(committed.Token, retried);

            if (isDirectory)
            {
                DfsDirectoryInfo now = await v1.Directories.GetInfoAsync(path, Ct);
                Assert.Equal(newInode, now.InodeId);
                Assert.Equal(newRevision, now.Revision);
                await ExpectContentAsync(ContentB, v1.Files.ReadAllBytesAsync(path + "/fresh", Ct), "file in the new directory");
                Assert.False(await v1.Files.ExistsAsync(path + "/sub/leaf", Ct), "the deleted subtree must not resurrect");
            }
            else
            {
                DfsFileInfo now = await v1.Files.GetInfoAsync(path, Ct);
                Assert.Equal(newInode, now.InodeId);
                Assert.Equal(newRevision, now.Revision);
                await ExpectContentAsync(ContentB, v1.Files.ReadAllBytesAsync(path, Ct), "new inode survives");
            }
        }, Ct);

    // ================================================================================================================
    // OUT-009 RenameRetryAfterFurtherMoves
    // ================================================================================================================

    public enum RenameKind
    {
        File,
        Directory,
        FileOverwritingDestination,
    }

    public static TheoryData<RenameKind, bool> RenameReuseCases
    {
        get
        {
            var data = new TheoryData<RenameKind, bool>();
            foreach (RenameKind kind in Enum.GetValues<RenameKind>())
            {
                data.Add(kind, false);
                data.Add(kind, true);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-009: rename K (/a → /b) commits; the moved inode is moved on to /c and new inodes occupy /a and /b; retrying K
    /// returns the original event and does not move the current occupants.
    /// </summary>
    [Theory]
    [MemberData(nameof(RenameReuseCases))]
    [Catalog("OUT-009", "S", "INV-06", "INV-02")]
    public Task OUT_009_RenameRetryAfterFurtherMoves(RenameKind kind, bool originalReplyLost) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            const string a = "/n/a", b = "/n/b", c = "/n/c";
            bool dir = kind == RenameKind.Directory;
            await v0.Directories.CreateDirectoryAsync("/n", Ct);

            async Task<InodeId> InodeOf(DfsVolume v, string p) =>
                dir ? (await v.Directories.GetInfoAsync(p, Ct)).InodeId : (await v.Files.GetInfoAsync(p, Ct)).InodeId;

            if (dir)
            {
                await v0.Directories.CreateDirectoryAsync(a, Ct);
                await v0.Files.WriteAllBytesAsync(a + "/inner", ContentA, Ct);
            }
            else
            {
                await v0.Files.WriteAllBytesAsync(a, ContentA, Ct);
            }

            InodeId? overwritten = null;
            if (kind == RenameKind.FileOverwritingDestination)
            {
                await v0.Files.WriteAllBytesAsync(b, ContentB, Ct);
                overwritten = await InodeOf(v0, b);
            }

            InodeId moved = await InodeOf(v0, a);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            var withK = new DfsMutationOptions { OperationId = k };
            Task<CommitToken> RenameAsync(DfsVolume v) => dir
                ? v.Directories.MoveAsync(a, b, withK, Ct)
                : v.Files.MoveAsync(a, b, kind == RenameKind.FileOverwritingDestination, withK, Ct);

            MessageRule? drop = originalReplyLost
                ? cluster.Network.DropReplies(new MessageFilter(From: ClientName("c0"), Type: MessageType.Move), count: 10_000)
                : null;
            Attempt<CommitToken> first = await TryAsync(() => RenameAsync(v0));
            if (originalReplyLost)
            {
                Assert.True(drop!.Matched > 0);
                if (!first.Succeeded)
                    Assert.Equal(OpOutcome.Unknown, first.Classification);
                drop.Cancel();
            }
            else
            {
                Assert.True(first.Succeeded, $"undisturbed rename must succeed: {first}");
            }

            DfsOperationOutcome committed = await v1.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, committed.Status);

            // Further moves and recreations.
            if (dir)
            {
                await v1.Directories.MoveAsync(b, c, Ct);
                await v1.Directories.CreateDirectoryAsync(a, Ct);
                await v1.Directories.CreateDirectoryAsync(b, Ct);
            }
            else
            {
                await v1.Files.MoveAsync(b, c, false, Ct);
                await v1.Files.WriteAllBytesAsync(a, Suffix, Ct);
                await v1.Files.WriteAllBytesAsync(b, OtherSuffix, Ct);
            }

            InodeId newA = await InodeOf(v1, a);
            InodeId newB = await InodeOf(v1, b);

            CommitToken retried = await RenameAsync(v1);
            Assert.Equal(committed.Token, retried);

            Assert.Equal(moved, await InodeOf(v1, c));
            Assert.Equal(newA, await InodeOf(v1, a));
            Assert.Equal(newB, await InodeOf(v1, b));
            if (dir)
            {
                await ExpectContentAsync(ContentA, v1.Files.ReadAllBytesAsync(c + "/inner", Ct), "moved directory contents");
                Assert.False(await v1.Files.ExistsAsync(b + "/inner", Ct));
            }
            else
            {
                await ExpectContentAsync(ContentA, v1.Files.ReadAllBytesAsync(c, Ct), "original inode at its later path");
                await ExpectContentAsync(Suffix, v1.Files.ReadAllBytesAsync(a, Ct), "new /a untouched");
                await ExpectContentAsync(OtherSuffix, v1.Files.ReadAllBytesAsync(b, Ct), "new /b untouched");
            }

            if (overwritten is not null)
            {
                // The inode replaced by the original overwrite stays gone; the retry did not overwrite anything again.
                Assert.NotEqual(overwritten.Value, await InodeOf(v1, a));
                Assert.NotEqual(overwritten.Value, await InodeOf(v1, b));
                Assert.NotEqual(overwritten.Value, await InodeOf(v1, c));
            }
        }, Ct);

    // ================================================================================================================
    // OUT-010 ClientCrashCanRecoverSavedId
    // ================================================================================================================

    public enum ClientCrashPoint
    {
        /// <summary>The request never left the network before the client died.</summary>
        BeforeDelivery,

        /// <summary>The request was accepted and blocked before its metadata proposal when the client died.</summary>
        InFlightBeforeProposal,

        /// <summary>The request committed; the reply was lost and the client died.</summary>
        AfterCommitBeforeReply,
    }

    public static TheoryData<ClientCrashPoint, RetryOp> ClientCrashCases
    {
        get
        {
            var data = new TheoryData<ClientCrashPoint, RetryOp>();
            foreach (ClientCrashPoint point in Enum.GetValues<ClientCrashPoint>())
            {
                data.Add(point, RetryOp.Append);
                data.Add(point, RetryOp.WriteAll);
                data.Add(point, RetryOp.Move);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-010: the client process dies after sending; a new client using the persisted id can obtain the durable
    /// decision (resolving pending/not-found to a definite answer), and the history including the dead client's
    /// operation is linearizable once narrowed.
    /// </summary>
    [Theory]
    [MemberData(nameof(ClientCrashCases))]
    [Catalog("OUT-010", "S", "INV-06", "INV-01")]
    public Task OUT_010_ClientCrashCanRecoverSavedId(ClientCrashPoint point, RetryOp op) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            RetryScenario scenario = Scenario(op);
            await scenario.Setup(v0, Ct);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            string persisted = k.ToString();

            MessageHold? hold = null;
            FaultArm? block = null;
            MessageRule? drop = null;
            var applied = new List<FaultContext>();
            switch (point)
            {
                case ClientCrashPoint.BeforeDelivery:
                    hold = cluster.Network.Hold(new MessageFilter(From: ClientName("c0"), Type: scenario.RequestType, Direction: LinkDirection.Request));
                    break;
                case ClientCrashPoint.InFlightBeforeProposal:
                {
                    TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
                    block = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftBeforePropose, Node: leader.Name, Operation: scenario.Operation), FaultAction.Block());
                    break;
                }

                case ClientCrashPoint.AfterCommitBeforeReply:
                    drop = cluster.Network.DropReplies(new MessageFilter(From: ClientName("c0"), Type: scenario.RequestType), count: 10_000);
                    Capture(cluster, new FaultMatch(FaultSites.ApplyAfterBeforeReply, Operation: scenario.Operation), applied);
                    break;
            }

            _ = scenario.Invoke(v0, k, Ct);
            if (hold is not null)
                await hold.WaitForAsync(1, Bounded);
            if (block is not null)
                await block.WaitTriggeredAsync(Bounded);
            if (drop is not null)
            {
                // Wait for the lost reply itself, so KillClient's isolation cannot race the reply transmission.
                await cluster.WaitUntilAsync(() => Task.FromResult(Hits(applied).Length > 0 && drop.Matched > 0), Bounded,
                    "the mutation is applied and its reply is lost", Ct);
            }

            cluster.KillClient(c0);

            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c1;
            DfsOperationId saved = DfsOperationId.Parse(persisted);

            switch (point)
            {
                case ClientCrashPoint.BeforeDelivery:
                {
                    DfsOperationOutcome unseen = await v1.QueryOperationAsync(saved, resolve: false, Ct);
                    Assert.True(unseen.Status is DfsOutcomeStatus.NotFound or DfsOutcomeStatus.Pending,
                        $"an undelivered operation is not seen (or pending), never decided: {unseen.Status}");
                    DfsOperationOutcome decided = AssertDefinite(await v1.QueryOperationAsync(saved, resolve: true, Ct), saved);
                    Assert.Equal(DfsOutcomeStatus.Aborted, decided.Status);
                    hold!.ReleaseAll();
                    await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
                    await scenario.AssertNotApplied(v1, Ct);
                    Assert.Equal(DfsOutcomeStatus.Aborted, (await v1.QueryOperationAsync(saved, resolve: false, Ct)).Status);
                    break;
                }

                case ClientCrashPoint.InFlightBeforeProposal:
                {
                    DfsOperationOutcome pending = await v1.QueryOperationAsync(saved, resolve: false, Ct);
                    Assert.True(pending.Status is DfsOutcomeStatus.Pending or DfsOutcomeStatus.NotFound,
                        $"an in-flight operation must not be reported as decided: {pending.Status}");
                    block!.Release();
                    cluster.Faults.AssertReached(block);
                    DfsOperationOutcome final = AssertDefinite(await v1.QueryOperationAsync(saved, resolve: true, Ct), saved);
                    if (final.Status == DfsOutcomeStatus.Committed)
                        await AssertSingleCommittedEffectAsync(scenario, v1, saved, final.Token, Ct);
                    else
                        await scenario.AssertNotApplied(v1, Ct);
                    break;
                }

                case ClientCrashPoint.AfterCommitBeforeReply:
                {
                    Assert.True(drop!.Matched > 0, "the reply must have been lost");
                    DfsOperationOutcome committed = await v1.QueryOperationAsync(saved, resolve: false, Ct);
                    Assert.Equal(DfsOutcomeStatus.Committed, committed.Status);
                    await AssertSingleCommittedEffectAsync(scenario, v1, saved, committed.Token, Ct);
                    // The new client can also retry with the saved id and gets the same decision.
                    Fingerprint retried = await scenario.Invoke(v1, saved, Ct);
                    Assert.Equal(committed.Token, retried.Token);
                    await AssertSingleCommittedEffectAsync(scenario, v1, saved, committed.Token, Ct);
                    break;
                }
            }

            hold?.Dispose();
        }, Ct);

    // ================================================================================================================
    // OUT-011 OutcomeSurvivesConsensusCompaction
    // ================================================================================================================

    public enum CompactionRecovery
    {
        CompactionOnly,
        FollowerRestartsFromCheckpoint,
        AllNodesCrashAndRestart,
        LaggingReplicaInstallsCheckpoint,
    }

    public static TheoryData<CompactionRecovery> CompactionRecoveries => new(Enum.GetValues<CompactionRecovery>());

    /// <summary>
    /// OUT-011: outcomes of committed (and definitely failed) operations survive log compaction, checkpoint restart,
    /// whole-cluster crash and checkpoint installation on a lagging replica: queries return the same tokens, same-payload
    /// retries return them without re-executing, and mismatched payloads are still rejected.
    /// </summary>
    [Theory]
    [MemberData(nameof(CompactionRecoveries))]
    [Catalog("OUT-011", "S", "INV-06", "INV-01")]
    public Task OUT_011_OutcomeSurvivesConsensusCompaction(CompactionRecovery recovery) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            TestNode? lagging = null;
            if (recovery == CompactionRecovery.LaggingReplicaInstallsCheckpoint)
            {
                lagging = await GetFollowerAsync(cluster, Ct);
                await lagging.StopAsync();
            }

            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            await using var own0 = c0;
            RetryScenario[] scenarios = Enum.GetValues<RetryOp>().Select(op => Scenario(op, "/o/" + op)).ToArray();
            await v0.Directories.CreateDirectoryAsync("/o", Ct);
            var committed = new List<(RetryScenario Scenario, DfsOperationId Id, Fingerprint Result)>();
            foreach (RetryScenario scenario in scenarios)
            {
                await scenario.Setup(v0, Ct);
                DfsOperationId id = await v0.NewOperationIdAsync(Ct);
                committed.Add((scenario, id, await scenario.Invoke(v0, id, Ct)));
            }

            // A definitely failed operation (source absent) also keeps a non-committed answer.
            DfsOperationId failedId = await v0.NewOperationIdAsync(Ct);
            await Assert.ThrowsAnyAsync<FileNotFoundException>(() => v0.Files.MoveAsync("/o/missing", "/o/never", false, new DfsMutationOptions { OperationId = failedId }, Ct));

            ulong highest = committed.Max(c => c.Result.Token.Sequence);

            // Compact from a different session so the outcomes above are not retired by their own session's window.
            (DfsClient load, DfsVolume lv) = await ConnectAsync(cluster, "load", Ct);
            await using var own1 = load;
            await GenerateLoadAsync(lv, cluster.Options.Timings.RaftSnapshotThreshold + 60, Ct);
            await WaitForCompactionAsync(cluster, highest, Bounded, Ct);

            switch (recovery)
            {
                case CompactionRecovery.FollowerRestartsFromCheckpoint:
                {
                    TestNode follower = await GetFollowerAsync(cluster, Ct);
                    await follower.CrashAsync();
                    await follower.RestartAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    // Stop a replica other than the restarted one (the leader, unless the restarted replica leads): the
                    // checkpoint-restored replica must now be part of every serving quorum.
                    TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
                    TestNode other = leader.Name != follower.Name ? leader : cluster.Nodes.First(n => n.Name != follower.Name);
                    await other.StopAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    break;
                }

                case CompactionRecovery.AllNodesCrashAndRestart:
                    await CrashAndRestartAllAsync(cluster, Ct);
                    break;

                case CompactionRecovery.LaggingReplicaInstallsCheckpoint:
                {
                    FaultArm install = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftSnapshotInstall, Node: lagging!.Name), FaultAction.Delay(TimeSpan.Zero));
                    await lagging.RestartAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    await cluster.WaitForReplicationAsync(Volume, Ct);
                    cluster.Faults.AssertReached(install);
                    await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
                    TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
                    if (leader.Name != lagging.Name)
                    {
                        await leader.StopAsync();
                        await cluster.WaitForStableAsync(Bounded, Ct);
                    }

                    break;
                }
            }

            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own2 = c1;
            foreach ((RetryScenario scenario, DfsOperationId id, Fingerprint result) in committed)
            {
                DfsOperationOutcome outcome = await v1.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.Equal(result.Token, outcome.Token);
                Assert.Equal(result, await scenario.Invoke(v1, id, Ct));
                await scenario.AssertAppliedOnce(v1, Ct);
            }

            DfsOperationOutcome failed = await v1.QueryOperationAsync(failedId, resolve: false, Ct);
            Assert.True(failed.Status is DfsOutcomeStatus.Aborted or DfsOutcomeStatus.NotFound,
                $"a definitely failed operation must never resolve as committed after compaction: {failed.Status}");
            Assert.False(await v1.Files.ExistsAsync("/o/never", Ct));

            // Payload binding survives compaction too.
            (RetryScenario appendScenario, DfsOperationId appendId, _) = committed.Single(c => c.Scenario.Op == RetryOp.Append);
            await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(
                () => v1.Files.AppendAllBytesAsync(appendScenario.Root + "/log", OtherSuffix, new DfsMutationOptions { OperationId = appendId }, Ct),
                DfsErrorCode.OperationIdMismatch);
        }, Ct);

    // ================================================================================================================
    // OUT-012 ExpiredIdCannotBecomeNewRequest
    // ================================================================================================================

    public enum RetirementPath
    {
        /// <summary>The owning session expires (client dead) and the outcome retention elapses.</summary>
        SessionExpiredAndRetentionElapsed,

        /// <summary>The owning session performs more operations than its outcome window retains.</summary>
        OutcomeWindowExceeded,

        /// <summary>The id was reserved but never used; its session then expired.</summary>
        ReservedUnusedInExpiredSession,

        /// <summary>The id was reserved but never used; later operations of the same session retired the sequence range.</summary>
        ReservedUnusedBelowRetiredWatermark,
    }

    public static TheoryData<RetirementPath, RetryOp> RetirementCases
    {
        get
        {
            var data = new TheoryData<RetirementPath, RetryOp>();
            foreach (RetirementPath path in Enum.GetValues<RetirementPath>())
            {
                data.Add(path, RetryOp.Append);
                data.Add(path, RetryOp.WriteAll);
                data.Add(path, RetryOp.CreateDirectory);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-012: once an id's outcome is retired (session expiry + retention, window overflow) or its sequence lies below
    /// the retired watermark, a replay is rejected with <see cref="DfsOperationExpiredException"/> and reported as
    /// Expired — never executed as a new request and never reported as NotFound.
    /// </summary>
    [Theory]
    [MemberData(nameof(RetirementCases))]
    [Catalog("OUT-012", "S", "INV-06", "INV-08")]
    public Task OUT_012_ExpiredIdCannotBecomeNewRequest(RetirementPath path, RetryOp op) =>
        ClusterTest.RunAsync(Sim(3, SmallOutcomeWindow()), async cluster =>
        {
            NodeTimings timings = cluster.Options.Timings;
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            RetryScenario scenario = Scenario(op);
            await scenario.Setup(v0, Ct);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            bool used = path is RetirementPath.SessionExpiredAndRetentionElapsed or RetirementPath.OutcomeWindowExceeded;
            Fingerprint? originalResult = used ? await scenario.Invoke(v0, k, Ct) : null;

            switch (path)
            {
                case RetirementPath.SessionExpiredAndRetentionElapsed:
                case RetirementPath.ReservedUnusedInExpiredSession:
                    cluster.KillClient(c0);
                    await cluster.DelayAsync(timings.SessionTimeout + timings.OutcomeRetention + TimeSpan.FromSeconds(5), Ct);
                    break;
                case RetirementPath.OutcomeWindowExceeded:
                case RetirementPath.ReservedUnusedBelowRetiredWatermark:
                    await v0.Directories.CreateDirectoryAsync("/filler", Ct);
                    for (int i = 0; i <= timings.MaxOutcomesPerSession + 1; i++)
                        await v0.Files.WriteAllBytesAsync("/filler/" + i, TestData.Bytes((ulong)i, 32), new DfsMutationOptions { OperationId = await v0.NewOperationIdAsync(Ct) }, Ct);
                    break;
            }

            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c1;

            DfsOperationOutcome status = await v1.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(k, status.Id);
            Assert.Equal(DfsOutcomeStatus.Expired, status.Status);

            DfsOperationExpiredException expired = await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                () => scenario.Invoke(v1, k, Ct), DfsErrorCode.OperationExpired);
            Assert.False(expired.IsRetryable);
            Assert.Equal(DfsOutcomeStatus.Expired, (await v1.QueryOperationAsync(k, resolve: true, Ct)).Status);

            if (used)
            {
                // The original effect exists exactly once; the replay did not execute again.
                await scenario.AssertAppliedOnce(v1, Ct);
                if (scenario.RevisionPath is not null)
                    Assert.Equal(originalResult!.Value.Token.Sequence, (await v1.Files.GetInfoAsync(scenario.RevisionPath, Ct)).Revision);
            }
            else
            {
                await scenario.AssertNotApplied(v1, Ct);
            }

            if (path is RetirementPath.OutcomeWindowExceeded or RetirementPath.ReservedUnusedBelowRetiredWatermark)
                await c0.DisposeAsync();
        }, Ct);

    // ================================================================================================================
    // OUT-013 DefiniteAbortPreventsLateCommit
    // ================================================================================================================

    public enum DelayedPath
    {
        HeldInNetworkThenReleased,
        BlockedBeforeProposalThenReleased,
        DelayedInNetworkPastDecision,
        CapturedFrameReplayedAfterDecision,
        LeaderChangeBetweenDecisionAndRelease,
    }

    public static TheoryData<DelayedPath, RetryOp> DelayedPathCases
    {
        get
        {
            var data = new TheoryData<DelayedPath, RetryOp>();
            foreach (DelayedPath delayed in Enum.GetValues<DelayedPath>())
            {
                data.Add(delayed, RetryOp.Append);
                data.Add(delayed, RetryOp.WriteAll);
                data.Add(delayed, RetryOp.Move);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-013: after an authoritative abort (resolve:true while the original is delayed), every delayed path of the
    /// original — held, blocked before proposal, delayed, replayed frame, delivered after a leader change — fails and
    /// never commits; the decision stays Aborted.
    /// </summary>
    [Theory]
    [MemberData(nameof(DelayedPathCases))]
    [Catalog("OUT-013", "S", "INV-06", "INV-02")]
    public Task OUT_013_DefiniteAbortPreventsLateCommit(DelayedPath delayed, RetryOp op) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            RetryScenario scenario = Scenario(op);
            await scenario.Setup(v0, Ct);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            var requestFilter = new MessageFilter(From: ClientName("c0"), Type: scenario.RequestType, Direction: LinkDirection.Request);

            MessageHold? hold = null;
            FaultArm? block = null;
            MessageRule? delay = null;
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            switch (delayed)
            {
                case DelayedPath.HeldInNetworkThenReleased:
                case DelayedPath.CapturedFrameReplayedAfterDecision:
                case DelayedPath.LeaderChangeBetweenDecisionAndRelease:
                    hold = cluster.Network.Hold(requestFilter);
                    break;
                case DelayedPath.BlockedBeforeProposalThenReleased:
                    block = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftBeforePropose, Node: leader.Name, Operation: scenario.Operation), FaultAction.Block());
                    break;
                case DelayedPath.DelayedInNetworkPastDecision:
                    // Every copy of the request (including SDK retries with the same id) is delayed past the decision.
                    delay = cluster.Network.DelayNext(requestFilter, TimeSpan.FromSeconds(6), count: 1_000);
                    break;
            }

            Task<Fingerprint> originalTask = scenario.Invoke(v0, k, Ct);
            if (hold is not null)
                await hold.WaitForAsync(1, Bounded);
            if (block is not null)
                await block.WaitTriggeredAsync(Bounded);
            if (delay is not null)
                await cluster.WaitUntilAsync(() => Task.FromResult(delay.Matched > 0), Bounded, "the original request is delayed in flight", Ct);

            // Authoritative decision while the original is still delayed.
            DfsOperationOutcome decision = AssertDefinite(await v1.QueryOperationAsync(k, resolve: true, Ct), k);
            Assert.Equal(DfsOutcomeStatus.Aborted, decision.Status);
            await scenario.AssertNotApplied(v1, Ct);

            switch (delayed)
            {
                case DelayedPath.HeldInNetworkThenReleased:
                    hold!.ReleaseAll();
                    hold.Dispose();   // stop capturing: later retries are delivered too
                    break;
                case DelayedPath.BlockedBeforeProposalThenReleased:
                    block!.Release();
                    cluster.Faults.AssertReached(block);
                    break;
                case DelayedPath.DelayedInNetworkPastDecision:
                    await cluster.DelayAsync(TimeSpan.FromSeconds(7), Ct);
                    break;
                case DelayedPath.CapturedFrameReplayedAfterDecision:
                {
                    CapturedMessage frame = hold!.Captured.First(m => m.Type == scenario.RequestType);
                    hold.DropAll();
                    DfsErrorCode replayed = await cluster.Network.ReplayAsync(frame);
                    Assert.NotEqual(DfsErrorCode.None, replayed);
                    break;
                }

                case DelayedPath.LeaderChangeBetweenDecisionAndRelease:
                    await leader.CrashAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    Assert.NotEqual(leader.Name, (await cluster.GetLeaderAsync(Volume, Ct)).Name);
                    await leader.RestartAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    hold!.ReleaseAll();
                    hold.Dispose();
                    break;
            }

            Attempt<Fingerprint> original = await TryAsync(originalTask);
            AssertNotSuccess(original, "the fenced original must not report success");

            // No late commit, now or later (long enough for every still-delayed copy of the request to arrive).
            await cluster.DelayAsync(TimeSpan.FromSeconds(delayed == DelayedPath.DelayedInNetworkPastDecision ? 7 : 3), Ct);
            if (delay is not null)
                delay.Cancel();
            await scenario.AssertNotApplied(v1, Ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, (await v1.QueryOperationAsync(k, resolve: false, Ct)).Status);
            Attempt<Fingerprint> lateRetry = await TryAsync(() => scenario.Invoke(v1, k, Ct));
            AssertNotSuccess(lateRetry, "a retry of a definitely aborted id must not commit");
            await scenario.AssertNotApplied(v1, Ct);
            hold?.Dispose();
        }, Ct);

    /// <summary>OUT-013 (transactions): a transaction commit fenced by an authoritative abort cannot commit when released.</summary>
    [Theory]
    [InlineData(DelayedPath.HeldInNetworkThenReleased)]
    [InlineData(DelayedPath.BlockedBeforeProposalThenReleased)]
    [Catalog("OUT-013", "S", "INV-06", "INV-02")]
    public Task OUT_013_DefiniteAbortPreventsLateCommit_TransactionCommit(DelayedPath delayed) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            await v0.Files.WriteAllBytesAsync("/t1", ContentA, Ct);
            DfsTransaction tx = await v0.Transactions.BeginAsync(null, Ct);
            await tx.Files.WriteAllBytesAsync("/t1", ContentB, Ct);
            await tx.Files.WriteAllBytesAsync("/t2", ContentB, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            MessageHold? hold = null;
            FaultArm? block = null;
            if (delayed == DelayedPath.HeldInNetworkThenReleased)
                hold = cluster.Network.Hold(new MessageFilter(From: ClientName("c0"), Type: MessageType.TxCommit, Direction: LinkDirection.Request));
            else
                block = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftBeforePropose, Node: leader.Name, Operation: DfsOperationKind.TxCommit), FaultAction.Block());

            Task<CommitToken> commit = tx.CommitAsync(Ct);
            if (hold is not null)
                await hold.WaitForAsync(1, Bounded);
            else
                await block!.WaitTriggeredAsync(Bounded);

            DfsOperationOutcome decision = await v1.Transactions.QueryOutcomeAsync(tx.Id, resolve: true, Ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, decision.Status);

            hold?.ReleaseAll();
            block?.Release();
            Attempt<CommitToken> result = await TryAsync(commit);
            AssertNotSuccess(result, "a fenced transaction commit must not report success");
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            await ExpectContentAsync(ContentA, v1.Files.ReadAllBytesAsync("/t1", Ct), "no partial or late transaction effect");
            Assert.False(await v1.Files.ExistsAsync("/t2", Ct));
            Assert.Equal(DfsOutcomeStatus.Aborted, (await v1.Transactions.QueryOutcomeAsync(tx.Id, resolve: false, Ct)).Status);
            await TryAsync(() => tx.DisposeAsync().AsTask());   // disposal of the fenced transaction is not the property
            hold?.Dispose();
        }, Ct);

    // ================================================================================================================
    // OUT-014 PendingIsNotNotFound
    // ================================================================================================================

    public enum QueryPoint
    {
        NeverSent,
        HeldInNetwork,
        BlockedBeforeProposal,
        BlockedAfterLocalAppend,
        BlockedAfterCommitBeforeApply,
        BlockedAfterApplyBeforeReply,
    }

    public static TheoryData<QueryPoint> QueryPoints => new(Enum.GetValues<QueryPoint>());

    /// <summary>
    /// OUT-014: queries of an operation that is in flight never answer NotFound or Aborted (pending/indeterminate is not
    /// absence); NotFound is reserved for ids never seen and does not fence them (the operation may still commit later);
    /// the unresolved operation stays Unknown in the history until a definite answer narrows it.
    /// </summary>
    [Theory]
    [MemberData(nameof(QueryPoints))]
    [Catalog("OUT-014", "S", "INV-06", "INV-08")]
    public Task OUT_014_PendingIsNotNotFound(QueryPoint point) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct, operationTimeout: TimeSpan.FromSeconds(3));
            await using var own0 = c0;
            await using var own1 = c1;
            const string path = "/q/f";
            await v0.Directories.CreateDirectoryAsync("/q", Ct);
            await v0.Files.WriteAllBytesAsync(path, ContentA, Ct);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            var history = new HistoryRecorder();

            if (point == QueryPoint.NeverSent)
            {
                DfsOperationOutcome absent = await v1.QueryOperationAsync(k, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.NotFound, absent.Status);
                Assert.True(absent.Token.IsNone);
                // NotFound is not a fence: the id can still be used and commit.
                CommitToken t = await v0.Files.WriteAllBytesAsync(path, ContentB, new DfsMutationOptions { OperationId = k }, Ct);
                DfsOperationOutcome committedLater = await v1.QueryOperationAsync(k, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, committedLater.Status);
                Assert.Equal(t, committedLater.Token);
                return;
            }

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            MessageHold? hold = null;
            FaultArm? block = null;
            string? site = point switch
            {
                QueryPoint.BlockedBeforeProposal => FaultSites.RaftBeforePropose,
                QueryPoint.BlockedAfterLocalAppend => FaultSites.RaftAfterLocalAppend,
                QueryPoint.BlockedAfterCommitBeforeApply => FaultSites.RaftAfterCommitBeforeApply,
                QueryPoint.BlockedAfterApplyBeforeReply => FaultSites.ApplyAfterBeforeReply,
                _ => null,
            };
            if (site is null)
                hold = cluster.Network.Hold(new MessageFilter(From: ClientName("c0"), Type: MessageType.WriteAll, Direction: LinkDirection.Request));
            else
                block = cluster.Faults.Arm(new FaultMatch(site, Node: leader.Name, Operation: DfsOperationKind.WriteAll), FaultAction.Block());

            long opIndex = history.Invoke("c0", "write", path, ContentB, k);
            Task<CommitToken> write = v0.Files.WriteAllBytesAsync(path, ContentB, new DfsMutationOptions { OperationId = k }, Ct);
            if (hold is not null)
                await hold.WaitForAsync(1, Bounded);
            else
                await block!.WaitTriggeredAsync(Bounded);

            Attempt<DfsOperationOutcome> query = await TryAsync(() => v1.QueryOperationAsync(k, resolve: false, Ct));
            if (query.Succeeded)
            {
                DfsOutcomeStatus status = query.Value!.Status;
                Assert.NotEqual(DfsOutcomeStatus.Aborted, status);
                Assert.NotEqual(DfsOutcomeStatus.Expired, status);
                switch (point)
                {
                    case QueryPoint.HeldInNetwork:
                        Assert.True(status is DfsOutcomeStatus.NotFound or DfsOutcomeStatus.Pending, $"an unseen request is NotFound/Pending, got {status}");
                        break;
                    case QueryPoint.BlockedAfterApplyBeforeReply:
                        Assert.Equal(DfsOutcomeStatus.Committed, status);
                        break;
                    default:
                        Assert.True(status is DfsOutcomeStatus.Pending or DfsOutcomeStatus.Committed,
                            $"an accepted in-flight operation must be Pending (or Committed once decided), never {status}");
                        break;
                }

                if (status == DfsOutcomeStatus.Pending)
                    Assert.True(query.Value.Token.IsNone);
            }
            else
            {
                // Not answering (no authority/apply progress) is legal; inventing absence or abort is not.
                Assert.True(query.Error is DfsUnavailableException or TimeoutException || query.Classification == OpOutcome.Unknown,
                    $"a query may fail with an availability error but must not misreport: {query.Error}");
            }

            // Still unresolved in the history.
            Assert.Equal(OpOutcome.Unknown, history.Get(opIndex).Outcome);

            hold?.ReleaseAll();
            block?.Release();
            if (block is not null)
                cluster.Faults.AssertReached(block);
            Attempt<CommitToken> result = await TryAsync(write);

            DfsOperationOutcome final = AssertDefinite(await v1.QueryOperationAsync(k, resolve: true, Ct), k);
            if (point == QueryPoint.BlockedAfterApplyBeforeReply)
                Assert.Equal(DfsOutcomeStatus.Committed, final.Status);   // it was already applied when queried
            if (result.Succeeded)
            {
                Assert.Equal(DfsOutcomeStatus.Committed, final.Status);
                Assert.Equal(final.Token, result.Value);
                history.Complete(opIndex, OpOutcome.Ok);
            }
            else
            {
                // A held/blocked leader may lose leadership meanwhile; the SDK may then report the outcome as unknown, and
                // the authoritative resolution decides it (the pending answer above was never treated as an abort).
                Assert.Equal(OpOutcome.Unknown, result.Classification);
                history.Complete(opIndex, OpOutcome.Unknown, result.Error);
                history.Resolve(opIndex, final.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);
            }

            byte[] read = await history.RecordAsync("c1", "read", path, null, () => v1.Files.ReadAllBytesAsync(path, Ct));
            AssertEx.BytesEqual(final.Status == DfsOutcomeStatus.Committed ? ContentB : ContentA, read, "visible state matches the resolved outcome");
            LinearizabilityChecker.Check(history.Operations, new RegisterModel(ContentA)).AssertValid();
            hold?.Dispose();
        }, Ct);

    // ================================================================================================================
    // OUT-016 CancellationBeforePublication
    // ================================================================================================================

    public enum CancelPoint
    {
        TokenAlreadyCancelled,
        AtOperationStart,
        AfterLocalValidation,
        DuringConnectOrReuse,
        AfterTransmissionHeldInNetwork,
    }

    public enum CancelledMutation
    {
        WriteAll,
        Append,
        StreamCommit,
        TransactionCommit,
    }

    public static TheoryData<CancelPoint, CancelledMutation> CancelBeforeCases
    {
        get
        {
            var data = new TheoryData<CancelPoint, CancelledMutation>();
            foreach (CancelPoint point in Enum.GetValues<CancelPoint>())
            {
                foreach (CancelledMutation m in Enum.GetValues<CancelledMutation>())
                    data.Add(point, m);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-016: cancellation before publication. Before transmission (F00–F02) the SDK reports a definite
    /// <see cref="OperationCanceledException"/> and the server never saw the operation; after transmission it may only
    /// report cancellation once the abort is confirmed (otherwise the outcome is unknown). After the confirmed abort no
    /// delayed path — held request, retry with the same id — can publish under the cancelled decision.
    /// </summary>
    [Theory]
    [MemberData(nameof(CancelBeforeCases))]
    [Catalog("OUT-016", "S", "INV-06", "INV-02", "INV-08")]
    public Task OUT_016_CancellationBeforePublication(CancelPoint point, CancelledMutation mutation) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            const string path = "/x/f";
            await v0.Directories.CreateDirectoryAsync("/x", Ct);
            await v0.Files.WriteAllBytesAsync(path, ContentA, Ct);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);

            DfsStream? stream = null;
            DfsTransaction? tx = null;
            DfsOperationKind kind;
            MessageType requestType;
            switch (mutation)
            {
                case CancelledMutation.WriteAll:
                    kind = DfsOperationKind.WriteAll;
                    requestType = MessageType.WriteAll;
                    break;
                case CancelledMutation.Append:
                    kind = DfsOperationKind.Append;
                    requestType = MessageType.Append;
                    break;
                case CancelledMutation.StreamCommit:
                    kind = DfsOperationKind.CommitFile;
                    requestType = MessageType.CommitFile;
                    stream = await v0.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
                    await stream.WriteAsync(ContentB, Ct);
                    break;
                default:
                    kind = DfsOperationKind.TxCommit;
                    requestType = MessageType.TxCommit;
                    tx = await v0.Transactions.BeginAsync(null, Ct);
                    await tx.Files.WriteAllBytesAsync(path, ContentB, Ct);
                    await tx.Files.WriteAllBytesAsync("/x/g", ContentB, Ct);
                    break;
            }

            Task Invoke(DfsVolume v, CancellationToken token) => mutation switch
            {
                CancelledMutation.WriteAll => v.Files.WriteAllBytesAsync(path, ContentB, new DfsMutationOptions { OperationId = k }, token),
                CancelledMutation.Append => v.Files.AppendAllBytesAsync(path, Suffix, new DfsMutationOptions { OperationId = k }, token),
                CancelledMutation.StreamCommit => stream!.CommitAsync(new DfsMutationOptions { OperationId = k }, token),
                _ => tx!.CommitAsync(token),
            };

            async Task AssertUnpublished()
            {
                // The cancelled stream's writer handle may still be open: read with a writer-compatible share mode.
                await ExpectContentAsync(ContentA, ReadSharedAsync(v1, path, Ct), "committed state unchanged by a cancelled operation");
                Assert.False(await v1.Files.ExistsAsync("/x/g", Ct));
            }

            async Task<DfsOperationOutcome> ResolveAsync() => tx is not null
                ? await v1.Transactions.QueryOutcomeAsync(tx.Id, resolve: true, Ct)
                : await v1.QueryOperationAsync(k, resolve: true, Ct);

            var accepted = new List<FaultContext>();
            FaultArm acceptedArm = Capture(cluster, new FaultMatch(FaultSites.RpcRequestAccepted, Operation: kind), accepted);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            MessageHold? hold = null;
            FaultArm? cancelArm = null;
            Task invocation;
            switch (point)
            {
                case CancelPoint.TokenAlreadyCancelled:
                    cts.Cancel();
                    invocation = Invoke(v0, cts.Token);
                    break;
                case CancelPoint.AtOperationStart:
                case CancelPoint.AfterLocalValidation:
                case CancelPoint.DuringConnectOrReuse:
                {
                    string site = point switch
                    {
                        CancelPoint.AtOperationStart => FaultSites.ClientOperationStart,
                        CancelPoint.AfterLocalValidation => FaultSites.ClientValidated,
                        _ => FaultSites.ClientConnect,
                    };
                    cancelArm = cluster.Faults.Arm(new FaultMatch(site, Operation: kind), FaultAction.CancelCaller());
                    invocation = Invoke(v0, cts.Token);
                    break;
                }

                default:
                    hold = cluster.Network.Hold(new MessageFilter(From: ClientName("c0"), Type: requestType, Direction: LinkDirection.Request));
                    invocation = Invoke(v0, cts.Token);
                    await hold.WaitForAsync(1, Bounded);
                    cts.Cancel();
                    break;
            }

            Attempt<bool> result = await TryAsync(() => invocation);
            Assert.False(result.Succeeded, "a cancelled operation must not report success before publication");
            if (cancelArm is not null)
                cluster.Faults.AssertReached(cancelArm);

            if (point == CancelPoint.AfterTransmissionHeldInNetwork)
            {
                // Transmitted: cancellation may only be reported as definite after a confirmed (fenced) abort.
                Assert.True(result.Error is OperationCanceledException || result.Classification == OpOutcome.Unknown,
                    $"after transmission the SDK must report cancellation (abort confirmed) or an unknown outcome: {result.Error}");
                if (result.Error is OperationCanceledException)
                    Assert.Equal(DfsOutcomeStatus.Aborted, (tx is not null
                        ? await v1.Transactions.QueryOutcomeAsync(tx.Id, resolve: false, Ct)
                        : await v1.QueryOperationAsync(k, resolve: false, Ct)).Status);
            }
            else
            {
                Assert.IsAssignableFrom<OperationCanceledException>(result.Error);
                Assert.DoesNotContain(Hits(accepted), h => h.OperationId == k);
                Assert.DoesNotContain(Hits(accepted), h => h.Operation == kind);
            }

            // Confirm the decision authoritatively, then release every delayed path.
            DfsOperationOutcome decision = await ResolveAsync();
            Assert.Equal(DfsOutcomeStatus.Aborted, decision.Status);
            hold?.ReleaseAll();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            await AssertUnpublished();
            if (tx is null)
            {
                Attempt<bool> replay = await TryAsync(() => Invoke(v1, Ct));
                AssertNotSuccess(replay, "a replay under the cancelled (aborted) decision must not publish");
            }
            else
            {
                Attempt<CommitToken> recommit = await TryAsync(() => tx.CommitAsync(Ct));
                AssertNotSuccess(recommit, "a transaction whose commit was confirmed aborted cannot commit later");
            }

            await AssertUnpublished();

            // The server-side observation point itself is live: the replay (or the released held request) reached it, so the
            // absence of earlier hits for the cancelled call is meaningful evidence rather than an unobserved site.
            if (tx is null || point == CancelPoint.AfterTransmissionHeldInNetwork)
                cluster.Faults.AssertReached(acceptedArm);
            if (stream is not null)
                await TryAsync(() => stream.AbortAsync(Ct));
            if (tx is not null)
                await TryAsync(() => tx.DisposeAsync().AsTask());
            hold?.Dispose();
        }, Ct);

    // ================================================================================================================
    // OUT-017 CancellationCannotRollBackCommit
    // ================================================================================================================

    public enum CancelAfterDecision
    {
        CommittedBeforeApply,
        AppliedBeforeReply,
        DuringReplyTransmission,
        AfterClientReceipt,
    }

    public static TheoryData<CancelAfterDecision, CancelledMutation> CancelAfterCases
    {
        get
        {
            var data = new TheoryData<CancelAfterDecision, CancelledMutation>();
            foreach (CancelAfterDecision point in Enum.GetValues<CancelAfterDecision>())
            {
                foreach (CancelledMutation m in Enum.GetValues<CancelledMutation>())
                    data.Add(point, m);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-017: cancellation after the durable decision (F11–F14) never surfaces as a definite cancel; the caller sees
    /// success or an unknown outcome, resolution reports Committed with the same token, and the contents survive a crash
    /// of every node.
    /// </summary>
    [Theory]
    [MemberData(nameof(CancelAfterCases))]
    [Catalog("OUT-017", "S", "INV-06", "INV-01", "INV-08")]
    public Task OUT_017_CancellationCannotRollBackCommit(CancelAfterDecision point, CancelledMutation mutation) =>
        ClusterTest.RunAsync(Sim(), async cluster =>
        {
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            const string path = "/y/f";
            await v0.Directories.CreateDirectoryAsync("/y", Ct);
            await v0.Files.WriteAllBytesAsync(path, ContentA, Ct);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            byte[] expected = mutation == CancelledMutation.Append ? Concat(ContentA, Suffix) : ContentB;

            DfsStream? stream = null;
            DfsTransaction? tx = null;
            DfsOperationKind kind = mutation switch
            {
                CancelledMutation.WriteAll => DfsOperationKind.WriteAll,
                CancelledMutation.Append => DfsOperationKind.Append,
                CancelledMutation.StreamCommit => DfsOperationKind.CommitFile,
                _ => DfsOperationKind.TxCommit,
            };
            if (mutation == CancelledMutation.StreamCommit)
            {
                stream = await v0.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Truncate, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
                await stream.WriteAsync(ContentB, Ct);
            }
            else if (mutation == CancelledMutation.TransactionCommit)
            {
                tx = await v0.Transactions.BeginAsync(null, Ct);
                await tx.Files.WriteAllBytesAsync(path, ContentB, Ct);
            }

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            FaultAction cancel = FaultAction.Custom(_ =>
            {
                cts.Cancel();
                return Task.CompletedTask;
            });
            FaultArm arm = point switch
            {
                CancelAfterDecision.CommittedBeforeApply => cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: leader.Name, Operation: kind), cancel),
                CancelAfterDecision.AppliedBeforeReply => cluster.Faults.Arm(new FaultMatch(FaultSites.ApplyAfterBeforeReply, Node: leader.Name, Operation: kind), cancel),
                CancelAfterDecision.DuringReplyTransmission => cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Node: leader.Name, Operation: kind), cancel),
                _ => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientReplyReceived, Operation: kind), FaultAction.CancelCaller()),
            };

            Attempt<CommitToken> result = await TryAsync(() => mutation switch
            {
                CancelledMutation.WriteAll => v0.Files.WriteAllBytesAsync(path, ContentB, new DfsMutationOptions { OperationId = k }, cts.Token),
                CancelledMutation.Append => v0.Files.AppendAllBytesAsync(path, Suffix, new DfsMutationOptions { OperationId = k }, cts.Token),
                CancelledMutation.StreamCommit => stream!.CommitAsync(new DfsMutationOptions { OperationId = k }, cts.Token),
                _ => tx!.CommitAsync(cts.Token),
            });
            cluster.Faults.AssertReached(arm);
            Assert.False(result.Error is OperationCanceledException,
                $"cancellation after the durable decision must not be reported as a definite cancel: {result.Error}");
            if (!result.Succeeded)
            {
                DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(result.Error);
                if (tx is not null)
                    Assert.Equal(tx.Id, unknown.TransactionId);
                else
                    Assert.Equal(k, unknown.OperationId);
            }

            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c1;
            DfsOperationOutcome outcome = tx is not null
                ? await v1.Transactions.QueryOutcomeAsync(tx.Id, resolve: true, Ct)
                : await v1.QueryOperationAsync(k, resolve: true, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            if (result.Succeeded)
                Assert.Equal(result.Value, outcome.Token);
            // The stream (if any) is still open here: read with a writer-compatible share mode.
            await ExpectContentAsync(expected, ReadSharedAsync(v1, path, Ct), "committed contents after cancellation");

            if (stream is not null)
                await TryAsync(() => stream.DisposeAsync().AsTask());
            if (tx is not null)
                await TryAsync(() => tx.DisposeAsync().AsTask());
            await c0.DisposeAsync();

            // Durable: survives a crash of every node.
            await CrashAndRestartAllAsync(cluster, Ct);
            (DfsClient c2, DfsVolume v2) = await ConnectAsync(cluster, "c2", Ct);
            await using var own1 = c2;
            await ExpectContentAsync(expected, v2.Files.ReadAllBytesAsync(path, Ct), "committed contents after whole-cluster crash");
            DfsOperationOutcome afterCrash = tx is not null
                ? await v2.Transactions.QueryOutcomeAsync(tx.Id, resolve: false, Ct)
                : await v2.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, afterCrash.Status);
            Assert.Equal(outcome.Token, afterCrash.Token);
        }, Ct);

    // ================================================================================================================
    // OUT-019 OutcomeWindowRetirementIsDurable
    // ================================================================================================================

    public enum RetirementCrash
    {
        CrashAllAfterExpiryObserved,
        LeaderCrashAfterLocalAppendOfRetiringEntry,
        LeaderCrashAfterCommitOfRetiringEntry,
        CrashAgainDuringJournalReplay,
        CompactThenCrashAllAfterExpiry,
        SessionExpiryCrashAllJustBeforeDeadline,
        SessionExpiryCrashAllJustAfterDeadline,
    }

    /// <summary>
    /// Which further WriteAll of the session retires K depends on how many of the session's outcomes are still retained
    /// (setup operations, acknowledgement watermark), so the leader-crash variants crash at every candidate entry 1..W+2
    /// (W = per-session outcome window); the others take no occurrence (0).
    /// </summary>
    public static TheoryData<RetirementCrash, int> RetirementCrashes
    {
        get
        {
            var data = new TheoryData<RetirementCrash, int>();
            int window = SmallOutcomeWindow().MaxOutcomesPerSession;
            foreach (RetirementCrash crash in Enum.GetValues<RetirementCrash>())
            {
                if (crash is RetirementCrash.LeaderCrashAfterLocalAppendOfRetiringEntry or RetirementCrash.LeaderCrashAfterCommitOfRetiringEntry)
                {
                    for (int occurrence = 1; occurrence <= window + 2; occurrence++)
                        data.Add(crash, occurrence);
                }
                else
                {
                    data.Add(crash, 0);
                }
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-019: crashes while the session/outcome retirement watermark advances (and around session-expiry deadlines,
    /// during journal replay and after compaction) never re-enable a replay: once Expired was observed it stays Expired,
    /// and in every branch the original effect exists exactly once (never re-executed as a new request).
    /// </summary>
    [Theory]
    [MemberData(nameof(RetirementCrashes))]
    [Catalog("OUT-019", "S", "INV-06", "INV-01")]
    public Task OUT_019_OutcomeWindowRetirementIsDurable(RetirementCrash crash, int crashOccurrence) =>
        ClusterTest.RunAsync(Sim(3, SmallOutcomeWindow()), async cluster =>
        {
            NodeTimings timings = cluster.Options.Timings;
            (DfsClient c0, DfsVolume v0) = await ConnectAsync(cluster, "c0", Ct);
            RetryScenario scenario = Scenario(RetryOp.Append);
            await scenario.Setup(v0, Ct);
            DfsOperationId k = await v0.NewOperationIdAsync(Ct);
            CommitToken token = (await scenario.Invoke(v0, k, Ct)).Token;
            await v0.Directories.CreateDirectoryAsync("/filler", Ct);
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            bool expiryObservedBeforeCrash = false;

            async Task FillWindowAsync(DfsVolume v, int count)
            {
                for (int i = 0; i < count; i++)
                    await TryAsync(() => v.Files.WriteAllBytesAsync("/filler/" + i, TestData.Bytes((ulong)i, 32), Ct));
            }

            async Task<bool> ObservedExpiredAsync(DfsVolume v) =>
                (await v.QueryOperationAsync(k, resolve: false, Ct)).Status == DfsOutcomeStatus.Expired;

            switch (crash)
            {
                case RetirementCrash.CrashAllAfterExpiryObserved:
                case RetirementCrash.CrashAgainDuringJournalReplay:
                case RetirementCrash.CompactThenCrashAllAfterExpiry:
                {
                    await FillWindowAsync(v0, timings.MaxOutcomesPerSession + 2);
                    Assert.True(await ObservedExpiredAsync(v0), "the window overflow must retire K");
                    expiryObservedBeforeCrash = true;
                    if (crash == RetirementCrash.CompactThenCrashAllAfterExpiry)
                    {
                        (DfsClient load, DfsVolume lv) = await ConnectAsync(cluster, "load", Ct);
                        await GenerateLoadAsync(lv, timings.RaftSnapshotThreshold + 60, Ct);
                        await load.DisposeAsync();
                        await WaitForCompactionAsync(cluster, token.Sequence + (ulong)timings.MaxOutcomesPerSession, Bounded, Ct);
                    }

                    await c0.DisposeAsync();
                    foreach (TestNode node in cluster.Nodes)
                        await node.CrashAsync();
                    if (crash == RetirementCrash.CrashAgainDuringJournalReplay)
                    {
                        FaultArm replayCrash = cluster.Faults.Arm(new FaultMatch(FaultSites.RecoveryJournalReplay, Node: leader.Name), FaultAction.CrashNode());
                        await TryAsync(() => leader.RestartAsync());
                        await cluster.WaitUntilAsync(() => Task.FromResult(replayCrash.Triggered), Bounded, "the restart crashes again during journal replay", Ct);
                        cluster.Faults.AssertReached(replayCrash);
                        replayCrash.Disarm();
                    }

                    foreach (TestNode node in cluster.Nodes.Where(n => n.State != TestNodeState.Running))
                        await node.RestartAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    break;
                }

                case RetirementCrash.LeaderCrashAfterLocalAppendOfRetiringEntry:
                case RetirementCrash.LeaderCrashAfterCommitOfRetiringEntry:
                {
                    // One of the further WriteAlls of the session is the entry that pushes K out of the window; the theory
                    // crashes the leader at each candidate (crashOccurrence = 1..W+2), one of which is the retiring entry.
                    string site = crash == RetirementCrash.LeaderCrashAfterLocalAppendOfRetiringEntry
                        ? FaultSites.RaftAfterLocalAppend
                        : FaultSites.RaftAfterCommitBeforeApply;
                    FaultArm crashArm = cluster.Faults.Arm(
                        new FaultMatch(site, Node: leader.Name, Operation: DfsOperationKind.WriteAll, Occurrence: crashOccurrence),
                        FaultAction.CrashNode());
                    await FillWindowAsync(v0, timings.MaxOutcomesPerSession + 2);
                    cluster.Faults.AssertReached(crashArm);
                    await leader.RestartAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    expiryObservedBeforeCrash = false;
                    await c0.DisposeAsync();
                    break;
                }

                case RetirementCrash.SessionExpiryCrashAllJustBeforeDeadline:
                case RetirementCrash.SessionExpiryCrashAllJustAfterDeadline:
                {
                    cluster.KillClient(c0);
                    TimeSpan deadline = timings.SessionTimeout + timings.OutcomeRetention;
                    TimeSpan offset = crash == RetirementCrash.SessionExpiryCrashAllJustBeforeDeadline ? -TimeSpan.FromMilliseconds(500) : TimeSpan.FromSeconds(2);
                    await cluster.DelayAsync(deadline + offset, Ct);
                    (DfsClient probe, DfsVolume pv) = await ConnectAsync(cluster, "probe", Ct);
                    expiryObservedBeforeCrash = await ObservedExpiredAsync(pv);
                    await probe.DisposeAsync();
                    await CrashAndRestartAllAsync(cluster, Ct);
                    await cluster.DelayAsync(deadline + TimeSpan.FromSeconds(5), Ct);   // a recovered leader may restart session clocks conservatively
                    break;
                }
            }

            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c1;
            DfsOperationOutcome status = await v1.QueryOperationAsync(k, resolve: false, Ct);
            Attempt<Fingerprint> replay = await TryAsync(() => scenario.Invoke(v1, k, Ct));

            if (expiryObservedBeforeCrash || crash is RetirementCrash.SessionExpiryCrashAllJustBeforeDeadline or RetirementCrash.SessionExpiryCrashAllJustAfterDeadline)
            {
                // Observed (or by now certainly due) retirement survives recovery.
                Assert.Equal(DfsOutcomeStatus.Expired, status.Status);
                Assert.IsAssignableFrom<DfsOperationExpiredException>(replay.Error);
            }
            else
            {
                // The retiring entry may or may not have committed: either the original outcome or explicit expiry.
                Assert.True(status.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired, $"unexpected status {status.Status}");
                if (status.Status == DfsOutcomeStatus.Committed)
                {
                    Assert.Equal(token, status.Token);
                    Assert.True(replay.Succeeded && replay.Value.Token == token, $"a retained outcome must be returned unchanged: {replay}");
                }
                else
                {
                    Assert.IsAssignableFrom<DfsOperationExpiredException>(replay.Error);
                }

                // Once Expired is observed it never reverts.
                if (status.Status == DfsOutcomeStatus.Expired)
                {
                    await CrashAndRestartAllAsync(cluster, Ct);
                    (DfsClient c2, DfsVolume v2) = await ConnectAsync(cluster, "c2", Ct);
                    await using var own1 = c2;
                    Assert.Equal(DfsOutcomeStatus.Expired, (await v2.QueryOperationAsync(k, resolve: false, Ct)).Status);
                }
            }

            // In every branch the effect exists exactly once.
            await scenario.AssertAppliedOnce(v1, Ct);
            Assert.Equal(token.Sequence, (await v1.Files.GetInfoAsync(scenario.RevisionPath!, Ct)).Revision);
        }, Ct);

    // ================================================================================================================
    // OUT-020 BarrierTimeoutPreservesLiveCommit
    // ================================================================================================================

    public enum BarrierKind
    {
        BackupAtLeastCommit,
        BackupExactSnapshot,
        LiveTargetCopies,
    }

    public enum BarrierOutage
    {
        TargetStopped,
        TargetIsolated,
    }

    public static TheoryData<BarrierKind, BarrierOutage> BarrierCases
    {
        get
        {
            var data = new TheoryData<BarrierKind, BarrierOutage>();
            foreach (BarrierKind kind in Enum.GetValues<BarrierKind>())
            {
                foreach (BarrierOutage outage in Enum.GetValues<BarrierOutage>())
                    data.Add(kind, outage);
            }

            return data;
        }
    }

    /// <summary>
    /// OUT-020: a durability barrier beyond the live commit (backup at-least boundary, exact backup snapshot, full target
    /// copy count) times out while its target is unavailable; the live commit stays committed and durable, the timeout
    /// reports the actually achieved (weaker) state, and the stronger guarantee is reported accurately once reached.
    /// </summary>
    [Theory]
    [MemberData(nameof(BarrierCases))]
    [Catalog("OUT-020", "S", "INV-01", "INV-06", "INV-08")]
    public Task OUT_020_BarrierTimeoutPreservesLiveCommit(BarrierKind kind, BarrierOutage outage) =>
        ClusterTest.RunAsync(Sim(3, null, backups: kind == BarrierKind.LiveTargetCopies ? 0 : 1), async cluster =>
        {
            (DfsClient c0, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct);
            await using var own0 = c0;
            const string path = "/b/f";
            byte[] data = TestData.Bytes(0x2020, 200 * 1024);
            await v.Directories.CreateDirectoryAsync("/b", Ct);
            TimeSpan shortBarrier = TimeSpan.FromSeconds(3);

            if (kind == BarrierKind.LiveTargetCopies)
            {
                TestNode victim = await GetFollowerAsync(cluster, Ct);
                if (outage == BarrierOutage.TargetStopped)
                    await victim.StopAsync();
                else
                    cluster.Network.Isolate(victim.Name);

                DfsOperationId id = await v.NewOperationIdAsync(Ct);
                CommitToken token = await v.Files.WriteAllBytesAsync(path, data, new DfsMutationOptions { OperationId = id }, Ct);
                FileInspection file = (await cluster.Inspect.GetFileAsync(Volume, path))!;
                Assert.NotEmpty(file.Extents);

                // The barrier "all target copies" cannot be met while a target host is out; it must time out ...
                await Assert.ThrowsAnyAsync<TimeoutException>(() => cluster.WaitForRepairAsync(Volume, shortBarrier, Ct));
                // ... while the live commit (ack two copies in two zones) stays committed, and the deficit is reported truthfully.
                DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
                Assert.True(health.UnderReplicatedObjects > 0, "the missing third copy must be reported as a deficit");
                foreach (ExtentInspection extent in file.Extents)
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(extent.Object);
                    Assert.True(copies >= 2 && zones >= 2, $"acknowledged copies/zones {copies}/{zones} below the ack policy");
                    Assert.True(copies < 3, "a copy on the unavailable host must not be counted");
                }

                DfsOperationOutcome liveOutcome = await v.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, liveOutcome.Status);
                Assert.Equal(token, liveOutcome.Token);
                await ExpectContentAsync(data, v.Files.ReadAllBytesAsync(path, Ct), "live commit readable during the barrier timeout");

                // Restore the target: the stronger guarantee is reached and reported.
                if (outage == BarrierOutage.TargetStopped)
                    await victim.RestartAsync();
                else
                    cluster.Network.Heal();
                await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
                foreach (ExtentInspection extent in file.Extents)
                    Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(extent.Object));
                Assert.Equal(token.Sequence, (await v.Files.GetInfoAsync(path, Ct)).Revision);
                return;
            }

            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: true),
                new DfsBackupBudgets(MaxPinAge: TimeSpan.FromMinutes(10), MaxPinnedBytes: 1L << 30));

            // An earlier published checkpoint so the timeout can report the last *actual* recovery point.
            await v.Files.WriteAllBytesAsync(path, TestData.Bytes(0x2019, 1000), Ct);
            await v.Snapshots.CreateAsync("before", null, Ct);
            await v.Backups.RequestSyncAsync(backup.TargetName, "before", Ct);
            DfsBackupCheckpoint previous = await v.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "before", Bounded, Ct);

            if (outage == BarrierOutage.TargetStopped)
                await backup.StopAsync();
            else
                cluster.Network.Isolate(backup.Name);

            CommitToken live;
            string? exactName = null;
            if (kind == BarrierKind.BackupExactSnapshot)
            {
                exactName = "exact";
                live = await v.Files.WriteAllBytesAsync(path, data, new DfsMutationOptions { CommitSnapshotName = exactName }, Ct);
            }
            else
            {
                live = await v.Files.WriteAllBytesAsync(path, data, Ct);
                await v.Snapshots.CreateAsync("after", null, Ct);
            }

            DfsBackupNotReachedException timeout = kind == BarrierKind.BackupExactSnapshot
                ? await Assert.ThrowsAsync<DfsBackupNotReachedException>(() => v.Backups.AwaitBackupSnapshotAsync(backup.TargetName, exactName!, shortBarrier, Ct))
                : await Assert.ThrowsAsync<DfsBackupNotReachedException>(() => v.Backups.AwaitBackupAsync(backup.TargetName, live, shortBarrier, Ct));
            Assert.Equal(DfsErrorCode.BackupNotReached, timeout.ErrorCode);
            Assert.Equal(backup.TargetName, timeout.Target);
            Assert.NotNull(timeout.LastPublished);
            Assert.Equal(previous.SnapshotName, timeout.LastPublished!.SnapshotName);
            Assert.True(timeout.LastPublished.CoveredCommit.CompareTo(live) < 0, "the reported recovery point must be the older checkpoint, not the live commit");

            DfsBackupStatus status = await v.Backups.GetStatusAsync(backup.TargetName, Ct);
            Assert.True(status.PublishedBoundary.IsNone || status.PublishedBoundary.CompareTo(live) < 0,
                "backup status must not certify the unreplicated live commit");

            // The live commit is untouched by the barrier timeout and durable across a crash of every live node.
            await ExpectContentAsync(data, v.Files.ReadAllBytesAsync(path, Ct), "live commit after barrier timeout");
            Assert.Equal(live.Sequence, (await v.Files.GetInfoAsync(path, Ct)).Revision);
            await CrashAndRestartAllAsync(cluster, Ct);
            (DfsClient c1, DfsVolume v1) = await ConnectAsync(cluster, "c1", Ct);
            await using var own1 = c1;
            await ExpectContentAsync(data, v1.Files.ReadAllBytesAsync(path, Ct), "live commit after whole-cluster crash");

            // Return the target: the stronger guarantee is reached and reported with the correct contract.
            if (outage == BarrierOutage.TargetStopped)
                await backup.RestartAsync();
            else
                cluster.Network.Heal();
            if (kind == BarrierKind.BackupExactSnapshot)
            {
                await v1.Backups.RequestSyncAsync(backup.TargetName, exactName, Ct);
                DfsBackupCheckpoint exact = await v1.Backups.AwaitBackupSnapshotAsync(backup.TargetName, exactName!, Bounded, Ct);
                Assert.True(exact.IsExactSnapshot);
                Assert.Equal(exactName, exact.SnapshotName);
                Assert.Equal(live, exact.CoveredCommit);
            }
            else
            {
                await v1.Backups.RequestSyncAsync(backup.TargetName, "after", Ct);
                DfsBackupCheckpoint atLeast = await v1.Backups.AwaitBackupAsync(backup.TargetName, live, Bounded, Ct);
                Assert.True(atLeast.CoveredCommit.IsAtOrAfter(live));
                Assert.Equal(backup.TargetName, atLeast.Target);
            }
        }, Ct);
}
