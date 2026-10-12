using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Simulation.Sdk;

/// <summary>
/// SDK suite, level S (test plan §7.2, FSD §§5, 13): discovery with failed seeds, cluster identity, rerouting while
/// leadership moves, and client disposal with requests in flight — in deterministic whole-cluster simulation.
/// </summary>
public sealed class SdkSimulationTests
{
    private const string Volume = SdkHelpers.Volume;

    private static CancellationToken Ct => SdkHelpers.Ct;

    // =====================================================================================================
    // SDK-004 SeedEndpointOffline
    // =====================================================================================================

    public static TheoryData<string> FirstSeedFailures => new("Cut", "BlackHole", "Crashed", "Paused", "Stopped");

    [Theory, MemberData(nameof(FirstSeedFailures))]
    [Catalog("SDK-004", "S", "INV-17", "INV-12")]
    public async Task SDK_004_SeedEndpointOffline(string failure)
    {
        await ClusterTest.RunAsync(SdkHelpers.Deterministic, async cluster =>
        {
            const string ClientName = "c4";
            string me = SdkHelpers.NetworkName(ClientName);
            TestNode volumeLeader = await cluster.GetLeaderAsync(Volume);
            TestNode controlLeader = await cluster.GetControlLeaderAsync();
            // The unreachable first seed is a node whose loss keeps both quorums and both current leaders.
            TestNode first = cluster.Nodes.FirstOrDefault(n => n.Name != volumeLeader.Name && n.Name != controlLeader.Name)
                ?? cluster.Nodes.First(n => n.Name != volumeLeader.Name);
            string[] seeds = [first.Name, .. cluster.Nodes.Where(n => n.Name != first.Name).Select(n => n.Name)];

            switch (failure)
            {
                case "Cut":
                    cluster.Network.CutBoth(me, first.Name);
                    break;
                case "BlackHole":
                    cluster.Network.BlackHole(me, first.Name);
                    cluster.Network.BlackHole(first.Name, me);
                    break;
                case "Crashed":
                    await first.CrashAsync();
                    break;
                case "Paused":
                    await first.PauseAsync();
                    break;
                case "Stopped":
                    await first.StopAsync();
                    break;
            }

            TimeSpan connectTimeout = TimeSpan.FromSeconds(8);
            long started = cluster.Time.GetTimestamp();
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = ClientName, SeedNodes = seeds, ConnectTimeout = connectTimeout });
            TimeSpan elapsed = cluster.Time.GetElapsedTime(started);
            SdkHelpers.Log($"SDK-004 {failure}: connected after {elapsed} with first seed {first.Name} unavailable");
            Assert.True(elapsed <= connectTimeout, $"discovery took {elapsed}, beyond the {connectTimeout} connect deadline");

            // The client reached the intended cluster through another trusted seed and works.
            Assert.Equal(cluster.ClusterId, client.Cluster.ClusterId);
            byte[] bytes = TestData.Bytes(4004, 300);
            CommitToken token = await client.GetVolume(Volume).Files.WriteAllBytesAsync("/sdk004.bin", bytes);
            Assert.Equal(cluster.ClusterId, token.ClusterId);
            byte[] readBack = await SdkHelpers.ReadAsync(client.GetVolume(Volume), "/sdk004.bin");
            AssertEx.BytesEqual(bytes, readBack);
            FileInspection? file = await cluster.Inspect.GetFileAsync(Volume, "/sdk004.bin");
            Assert.NotNull(file);
            Assert.Equal(bytes.Length, file.Length);

            // The first seed did not serve the client. (Requests *to* it may legitimately have been attempted and logged
            // for a cut or black-holed link — seeds are tried in order — so only its replies are checked there.)
            if (failure is "Crashed" or "Stopped")
                Assert.Equal(0, cluster.Network.Log.CountMessages(new MessageFilter(From: me, To: first.Name)));
            if (failure is "Cut" or "Crashed" or "Stopped")
                Assert.Equal(0, cluster.Network.Log.CountMessages(new MessageFilter(From: first.Name, To: me, Direction: LinkDirection.Reply)));
            long viaOthers = seeds.Skip(1).Sum(n => cluster.Network.Log.CountMessages(new MessageFilter(From: me, To: n)));
            Assert.True(viaOthers > 0, "the client must have been served by another seed");

            // Recovery: the first seed returns and the replicas converge.
            switch (failure)
            {
                case "Cut":
                case "BlackHole":
                    cluster.Network.Heal();
                    break;
                case "Crashed":
                case "Stopped":
                    await first.RestartAsync();
                    break;
                case "Paused":
                    await first.ResumeAsync();
                    break;
            }

            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30));
            await cluster.WaitForReplicationAsync(Volume);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
        }, Ct);
    }

    // =====================================================================================================
    // SDK-005 AllSeedsOffline
    // =====================================================================================================

    public static TheoryData<string> AllSeedFailures => new("Cut", "BlackHole", "Crashed", "Paused");

    [Theory, MemberData(nameof(AllSeedFailures))]
    [Catalog("SDK-005", "S", "INV-17", "INV-08")]
    public async Task SDK_005_AllSeedsOffline(string failure)
    {
        await ClusterTest.RunAsync(SdkHelpers.Deterministic, async cluster =>
        {
            const string ClientName = "c5";
            string me = SdkHelpers.NetworkName(ClientName);
            TimeSpan connectTimeout = TimeSpan.FromSeconds(3);
            foreach (TestNode node in cluster.Nodes)
            {
                switch (failure)
                {
                    case "Cut":
                        cluster.Network.CutBoth(me, node.Name);
                        break;
                    case "BlackHole":
                        cluster.Network.BlackHole(me, node.Name);
                        cluster.Network.BlackHole(node.Name, me);
                        break;
                    case "Crashed":
                        await node.CrashAsync();
                        break;
                    case "Paused":
                        await node.PauseAsync();
                        break;
                }
            }

            // Every node is named as a seed explicitly (the harness default must not drop crashed/paused nodes and hand
            // the SDK an empty seed list, which would be an argument error instead of an availability failure).
            string[] allSeeds = cluster.Nodes.Select(n => n.Name).ToArray();
            long started = cluster.Time.GetTimestamp();
            Exception error = await SdkHelpers.CaptureAsync(
                () => cluster.ConnectAsync(new TestClientOptions { Name = ClientName, SeedNodes = allSeeds, ConnectTimeout = connectTimeout }), "every seed is offline");
            TimeSpan elapsed = cluster.Time.GetElapsedTime(started);
            Exception inner = SdkHelpers.Unwrap(error);
            SdkHelpers.Log($"SDK-005 {failure}: {inner.GetType().Name} after {elapsed}");
            Assert.False(inner is OperationCanceledException, "the caller did not cancel; the deadline must surface as a typed failure");
            Assert.True(SdkHelpers.IsTypedAvailabilityFailure(inner), $"expected a typed availability/timeout failure but got {inner.GetType().Name} ({DfsErrors.ToCode(inner)})");
            Assert.True(elapsed <= connectTimeout + TimeSpan.FromSeconds(1), $"connect failed after {elapsed}, beyond its {connectTimeout} deadline");

            // No leaked tasks or sockets: once every seed is reachable again, the abandoned attempt stays silent.
            switch (failure)
            {
                case "Cut":
                case "BlackHole":
                    cluster.Network.Heal();
                    break;
                case "Crashed":
                    foreach (TestNode node in cluster.Nodes)
                        await node.RestartAsync();
                    break;
                case "Paused":
                    foreach (TestNode node in cluster.Nodes)
                        await node.ResumeAsync();
                    break;
            }

            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30));
            await cluster.DelayAsync(TimeSpan.FromSeconds(1));
            cluster.Network.Log.Reset();
            await cluster.DelayAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(0, cluster.Network.Log.CountMessages(new MessageFilter(From: me)));
            Assert.Equal(0, cluster.Network.CountOpenConnections(me));

            // The cluster itself is unaffected.
            await using DfsClient ok = await cluster.ConnectAsync(new TestClientOptions { Name = "c6" });
            Assert.Equal(cluster.ClusterId, ok.Cluster.ClusterId);
        }, Ct);
    }

    // =====================================================================================================
    // SDK-006 ClusterIdentityMismatch
    // =====================================================================================================

    [Fact]
    [Catalog("SDK-006", "S", "INV-12", "INV-08")]
    public async Task SDK_006_ClusterIdentityMismatch()
    {
        await ClusterTest.RunAsync(SdkHelpers.Deterministic, async cluster =>
        {
            var foreign = new Guid("3b0c7c52-9a1e-4f43-8d2e-5f6a7b8c9d0e");
            Assert.NotEqual(foreign, cluster.ClusterId);
            const string ClientName = "c6";
            string me = SdkHelpers.NetworkName(ClientName);
            int sessionsBefore = (await cluster.Inspect.GetVolumeStateAsync(Volume)).Sessions.Count;

            Exception error = await SdkHelpers.CaptureAsync(
                () => cluster.ConnectAsync(new TestClientOptions { Name = ClientName, ExpectedClusterId = foreign }), "the seeds belong to another cluster");
            DfsClusterIdentityException mismatch = Assert.IsAssignableFrom<DfsClusterIdentityException>(SdkHelpers.Unwrap(error));
            Assert.Equal(DfsErrorCode.ClusterMismatch, mismatch.ErrorCode);
            Assert.False(mismatch.IsRetryable, "retrying cannot turn another cluster into the expected one");

            // The client did not bind to the reachable cluster's namespace.
            await cluster.DelayAsync(TimeSpan.FromSeconds(1));
            Assert.Equal(0, SdkHelpers.ForegroundRequests(cluster, me));
            Assert.Equal(sessionsBefore, (await cluster.Inspect.GetVolumeStateAsync(Volume)).Sessions.Count);
            Assert.Equal(0, cluster.Network.CountOpenConnections(me));

            // Control: the correct expected identity connects.
            await using DfsClient ok = await cluster.ConnectAsync(new TestClientOptions { Name = "c7", ExpectedClusterId = cluster.ClusterId });
            Assert.Equal(cluster.ClusterId, ok.Cluster.ClusterId);
        }, Ct);
    }

    public static TheoryData<string> ForeignSeedLayouts => new("ForeignSeedFirstThenOwn", "OnlyForeignSeed");

    [Theory, MemberData(nameof(ForeignSeedLayouts))]
    [Catalog("SDK-006", "S", "INV-12", "INV-08")]
    public async Task SDK_006_ClusterIdentityMismatch_ForeignClusterEndpoint(string layout)
    {
        // Two real clusters on QUIC so an endpoint of the other cluster is genuinely reachable from the client. Explicit,
        // different seeds: with Seed = 0 both would derive the same seed from the test name and get identical cluster ids.
        TestClusterOptions quic = TestClusterOptions.ThreeZones with { Transport = TransportKind.Quic };
        await ClusterTest.RunAsync(quic with { Seed = 0x5D_0006_0A }, async clusterA =>
        {
            await ClusterTest.RunAsync(quic with { Seed = 0x5D_0006_0B }, async clusterB =>
            {
                Assert.NotEqual(clusterA.ClusterId, clusterB.ClusterId);
                DfsClientOptions own = clusterA.CreateClientOptions();
                string foreignSeed = clusterB[0].Endpoint;
                DfsClientOptions options = own with
                {
                    SeedEndpoints = layout == "ForeignSeedFirstThenOwn" ? new[] { foreignSeed }.Concat(own.SeedEndpoints).ToArray() : new[] { foreignSeed },
                    ExpectedClusterId = clusterA.ClusterId,
                    // TLS succeeds against both clusters, so only the cluster-identity check can tell them apart.
                    Security = new DfsTransportSecurity { AllowUntrustedForDevelopment = true },
                    ConnectTimeout = TimeSpan.FromSeconds(10),
                };
                int foreignSessionsBefore = (await clusterB.Inspect.GetVolumeStateAsync(Volume)).Sessions.Count;
                clusterB.Network.Log.Reset();

                if (layout == "ForeignSeedFirstThenOwn")
                {
                    await using DfsClient client = await DfsClient.ConnectAsync(options, Ct);
                    Assert.Equal(clusterA.ClusterId, client.Cluster.ClusterId);
                    byte[] bytes = TestData.Bytes(6006, 200);
                    CommitToken token = await client.GetVolume(Volume).Files.WriteAllBytesAsync("/sdk006.bin", bytes, Ct);
                    Assert.Equal(clusterA.ClusterId, token.ClusterId);
                    Assert.NotNull(await clusterA.Inspect.GetFileAsync(Volume, "/sdk006.bin"));
                    Assert.Null(await clusterB.Inspect.GetFileAsync(Volume, "/sdk006.bin"));
                }
                else
                {
                    Exception error = await SdkHelpers.CaptureAsync(() => DfsClient.ConnectAsync(options, Ct), "only a foreign cluster is reachable");
                    DfsClusterIdentityException mismatch = Assert.IsAssignableFrom<DfsClusterIdentityException>(SdkHelpers.Unwrap(error));
                    Assert.Equal(DfsErrorCode.ClusterMismatch, mismatch.ErrorCode);
                }

                // The foreign cluster's namespace was never bound: no session, no namespace or data request.
                Assert.Equal(foreignSessionsBefore, (await clusterB.Inspect.GetVolumeStateAsync(Volume)).Sessions.Count);
                foreach (DfsOperationKind kind in new[] { DfsOperationKind.OpenSession, DfsOperationKind.WriteAll, DfsOperationKind.Open, DfsOperationKind.PutObject })
                    Assert.Equal(0, clusterB.Network.Log.CountMessages(new MessageFilter(Direction: LinkDirection.Request, Operation: kind)));
            }, Ct);
        }, Ct);
    }

    // =====================================================================================================
    // SDK-007 RoutingChangesDuringCall
    // =====================================================================================================

    private static readonly string[] LeadershipMoveNames =
        ["StaleRouteAfterHeal", "LeaderIsolatedDuringCall", "LeaderCrashBeforePropose", "LeaderCrashAfterLocalAppend", "LeaderCrashAfterCommitBeforeReply"];

    /// <summary>
    /// Every leadership move with an application-supplied operation id (outcome queryable and retryable by id) and with
    /// the SDK's own id (the SDK must reuse it across the reroute; a fresh id would append the suffix twice).
    /// </summary>
    public static TheoryData<string, bool> LeadershipMoves
    {
        get
        {
            var data = new TheoryData<string, bool>();
            foreach (string move in LeadershipMoveNames)
            {
                data.Add(move, true);
                data.Add(move, false);
            }

            return data;
        }
    }

    [Theory, MemberData(nameof(LeadershipMoves))]
    [Catalog("SDK-007", "S", "INV-06", "INV-17")]
    public async Task SDK_007_RoutingChangesDuringCall(string move, bool explicitOperationId)
    {
        await ClusterTest.RunAsync(SdkHelpers.Deterministic, async cluster =>
        {
            const string LogPath = "/sdk007.log";
            TimeSpan deadline = TimeSpan.FromSeconds(15);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = deadline });
            DfsVolume volume = client.GetVolume(Volume);
            byte[] prefix = TestData.Bytes(7001, 100);
            byte[] suffix = TestData.Bytes(7002, 37);
            CommitToken first = await volume.Files.WriteAllBytesAsync(LogPath, prefix); // discovery and routing to the current leader
            TestNode oldLeader = await cluster.GetLeaderAsync(Volume);
            ulong termBefore = (await cluster.Inspect.GetVolumeGroupAsync(Volume)).Term;
            DfsOperationId? opId = explicitOperationId ? await volume.NewOperationIdAsync() : null;
            var mutation = new DfsMutationOptions { OperationId = opId };

            FaultArm? arm = null;
            switch (move)
            {
                case "StaleRouteAfterHeal":
                    cluster.Network.Isolate(oldLeader.Name);
                    await WaitForLeaderOtherThanAsync(cluster, oldLeader);
                    cluster.Network.Heal();
                    await cluster.WaitUntilAsync(async () => (await oldLeader.Inspect.GetGroupAsync(Volume))?.Role != RaftRole.Leader,
                        TimeSpan.FromSeconds(10), "the old leader steps down after healing");
                    break;
                case "LeaderIsolatedDuringCall":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcRequestAccepted, Node: oldLeader.Name, Operation: DfsOperationKind.Append),
                        FaultAction.Custom(_ =>
                        {
                            cluster.Network.Isolate(oldLeader.Name);
                            return Task.CompletedTask;
                        }));
                    break;
                case "LeaderCrashBeforePropose":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftBeforePropose, Node: oldLeader.Name, Volume: Volume, Operation: DfsOperationKind.Append), FaultAction.CrashNode());
                    break;
                case "LeaderCrashAfterLocalAppend":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: oldLeader.Name, Volume: Volume, Operation: DfsOperationKind.Append), FaultAction.CrashNode());
                    break;
                case "LeaderCrashAfterCommitBeforeReply":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ApplyAfterBeforeReply, Node: oldLeader.Name, Volume: Volume, Operation: DfsOperationKind.Append), FaultAction.CrashNode());
                    break;
            }

            long started = cluster.Time.GetTimestamp();
            CommitToken token = await volume.Files.AppendAllBytesAsync(LogPath, suffix, mutation);
            TimeSpan elapsed = cluster.Time.GetElapsedTime(started);
            if (arm is not null)
                cluster.Faults.AssertReached(arm);
            SdkHelpers.Log($"SDK-007 {move}: append completed after {elapsed}");
            Assert.True(elapsed <= deadline, $"rerouting took {elapsed}, beyond the {deadline} operation deadline");

            // Leadership really moved, and the operation kept its identity and its single outcome.
            Assert.True((await cluster.Inspect.GetVolumeGroupAsync(Volume)).Term > termBefore, "a new term must have started");
            Assert.Equal(cluster.ClusterId, token.ClusterId);
            Assert.True(token.IsComparableWith(first) && token.IsAtOrAfter(first));
            byte[] content = await SdkHelpers.ReadAsync(volume, LogPath);
            AssertEx.BytesEqual([.. prefix, .. suffix], content, "the suffix is appended exactly once (the reroute reused the operation id)");
            Assert.Equal(prefix.Length + suffix.Length, (await cluster.Inspect.GetFileAsync(Volume, LogPath))!.Length);

            if (opId is { } id)
            {
                DfsOperationOutcome outcome = await volume.QueryOperationAsync(id);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.Equal(token, outcome.Token);

                // A retry under the same id reports the original outcome without a second effect.
                CommitToken retried = await volume.Files.AppendAllBytesAsync(LogPath, suffix, mutation);
                Assert.Equal(token, retried);
                content = await SdkHelpers.ReadAsync(volume, LogPath);
                AssertEx.BytesEqual([.. prefix, .. suffix], content, "a retry must not append again");
            }

            // Safety after recovery: the old leader returns, there is one leader per term and replicas converge.
            cluster.Network.Heal();
            if (oldLeader.State == TestNodeState.Crashed)
                await oldLeader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30));
            await cluster.WaitForReplicationAsync(Volume);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" });
            content = await SdkHelpers.ReadAsync(fresh.GetVolume(Volume), LogPath);
            AssertEx.BytesEqual([.. prefix, .. suffix], content);
        }, Ct);
    }

    private static Task WaitForLeaderOtherThanAsync(TestCluster cluster, TestNode old) =>
        cluster.WaitUntilAsync(async () =>
        {
            foreach (TestNode node in cluster.Nodes.Where(n => n.Name != old.Name && n.State == TestNodeState.Running))
            {
                if ((await node.Inspect.GetGroupAsync(Volume))?.Role == RaftRole.Leader)
                    return true;
            }

            return false;
        }, TimeSpan.FromSeconds(10), "a new volume leader is elected among the reachable nodes");

    // =====================================================================================================
    // SDK-012 DisposalDuringRequests
    // =====================================================================================================

    public static TheoryData<string> ShutdownTimings => new("HeldBeyondShutdownDeadline", "ReleasedWithinShutdownDeadline");

    [Theory, MemberData(nameof(ShutdownTimings))]
    [Catalog("SDK-012", "S", "INV-06", "INV-17")]
    public async Task SDK_012_DisposalDuringRequests(string timing)
    {
        TimeSpan shutdown = TimeSpan.FromSeconds(2);
        await ClusterTest.RunAsync(SdkHelpers.Deterministic, async cluster =>
        {
            await using DfsClient observerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" });
            DfsVolume observer = observerClient.GetVolume(Volume);
            const string ClientName = "c0";
            string me = SdkHelpers.NetworkName(ClientName);
            DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = ClientName }, o => o with { ShutdownTimeout = shutdown });
            DfsVolume volume = client.GetVolume(Volume);

            byte[] readData = TestData.Bytes(1201, 2000);
            byte[] prefix = TestData.Bytes(1202, 100);
            byte[] suffix = TestData.Bytes(1203, 40);
            byte[] oldWrite = TestData.Bytes(1204, 300);
            byte[] newWrite = TestData.Bytes(1205, 300);
            byte[] txOriginal = TestData.Bytes(1206, 50);
            byte[] streamOriginal = TestData.Bytes(1207, 50);
            await volume.Files.WriteAllBytesAsync("/read.bin", readData);
            await volume.Files.WriteAllBytesAsync("/append.bin", prefix);
            await volume.Files.WriteAllBytesAsync("/write.bin", oldWrite);
            await volume.Files.WriteAllBytesAsync("/tx.bin", txOriginal);
            await volume.Files.WriteAllBytesAsync("/stream.bin", streamOriginal);

            // State the application never committed: an open transaction and a writable stream with unflushed bytes.
            DfsTransaction tx = await volume.Transactions.BeginAsync();
            await tx.Files.WriteAllBytesAsync("/tx.bin", TestData.Bytes(1208, 64));
            DfsStream stream = await volume.Files.OpenAsync("/stream.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            await stream.WriteAsync(TestData.Bytes(1209, 64));
            DfsStream reader = await volume.Files.OpenReadAsync("/read.bin");
            DfsOperationId appendId = await volume.NewOperationIdAsync();
            DfsOperationId writeId = await volume.NewOperationIdAsync();

            // In flight at disposal: a live read and an append (requests held) and a WriteAll (executed, reply held).
            MessageHold readHold = cluster.Network.Hold(new MessageFilter(From: me, Direction: LinkDirection.Request, Operation: DfsOperationKind.Read));
            MessageHold appendHold = cluster.Network.Hold(new MessageFilter(From: me, Direction: LinkDirection.Request, Operation: DfsOperationKind.Append));
            MessageHold writeReplyHold = cluster.Network.Hold(new MessageFilter(To: me, Direction: LinkDirection.Reply, Operation: DfsOperationKind.WriteAll));
            byte[] buffer = new byte[readData.Length];
            Task<int> readTask = reader.ReadAsync(buffer).AsTask();
            Task<CommitToken> appendTask = volume.Files.AppendAllBytesAsync("/append.bin", suffix, new DfsMutationOptions { OperationId = appendId });
            Task<CommitToken> writeTask = volume.Files.WriteAllBytesAsync("/write.bin", newWrite, new DfsMutationOptions { OperationId = writeId });
            await readHold.WaitForAsync(1, TimeSpan.FromSeconds(5));
            await appendHold.WaitForAsync(1, TimeSpan.FromSeconds(5));
            await writeReplyHold.WaitForAsync(1, TimeSpan.FromSeconds(5));
            // Observer reads below use a Share.ReadWrite|Delete stream: the disposed client's writer handle on
            // /stream.bin and its transaction reservation on /tx.bin may still be active until their leases/sessions
            // expire, and a ReadAll's own handle could legally conflict with them (test plan §5).
            byte[] writeVisible = await SdkHelpers.ReadSharedAsync(observer, "/write.bin");
            AssertEx.BytesEqual(newWrite, writeVisible, "the WriteAll committed; only its reply is held");
            Assert.False(readTask.IsCompleted || appendTask.IsCompleted || writeTask.IsCompleted);

            long started = cluster.Time.GetTimestamp();
            Task disposal = client.DisposeAsync().AsTask();
            if (timing == "ReleasedWithinShutdownDeadline")
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(300));
                readHold.ReleaseAll();
                appendHold.ReleaseAll();
                writeReplyHold.ReleaseAll();
            }

            await Task.WhenAny(disposal, cluster.DelayAsync(shutdown + TimeSpan.FromSeconds(2)));
            Assert.True(disposal.IsCompleted, $"disposal did not finish within the {shutdown} shutdown deadline");
            await disposal;
            TimeSpan elapsed = cluster.Time.GetElapsedTime(started);
            Assert.True(elapsed <= shutdown + TimeSpan.FromSeconds(1), $"disposal took {elapsed}");
            Assert.True(client.IsDisposed);
            Assert.True(readTask.IsCompleted && appendTask.IsCompleted && writeTask.IsCompleted, "every in-flight operation is resolved by disposal");

            Exception? readError = await Record.ExceptionAsync(() => readTask);
            Exception? appendError = await Record.ExceptionAsync(() => appendTask);
            Exception? writeError = await Record.ExceptionAsync(() => writeTask);
            readHold.Dispose();
            appendHold.Dispose();
            writeReplyHold.Dispose();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2)); // delayed copies of held requests may arrive now

            // Read: real data or a cancellation/disposal error, never fabricated bytes.
            if (readError is null)
            {
                int n = await readTask;
                Assert.InRange(n, 1, readData.Length);
                AssertEx.BytesEqual(readData.AsSpan(0, n), buffer.AsSpan(0, n));
            }
            else
            {
                Exception e = SdkHelpers.Unwrap(readError);
                Assert.True(e is OperationCanceledException or ObjectDisposedException, $"read failed with {e.GetType().Name}");
            }

            // WriteAll committed: it is reported as success or as uncertain with its id — never as a definite failure.
            if (writeError is not null)
            {
                DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(SdkHelpers.Unwrap(writeError));
                Assert.Equal(writeId, unknown.OperationId);
            }

            DfsOperationOutcome writeOutcome = await observer.QueryOperationAsync(writeId, resolve: true);
            Assert.Equal(DfsOutcomeStatus.Committed, writeOutcome.Status);
            byte[] writeNow = await SdkHelpers.ReadSharedAsync(observer, "/write.bin");
            AssertEx.BytesEqual(newWrite, writeNow);

            // Append: success, uncertainty (with its id), or a definite cancellation that must then be a real abort.
            DfsOperationOutcome appendOutcome = await observer.QueryOperationAsync(appendId, resolve: true);
            Assert.Contains(appendOutcome.Status, new[] { DfsOutcomeStatus.Committed, DfsOutcomeStatus.Aborted });
            if (appendError is null)
            {
                Assert.Equal(DfsOutcomeStatus.Committed, appendOutcome.Status);
            }
            else
            {
                Exception e = SdkHelpers.Unwrap(appendError);
                if (e is DfsCommitOutcomeUnknownException unknown)
                    Assert.Equal(appendId, unknown.OperationId);
                else if (e is OperationCanceledException or ObjectDisposedException)
                    Assert.Equal(DfsOutcomeStatus.Aborted, appendOutcome.Status);
                else
                    Assert.Fail($"append failed with unexpected {e.GetType().Name}: {e.Message}");
            }

            byte[] appended = await SdkHelpers.ReadSharedAsync(observer, "/append.bin");
            byte[] expectedAppend = appendOutcome.Status == DfsOutcomeStatus.Committed ? [.. prefix, .. suffix] : prefix;
            AssertEx.BytesEqual(expectedAppend, appended, "append effect matches its resolved outcome");

            // Disposal never commits transactions and never flushes streams.
            Assert.NotEqual(DfsTransactionState.Committed, tx.State);
            DfsOperationOutcome txOutcome = await observer.Transactions.QueryOutcomeAsync(tx.Id, resolve: true);
            Assert.NotEqual(DfsOutcomeStatus.Committed, txOutcome.Status);
            byte[] txNow = await SdkHelpers.ReadSharedAsync(observer, "/tx.bin");
            AssertEx.BytesEqual(txOriginal, txNow, "an uncommitted transaction is not committed by client disposal");
            byte[] streamNow = await SdkHelpers.ReadSharedAsync(observer, "/stream.bin");
            AssertEx.BytesEqual(streamOriginal, streamNow, "unflushed stream bytes are not published by client disposal");

            // Even after the abandoned handle/transaction expire, nothing is published later.
            await cluster.DelayAsync(TimeSpan.FromSeconds(12)); // > session timeout (Fast: 10 s) and lease (2 s)
            txNow = await SdkHelpers.ReadSharedAsync(observer, "/tx.bin");
            AssertEx.BytesEqual(txOriginal, txNow, "expiry of the abandoned transaction does not commit it");
            streamNow = await SdkHelpers.ReadSharedAsync(observer, "/stream.bin");
            AssertEx.BytesEqual(streamOriginal, streamNow, "expiry of the abandoned handle does not publish it");

            // Nothing remains: no connections, new work rejected (GetVolume does no I/O, so it may instead return a
            // facade whose operations throw ObjectDisposedException).
            Assert.Equal(0, cluster.Network.CountOpenConnections(me));
            DfsVolume? late = null;
            Exception? getVolume = Record.Exception(() => late = client.GetVolume(Volume));
            if (getVolume is not null)
                Assert.IsAssignableFrom<ObjectDisposedException>(getVolume);
            else
                await Assert.ThrowsAsync<ObjectDisposedException>(() => late!.Files.ExistsAsync("/read.bin"));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => volume.Files.ReadAllBytesAsync("/read.bin"));
        }, Ct);
    }
}
