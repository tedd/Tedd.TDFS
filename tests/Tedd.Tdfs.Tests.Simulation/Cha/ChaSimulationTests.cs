using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Cha.ChaHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Cha;

/// <summary>
/// CHA — compound-failure regression scenarios (test plan §7.30, FSD §§7–16, 18). Every scenario runs in the deterministic
/// simulation, combines at least two concurrent failures/transitions, asserts that each injected fault point was reached,
/// and checks its safety properties both during the failure/recovery window and after convergence.
/// </summary>
public sealed class ChaSimulationTests
{
    // ------------------------------------------------------------------------------------------------------------------
    // CHA-001
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>The mutation whose commit reply is lost while the leader crashes.</summary>
    public enum LostReplyMutation
    {
        /// <summary>Atomic append (must never be applied twice).</summary>
        Append,

        /// <summary>Atomic whole-file replacement (must preserve inode identity).</summary>
        WriteAll,

        /// <summary>Atomic rename (a re-execution would fail or move a different inode).</summary>
        Move,
    }

    /// <summary>When the new client retries K relative to metadata compaction and outcome retention (§9.2 repeats).</summary>
    public enum RetryWindow
    {
        /// <summary>Right after the failover.</summary>
        Immediate,

        /// <summary>After the metadata log was compacted past K's entry, still within the retained outcome window.</summary>
        AfterCompactionWithinWindow,

        /// <summary>After compaction and after K's outcome retention expired: explicit expired-id rejection, never a replay.</summary>
        BeyondRetentionWindow,
    }

    /// <summary>
    /// Every mutation × every crash point after the quorum commit (F11 before apply, F12 before reply, F13 reply send), retried
    /// immediately; plus every mutation retried after compaction within / beyond the outcome window (crash at F12).
    /// </summary>
    public static TheoryData<LostReplyMutation, string, RetryWindow> Cha001Cases()
    {
        var data = new TheoryData<LostReplyMutation, string, RetryWindow>();
        foreach (LostReplyMutation mutation in Enum.GetValues<LostReplyMutation>())
        {
            foreach (string site in new[] { FaultSites.RaftAfterCommitBeforeApply, FaultSites.ApplyAfterBeforeReply, FaultSites.RpcReplySend })
                data.Add(mutation, site, RetryWindow.Immediate);
            data.Add(mutation, FaultSites.ApplyAfterBeforeReply, RetryWindow.AfterCompactionWithinWindow);
            data.Add(mutation, FaultSites.ApplyAfterBeforeReply, RetryWindow.BeyondRetentionWindow);
        }

        return data;
    }

    /// <summary>
    /// §9.2: the caller persists operation id K, submits the mutation, the leader crashes after the quorum commit and every
    /// reply to the caller is lost, the caller's process dies; a new client queries and retries K through the new leader.
    /// Exactly one complete durable mutation results, the retry reports the original commit, and incompatible reuse of K is
    /// rejected. Repeated after metadata compaction within the retained outcome window (same result) and beyond it, where K
    /// must be rejected as expired (query reports <see cref="DfsOutcomeStatus.Expired"/>) and never re-executed.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha001Cases))]
    [Catalog("CHA-001", "S", "INV-01", "INV-02", "INV-06")]
    public async Task CHA_001_LeaderFailoverPlusLostCommitReply(LostReplyMutation mutation, string crashSite, RetryWindow window)
    {
        CancellationToken ct = Ct;

        // "Within the window" must not depend on how long the compaction takes: in that variant the dead caller's session
        // timeout and the outcome retention are long, so K's outcome is certainly still retained when it is queried/retried.
        TestClusterOptions options = Sim(TestClusterOptions.ThreeZones);
        if (window == RetryWindow.AfterCompactionWithinWindow)
        {
            options = options with
            {
                Timings = NodeTimings.Fast with { SessionTimeout = TimeSpan.FromSeconds(120), OutcomeRetention = TimeSpan.FromSeconds(120) },
            };
        }

        await ClusterTest.RunAsync(options, async cluster =>
        {
            const string dir = "/cha001";
            const string target = dir + "/target";
            const string moved = dir + "/moved";
            const string elsewhere = dir + "/elsewhere";
            byte[] prefix = TestData.Bytes(1001, MultiExtentLength);     // P
            byte[] payload = TestData.Bytes(1002, 9_000);                // S
            byte[] changed = TestData.Bytes(1003, 9_000);                // S' (incompatible reuse of K)

            var c0Options = new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(4) };
            DfsClient c0 = await cluster.ConnectAsync(c0Options, ct);   // abandoned via KillClient, never disposed
            DfsVolume v0 = c0.GetVolume(Vol);
            await v0.Directories.CreateDirectoryAsync(dir, ct);

            HistoryRecorder history = cluster.History;
            string kindName = mutation == LostReplyMutation.Append ? "append" : "write";
            if (mutation == LostReplyMutation.Append)
            {
                await v0.Files.WriteAllBytesAsync(target, ReadOnlyMemory<byte>.Empty, ct);
                await history.RecordAsync("c0", "append", target, prefix, () => v0.Files.AppendAllBytesAsync(target, prefix, ct));
            }
            else if (mutation == LostReplyMutation.WriteAll)
            {
                await history.RecordAsync("c0", "write", target, prefix, () => v0.Files.WriteAllBytesAsync(target, prefix, ct));
            }
            else
            {
                await v0.Files.WriteAllBytesAsync(target, prefix, ct);
            }

            InodeId inode = (await v0.Files.GetInfoAsync(target, ct)).InodeId;
            DfsOperationId k = await v0.NewOperationIdAsync(ct);   // persisted by the application before transmission
            TestNode oldLeader = await cluster.GetLeaderAsync(Vol, ct);
            (DfsOperationKind kind, MessageType request) = mutation switch
            {
                LostReplyMutation.Append => (DfsOperationKind.Append, MessageType.Append),
                LostReplyMutation.WriteAll => (DfsOperationKind.WriteAll, MessageType.WriteAll),
                _ => (DfsOperationKind.Move, MessageType.Move),
            };

            FaultArm crash = cluster.Faults.Arm(new FaultMatch(crashSite, Node: oldLeader.Name, Volume: Vol, Operation: kind), FaultAction.CrashNode());
            MessageRule lostReplies = cluster.Network.DropReplies(new MessageFilter(From: c0Options.NetworkName, Type: request), count: 10_000);
            MessageRule lostQueries = cluster.Network.DropReplies(new MessageFilter(From: c0Options.NetworkName, Type: MessageType.QueryOperation), count: 10_000);

            long historyId = mutation == LostReplyMutation.Move ? 0 : history.Invoke("c0", kindName, target, payload, k);
            Task<CommitToken> first = ApplyLostReplyMutationAsync(v0, mutation, target, moved, elsewhere, payload, changed, k, changedArguments: false);
            await crash.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(crash);

            Task finished = await Task.WhenAny(first, cluster.DelayAsync(TimeSpan.FromSeconds(60), ct));
            Assert.True(ReferenceEquals(finished, first), "the caller's call must end (success or unknown outcome) within its deadline");
            CommitToken firstToken = CommitToken.None;
            Exception? firstError = null;
            try
            {
                firstToken = await first;
            }
            catch (Exception ex)
            {
                firstError = ex;
            }

            if (firstError is null)
            {
                if (historyId != 0)
                    history.Complete(historyId, OpOutcome.Ok, firstToken);
            }
            else
            {
                // The request reached the commit; a definite failure would misreport uncertainty as rollback (FSD §13).
                Assert.True(firstError is DfsCommitOutcomeUnknownException, $"after a possible commit the caller must see an unknown outcome, got {firstError}");
                Assert.Equal(k, ((DfsCommitOutcomeUnknownException)firstError).OperationId);
                if (historyId != 0)
                    history.Complete(historyId, OpOutcome.Unknown, firstError);
            }

            cluster.KillClient(c0);   // the caller's process dies; only K survives in the application's own storage
            lostReplies.Cancel();
            lostQueries.Cancel();

            TestNode newLeader = await cluster.GetLeaderAsync(Vol, ct);
            Assert.NotEqual(oldLeader.Name, newLeader.Name);

            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            DfsVolume v1 = c1.GetVolume(Vol);
            if (window != RetryWindow.Immediate)
            {
                // Compact the metadata log past K's entry (through another client/session, so K's own session keeps its window).
                ulong appliedBefore = (await newLeader.Inspect.GetGroupAsync(Vol))!.AppliedIndex;
                await FillLogAsync(v1, $"{dir}/fill", cluster.Options.Timings.RaftSnapshotThreshold + 40);
                await cluster.WaitUntilAsync(async () => (await (await cluster.GetLeaderAsync(Vol, ct)).Inspect.GetGroupAsync(Vol))!.SnapshotIndex > appliedBefore,
                    TimeSpan.FromSeconds(30), "the metadata log was compacted past K's committed entry", ct);
                if (window == RetryWindow.BeyondRetentionWindow)
                {
                    // The dead caller's session times out, then K's outcome retention elapses.
                    await cluster.DelayAsync(cluster.Options.Timings.SessionTimeout + cluster.Options.Timings.OutcomeRetention + TimeSpan.FromSeconds(10), ct);
                }
            }

            DfsOperationOutcome outcome = await v1.QueryOperationAsync(k, resolve: false, ct);
            if (window == RetryWindow.BeyondRetentionWindow)
            {
                // Outside the retained window: explicit expiry, never "not found" (which would invite a replay) or a new effect.
                Assert.Equal(DfsOutcomeStatus.Expired, outcome.Status);
                await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                    () => ApplyLostReplyMutationAsync(v1, mutation, target, moved, elsewhere, payload, changed, k, changedArguments: false),
                    DfsErrorCode.OperationExpired);
                await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                    () => ApplyLostReplyMutationAsync(v1, mutation, target, moved, elsewhere, payload, changed, k, changedArguments: true),
                    DfsErrorCode.OperationExpired);
            }
            else
            {
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.False(outcome.Token.IsNone);
                Assert.Equal(cluster.ClusterId, outcome.Token.ClusterId);
                if (firstError is null)
                    Assert.Equal(firstToken, outcome.Token);
                else if (historyId != 0)
                    history.Resolve(historyId, OpOutcome.Ok, outcome.Token);

                CommitToken retried = await ApplyLostReplyMutationAsync(v1, mutation, target, moved, elsewhere, payload, changed, k, changedArguments: false);
                Assert.Equal(outcome.Token, retried);   // the original committed result, not a second execution

                await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(
                    () => ApplyLostReplyMutationAsync(v1, mutation, target, moved, elsewhere, payload, changed, k, changedArguments: true),
                    DfsErrorCode.OperationIdMismatch);
            }

            (string finalPath, byte[] expected) = mutation switch
            {
                LostReplyMutation.Append => (target, [.. prefix, .. payload]),
                LostReplyMutation.WriteAll => (target, payload),
                _ => (moved, prefix),
            };

            async Task AssertExactlyOneMutationAsync(DfsVolume volume, string because)
            {
                byte[] actual = await volume.Files.ReadAllBytesAsync(finalPath, ct);
                AssertEx.BytesEqual(expected, actual, because);
                Assert.Equal(inode, (await volume.Files.GetInfoAsync(finalPath, ct)).InodeId);
                if (mutation == LostReplyMutation.Move)
                {
                    Assert.False(await volume.Files.ExistsAsync(target, ct));
                    Assert.False(await volume.Files.ExistsAsync(elsewhere, ct));
                }
            }

            await AssertExactlyOneMutationAsync(v1, "after retry through the new leader");
            if (historyId != 0)
            {
                long read = history.Invoke("c1", "read", target, null);
                history.Complete(read, OpOutcome.Ok, await v1.Files.ReadAllBytesAsync(target, ct));
            }

            // Durability: the crashed leader returns from its durable image and every replica converges on the same result.
            await oldLeader.RestartAsync();
            await cluster.WaitForReplicationAsync(Vol, ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            foreach (FileInspection? view in await ReplicaViewsAsync(cluster, finalPath))
            {
                Assert.NotNull(view);
                Assert.Equal(inode, view.Inode);
                Assert.Equal(expected.Length, view.Length);
            }

            await using DfsClient c2 = await ConnectAsync(cluster, "c2", seeds: [oldLeader.Name]);
            await AssertExactlyOneMutationAsync(c2.GetVolume(Vol), "after the old leader rejoined");

            IReadOnlyList<HistoryOp> ops = history.Operations.Where(o => o.Key == target).ToArray();
            if (mutation == LostReplyMutation.Append)
                LinearizabilityChecker.Check(ops, new AppendLogModel()).AssertValid();
            else if (mutation == LostReplyMutation.WriteAll)
                LinearizabilityChecker.Check(ops, new RegisterModel()).AssertValid();
        }, ct);
    }

    private static Task<CommitToken> ApplyLostReplyMutationAsync(DfsVolume volume, LostReplyMutation mutation, string target, string moved,
        string elsewhere, byte[] payload, byte[] changed, DfsOperationId id, bool changedArguments)
    {
        var options = new DfsMutationOptions { OperationId = id };
        return mutation switch
        {
            LostReplyMutation.Append => volume.Files.AppendAllBytesAsync(target, changedArguments ? changed : payload, options, Ct),
            LostReplyMutation.WriteAll => volume.Files.WriteAllBytesAsync(target, changedArguments ? changed : payload, options, Ct),
            _ => volume.Files.MoveAsync(target, changedArguments ? elsewhere : moved, overwrite: false, options, Ct),
        };
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-002
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>When the zone is lost relative to the policy migration.</summary>
    public enum MigrationInterruption
    {
        /// <summary>While the control group performs the policy transition itself (F21).</summary>
        AtPolicyTransition,

        /// <summary>While migration copies are being written (F06 of the second copy).</summary>
        MidMigrationCopy,
    }

    /// <summary>Each zone (node) lost × each migration stage.</summary>
    public static TheoryData<int, MigrationInterruption> Cha002Cases()
    {
        var data = new TheoryData<int, MigrationInterruption>();
        for (int zone = 0; zone < 3; zone++)
        {
            foreach (MigrationInterruption when in Enum.GetValues<MigrationInterruption>())
                data.Add(zone, when);
        }

        return data;
    }

    /// <summary>
    /// Upgrade the volume policy from 2 copies/2 zones to 3 copies/3 zones with migration, and lose a zone during the
    /// migration. The strong policy must not be reported effective while a zone is missing, the deficit must be exposed,
    /// no old copy may be discarded on a surviving holder before the new placement is established, and the migration
    /// completes once the zone returns.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha002Cases))]
    [Catalog("CHA-002", "S", "INV-08", "INV-09", "INV-11", "INV-18")]
    public async Task CHA_002_ZoneFailureDuringPolicyMigration(int lostNodeIndex, MigrationInterruption interruption)
    {
        CancellationToken ct = Ct;
        var weak = new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 2, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 };
        var strong = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 };
        TestClusterOptions options = Sim(TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec(Vol) { DefaultPolicy = weak }] });

        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/cha002", ct);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 6; i++)
            {
                string path = $"/cha002/f{i}";
                files[path] = TestData.Bytes(2000 + (ulong)i, MultiExtentLength);
                await vol.Files.WriteAllBytesAsync(path, files[path], ct);
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), ct);
            var oldHolders = new Dictionary<ObjectId, IReadOnlyList<string>>();
            foreach (string path in files.Keys)
            {
                foreach (ObjectId obj in await ObjectsOfAsync(cluster, path))
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(obj);
                    Assert.True(holders.Count >= 2, $"precondition: {obj} meets the weak policy before migration");
                    oldHolders[obj] = holders;
                }
            }

            TestNode lost = cluster[lostNodeIndex];
            if (interruption == MigrationInterruption.AtPolicyTransition)
            {
                FaultArm transition = cluster.Faults.Arm(new FaultMatch(FaultSites.Reconfiguration, Operation: DfsOperationKind.PolicyChange), FaultAction.Block());
                Task request = cluster.Admin.SetDefaultPolicyAsync(Vol, strong, migrateExisting: true, ct);
                await transition.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
                cluster.Faults.AssertReached(transition);
                await lost.CrashAsync();
                transition.Release();
                await EnsurePolicyRequestedAsync(cluster, request, strong);
            }
            else
            {
                FaultArm copy = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectWrite, Occurrence: 2), FaultAction.Block());
                await cluster.Admin.SetDefaultPolicyAsync(Vol, strong, migrateExisting: true, ct);
                await copy.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
                cluster.Faults.AssertReached(copy);
                await lost.CrashAsync();
                copy.Release();
            }

            // Safety while the zone is missing: no false activation, visible deficit, no premature old-copy deletion.
            for (int sample = 0; sample < 16; sample++)
            {
                DfsPolicyMigrationStatus status = await cluster.Admin.GetPolicyMigrationStatusAsync(Vol, ct);
                Assert.Equal(strong, status.Target);
                Assert.False(status.Effective, $"strong policy reported effective while zone of {lost.Name} is down (sample {sample})");
                Assert.True(status.PendingObjects > 0, "objects still lacking the third zone must be reported pending");
                DfsReplicaHealth health = await cluster.Admin.GetReplicaHealthAsync(Vol, ct);
                Assert.True(health.ZoneDeficitObjects + health.UnderReplicatedObjects > 0, "the actual zone/copy deficit must be exposed, not reported as full durability");
                foreach ((ObjectId obj, IReadOnlyList<string> holders) in oldHolders)
                {
                    foreach (string holder in holders)
                    {
                        TestNode node = cluster.Node(holder);
                        if (node.State == TestNodeState.Running)
                            Assert.True(await node.Inspect.HasObjectAsync(obj), $"old copy of {obj} on {holder} was discarded before the new placement was established");
                    }
                }

                await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), ct);
            }

            // A write during the outage: either honest placement failure, or success that meets at least the old ack.
            byte[] during = TestData.Bytes(2100, MultiExtentLength);
            try
            {
                await vol.Files.WriteAllBytesAsync("/cha002/during", during, ct);
                files["/cha002/during"] = during;
                foreach (ObjectId obj in await ObjectsOfAsync(cluster, "/cha002/during"))
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
                    Assert.True(copies >= weak.DurableCopiesBeforeAck && zones >= weak.DurableZonesBeforeAck, $"acknowledged write below its policy: {copies} copies / {zones} zones");
                }
            }
            catch (DfsPlacementUnavailableException ex)
            {
                Assert.True(ex.AchievedZones < ex.RequiredZones || ex.AchievedCopies < ex.RequiredCopies, "placement failure must report the actual deficit");
                Assert.Null(await TryReadAllAsync(vol.Files, "/cha002/during"));
            }

            // Liveness once the zone returns: migration completes and every object meets the strong policy.
            await lost.RestartAsync();
            await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetPolicyMigrationStatusAsync(Vol, ct)).Effective,
                TimeSpan.FromSeconds(90), "policy migration completes once all three zones are available again", ct);
            DfsPolicyMigrationStatus final = await cluster.Admin.GetPolicyMigrationStatusAsync(Vol, ct);
            Assert.Equal(0, final.PendingObjects);
            foreach ((string path, byte[] bytes) in files)
            {
                await AssertBytesAsync(bytes, vol.Files.ReadAllBytesAsync(path, ct), path);
                foreach (ObjectId obj in await ObjectsOfAsync(cluster, path))
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
                    Assert.True(copies >= 3 && zones >= 3, $"{path}: {obj} has {copies} copies / {zones} zones after the strong policy became effective");
                }
            }
        }, ct);
    }

    private static async Task EnsurePolicyRequestedAsync(TestCluster cluster, Task request, DfsStoragePolicy policy)
    {
        try
        {
            await request;
        }
        catch (Exception ex) when (IsExplicitUnavailability(ex))
        {
            // The control leader may have been the lost node: re-issue the (idempotent) request once a leader exists.
            Log($"policy request outcome uncertain ({ex.GetType().Name}); re-issuing");
            await cluster.WaitUntilAsync(async () =>
            {
                try
                {
                    await cluster.Admin.SetDefaultPolicyAsync(Vol, policy, migrateExisting: true, Ct);
                    return true;
                }
                catch (Exception retry) when (IsExplicitUnavailability(retry))
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(30), "the policy change can be requested once the control group has a leader", Ct);
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-003
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// §9.3 during resynchronisation: writer A (routed only through the current leader) holds private staged data; the
    /// leader and A are partitioned away, A's lease expires on the new authority, writer B obtains a newer generation and
    /// (variant) commits; optionally the log is compacted beyond the stale node so it must install a checkpoint. On heal
    /// the stale node catches up (installation held open) while A resumes: every publication path from A is rejected
    /// throughout, A's stream faults, B stays authoritative, and A's orphan uploads are eventually reclaimed.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [Catalog("CHA-003", "S", "INV-05", "INV-18")]
    public async Task CHA_003_OldWriterResumeDuringResync(bool replacementCommitted, bool compactBeyondStaleNode)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones), async cluster =>
        {
            const string path = "/cha003/f";
            byte[] baseBytes = TestData.Bytes(3001, MultiExtentLength);
            byte[] aBytes = TestData.Bytes(3002, MultiExtentLength + 50_000);   // A's private data (distinct length)
            byte[] bBytes = TestData.Bytes(3003, MultiExtentLength - 10_000);

            TestNode stale = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] majority = cluster.Nodes.Where(n => n.Name != stale.Name).ToArray();
            var setupOptions = new TestClientOptions { Name = "setup", SeedNodes = Names(majority) };
            var aOptions = new TestClientOptions { Name = "wa", SeedNodes = [stale.Name], OperationTimeout = TimeSpan.FromSeconds(3) };
            var bOptions = new TestClientOptions { Name = "wb", SeedNodes = Names(majority) };

            await using DfsClient setup = await cluster.ConnectAsync(setupOptions, ct);
            DfsVolume setupVol = setup.GetVolume(Vol);
            await setupVol.Directories.CreateDirectoryAsync("/cha003", ct);
            await setupVol.Files.WriteAllBytesAsync(path, baseBytes, ct);

            DfsClient a = await cluster.ConnectAsync(aOptions, ct);
            DfsStream writerA = await a.GetVolume(Vol).Files.OpenAsync(path,
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read }, ct);
            ulong generationA = writerA.WriterGeneration;
            Assert.True(generationA > 0);
            await writerA.WriteAsync(aBytes, ct);   // larger than the buffer: complete chunks are uploaded, nothing is published
            Assert.True(writerA.PendingBytes > 0);

            ulong staleIndex = (await stale.Inspect.GetGroupAsync(Vol))!.AppliedIndex;
            cluster.Network.Partition([stale.Name, aOptions.NetworkName], [.. Names(majority), bOptions.NetworkName, setupOptions.NetworkName]);
            TestNode newLeader = await WaitForLeaderAmongAsync(cluster, majority, Vol, stale.Name, TimeSpan.FromSeconds(30),
                "the majority elects a new leader while the old one is partitioned");
            await cluster.DelayAsync(Lease * 4, ct);   // A's renewals cannot reach the new authority: its lease expires there

            DfsClient b = await cluster.ConnectAsync(bOptions, ct);
            DfsVolume bVol = b.GetVolume(Vol);
            DfsStream writerB = await OpenWriterEventuallyAsync(cluster, bVol, path, TimeSpan.FromSeconds(30));
            ulong generationB = writerB.WriterGeneration;
            Assert.True(generationB > generationA, $"replacement writer generation {generationB} must exceed the expired {generationA}");
            byte[] authoritative = baseBytes;
            if (replacementCommitted)
            {
                await writerB.WriteAsync(bBytes, ct);
                await writerB.SetLengthAsync(bBytes.Length, ct);
                await writerB.CommitAsync(ct);
                authoritative = bBytes;
            }

            // §9.3: a metadata leader change between A's expiry and A's return; the new term must not revive A's authority.
            ulong termBefore = (await newLeader.Inspect.GetGroupAsync(Vol))!.Term;
            await newLeader.CrashAsync();
            await newLeader.RestartAsync();
            newLeader = await WaitForLeaderAmongAsync(cluster, majority, Vol, stale.Name, TimeSpan.FromSeconds(30),
                "the majority re-elects a leader after the leader crash");
            Assert.True((await newLeader.Inspect.GetGroupAsync(Vol))!.Term > termBefore, "a new leadership term began after A's expiry");
            Assert.DoesNotContain((await newLeader.Inspect.GetVolumeStateAsync(Vol))!.Handles,
                h => h.IsWriter && !h.Expired && h.Generation == generationA);

            if (compactBeyondStaleNode)
            {
                await FillLogAsync(setupVol, "/cha003/fill", RaftSnapshotThreshold + 40);
                await cluster.WaitUntilAsync(async () => (await newLeader.Inspect.GetGroupAsync(Vol))!.SnapshotIndex > staleIndex,
                    TimeSpan.FromSeconds(30), "the new leader compacted its log beyond the stale node's last index", ct);
            }

            FaultArm? install = compactBeyondStaleNode
                ? cluster.Faults.Arm(new FaultMatch(FaultSites.RaftSnapshotInstall, Node: stale.Name, Volume: Vol), FaultAction.Block())
                : null;

            // Throughout catch-up: no replica ever applies A's publication, and B's committed state is what clients see.
            SafetyMonitor monitor = SafetyMonitor.Start(cluster, "cha003-no-stale-publication", TimeSpan.FromMilliseconds(250), async () =>
            {
                foreach (TestNode node in Running(cluster))
                {
                    VolumeStateInspection? state = await node.Inspect.GetVolumeStateAsync(Vol);
                    FileInspection? f = state?.GetFile(path);
                    if (f is not null && f.Length == aBytes.Length)
                        return $"{node.Name} applied the expired writer's publication (length {f.Length})";
                }

                byte[] seen = await ReadSharedAsync(bVol.Files, path);   // writer B is still open: use a compatible reader
                return seen.AsSpan().SequenceEqual(authoritative) ? null : $"live read returned {seen.Length} bytes that are not the authoritative revision";
            });

            cluster.Network.Heal();
            if (install is not null)
            {
                await install.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
                cluster.Faults.AssertReached(install);
            }

            // A resumes while the stale node is still catching up.
            Exception first = await Assert.ThrowsAnyAsync<Exception>(() => writerA.FlushAsync(ct));
            await AssertStalePublicationRejectedAsync(bVol, first);
            Assert.True(writerA.IsFaulted, "lease loss must fault the stale writer's stream");

            install?.Release();
            await cluster.WaitForReplicationAsync(Vol, ct);

            // After catch-up the stale writer still cannot publish; the SDK never silently re-acquires authority.
            Exception second = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await writerA.WriteAsync(aBytes.AsMemory(0, 100), ct);
                await writerA.FlushAsync(ct);
            });
            Assert.True(second is DfsLeaseLostException || ReferenceEquals(second, writerA.Fault) || second is DfsCommitOutcomeUnknownException,
                $"further writes on the faulted stream must keep failing, got {second}");
            await monitor.StopAndAssertAsync(minSamples: 3);

            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            foreach (FileInspection? view in await ReplicaViewsAsync(cluster, path))
            {
                Assert.NotNull(view);
                Assert.Equal(generationB, view.WriterGeneration);
                Assert.Equal(authoritative.Length, view.Length);
            }

            if (compactBeyondStaleNode)
                Assert.True((await stale.Inspect.GetGroupAsync(Vol))!.SnapshotIndex > staleIndex, "the stale node caught up through a checkpoint install");

            await using (DfsClient viaStale = await ConnectAsync(cluster, "via-stale", seeds: [stale.Name]))
                await AssertBytesAsync(authoritative, ReadSharedAsync(viaStale.GetVolume(Vol).Files, path), "read routed via the returned node");

            // Orphans: A's uploads are reclaimed once its handle/staging session is safely abandoned.
            try
            {
                await writerA.DisposeAsync();
            }
            catch (Exception ex)
            {
                Log($"disposing the faulted stale writer surfaced {ex.GetType().Name} (expected)");
            }

            await writerB.DisposeAsync();
            await a.DisposeAsync();
            await b.DisposeAsync();
            await cluster.WaitUntilAsync(async () =>
            {
                IReadOnlySet<ObjectId> reachable = (await cluster.Inspect.GetVolumeStateAsync(Vol)).ReachableObjects();
                IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
                return stored.IsSubsetOf(reachable);
            }, TimeSpan.FromSeconds(60), "the expired writer's orphan uploads are reclaimed once safely abandoned", ct);
        }, ct);
    }

    private static async Task AssertStalePublicationRejectedAsync(DfsVolume authority, Exception failure)
    {
        if (failure is DfsCommitOutcomeUnknownException unknown)
        {
            DfsOperationOutcome resolved = await authority.QueryOperationAsync(unknown.OperationId, resolve: true, Ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
            return;
        }

        Assert.True(failure is DfsLeaseLostException, $"a stale writer's publication must be rejected with DfsLeaseLostException, got {failure}");
    }

    private static async Task FillLogAsync(DfsVolume volume, string prefix, int count)
    {
        for (int i = 0; i < count; i++)
            await volume.Files.WriteAllBytesAsync($"{prefix}{i % 4}", TestData.Bytes(90_000 + (ulong)i, 64), Ct);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-004
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Interruption of the returning node's checkpoint installation: remote install (two crash modes) or local recovery install.</summary>
    public static TheoryData<string, CrashMode> Cha004Cases() => new()
    {
        { FaultSites.RaftSnapshotInstall, CrashMode.LoseUnflushed },
        { FaultSites.RaftSnapshotInstall, CrashMode.TearLastWrite },
        { FaultSites.RecoveryCheckpointInstall, CrashMode.LoseUnflushed },
    };

    /// <summary>
    /// §9.4: C (holding inode I of /x, old mappings and an old request window) is stopped; A/B overwrite, truncate, rename
    /// and delete I, recreate /x as J, commit a multi-file transaction, create/delete snapshots, open a writer, retire old
    /// operation ids and compact. C returns through an interrupted checkpoint installation. C never serves stale live
    /// reads while catching up; afterwards (with A stopped) B/C serve J, no resurrection, complete transactions, snapshot
    /// references, fencing generations, reservations and replay rejection.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha004Cases))]
    [Catalog("CHA-004", "S", "INV-06", "INV-07", "INV-18")]
    public async Task CHA_004_DeleteThenLongOutageThenCompaction(string interruptSite, CrashMode interruptMode)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones), async cluster =>
        {
            const string dir = "/cha004";
            byte[] x1 = TestData.Bytes(4001, MultiExtentLength);
            byte[] x2 = TestData.Bytes(4002, MultiExtentLength + 70_000);
            byte[] x3 = TestData.Bytes(4003, MultiExtentLength - 5_000);
            byte[] retiredSuffix = TestData.Bytes(4004, 3_000);
            byte[] keptSuffix = TestData.Bytes(4005, 3_000);
            byte[] t1 = TestData.Bytes(4006, 9_000);
            byte[] t2 = TestData.Bytes(4007, 9_000);

            TestNode a = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] others = cluster.Nodes.Where(n => n.Name != a.Name).ToArray();
            TestNode b = others[0];
            TestNode c = others[1];

            await using DfsClient c0 = await ConnectAsync(cluster, "c0");
            DfsVolume v = c0.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync(dir, ct);
            await v.Files.WriteAllBytesAsync($"{dir}/x", x1, ct);
            InodeId inodeI = (await v.Files.GetInfoAsync($"{dir}/x", ct)).InodeId;
            await v.Files.WriteAllBytesAsync($"{dir}/y", TestData.Bytes(4008, 5_000), ct);
            await v.Files.WriteAllBytesAsync($"{dir}/log", ReadOnlyMemory<byte>.Empty, ct);
            DfsOperationId retiredId = await v.NewOperationIdAsync(ct);
            await v.Files.AppendAllBytesAsync($"{dir}/log", retiredSuffix, new DfsMutationOptions { OperationId = retiredId }, ct);
            await v.Snapshots.CreateAsync("cha004-s0", ct: ct);
            await FillLogAsync(v, $"{dir}/pre", RaftSnapshotThreshold + 20);   // C also holds a local checkpoint
            await cluster.WaitForReplicationAsync(Vol, ct);
            ulong cIndexAtStop = (await c.Inspect.GetGroupAsync(Vol))!.AppliedIndex;
            await c.StopAsync();

            // History C never sees.
            await v.Files.WriteAllBytesAsync($"{dir}/x", x2, ct);
            await using (DfsStream truncate = await v.Files.OpenAsync($"{dir}/x",
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read }, ct))
            {
                await truncate.SetLengthAsync(1_000, ct);
                await truncate.FlushAsync(ct);
            }

            await v.Files.MoveAsync($"{dir}/x", $"{dir}/x-renamed", ct: ct);
            await v.Files.DeleteAsync($"{dir}/x-renamed", ct);
            await v.Files.WriteAllBytesAsync($"{dir}/x", x3, ct);
            InodeId inodeJ = (await v.Files.GetInfoAsync($"{dir}/x", ct)).InodeId;
            Assert.NotEqual(inodeI, inodeJ);
            await using (DfsTransaction tx = await v.Transactions.BeginAsync(ct: ct))
            {
                await tx.Files.WriteAllBytesAsync($"{dir}/t1", t1, ct);
                await tx.Files.WriteAllBytesAsync($"{dir}/t2", t2, ct);
                await tx.Files.DeleteAsync($"{dir}/y", ct);
                await tx.CommitAsync(ct);
            }

            await v.Snapshots.CreateAsync("cha004-s1", ct: ct);
            await v.Snapshots.DeleteAsync("cha004-s0", ct);
            DfsStream writer = await v.Files.OpenAsync($"{dir}/w",
                new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.Read }, ct);
            ulong writerGeneration = writer.WriterGeneration;
            await FillLogAsync(v, $"{dir}/fill", NodeTimings.Fast.MaxOutcomesPerSession + 20);
            await cluster.DelayAsync(NodeTimings.Fast.OutcomeRetention + TimeSpan.FromSeconds(5), ct);
            DfsOperationId keptId = await v.NewOperationIdAsync(ct);
            CommitToken keptToken = await v.Files.AppendAllBytesAsync($"{dir}/log", keptSuffix, new DfsMutationOptions { OperationId = keptId }, ct);
            byte[] expectedLog = [.. retiredSuffix, .. keptSuffix];
            await cluster.WaitUntilAsync(async () => (await (await cluster.GetLeaderAsync(Vol, ct)).Inspect.GetGroupAsync(Vol))!.SnapshotIndex > cIndexAtStop,
                TimeSpan.FromSeconds(30), "the missing history was compacted, forcing checkpoint catch-up", ct);

            // C returns; its first installation is interrupted. c2 can reach only C.
            FaultArm interrupt = cluster.Faults.Arm(new FaultMatch(interruptSite, Node: c.Name), FaultAction.CrashNode(interruptMode));
            var c2Options = new TestClientOptions
            {
                Name = "c2",
                SeedNodes = [c.Name],
                OperationTimeout = TimeSpan.FromSeconds(2),
                ConnectTimeout = TimeSpan.FromSeconds(2),   // keep each probe well inside the monitor's 5 s sample budget
            };
            cluster.Network.CutBoth(c2Options.NetworkName, a.Name);
            cluster.Network.CutBoth(c2Options.NetworkName, b.Name);
            SafetyMonitor staleReads = SafetyMonitor.Start(cluster, "cha004-no-stale-live-read", TimeSpan.FromSeconds(1), async () =>
            {
                if (c.State != TestNodeState.Running)
                    return null;
                DfsClient? probe = null;
                try
                {
                    probe = await cluster.ConnectAsync(c2Options, ct);
                    byte[] seen = await probe.GetVolume(Vol).Files.ReadAllBytesAsync($"{dir}/x", ct);
                    return seen.AsSpan().SequenceEqual(x3) ? null : $"{c.Name} served {seen.Length} bytes of /x that are not inode J's revision";
                }
                catch (FileNotFoundException)
                {
                    return $"{c.Name} reported /x absent while catching up (false absence)";
                }
                catch (Exception ex) when (IsExplicitUnavailability(ex))
                {
                    return null;
                }
                finally
                {
                    if (probe is not null)
                        await probe.DisposeAsync();
                }
            });

            try
            {
                await c.RestartAsync();
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                Log($"first restart of {c.Name} ended with {ex.GetType().Name} (crash injected during recovery)");
            }

            await interrupt.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
            cluster.Faults.AssertReached(interrupt);
            await cluster.WaitUntilAsync(() => Task.FromResult(c.State == TestNodeState.Crashed), TimeSpan.FromSeconds(30),
                "the returning node crashed at the injected installation point", ct);
            await c.RestartAsync();
            await cluster.WaitForReplicationAsync(Vol, ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);   // let the probe observe the caught-up replica too
            await staleReads.StopAndAssertAsync(minSamples: 2);
            cluster.Network.Heal();
            Assert.True((await c.Inspect.GetGroupAsync(Vol))!.SnapshotIndex > cIndexAtStop, "C recovered through a checkpoint, not a log suffix");

            // Stop A: B and C alone serve the authoritative state.
            await a.StopAsync();
            await using DfsClient c3 = await ConnectAsync(cluster, "c3", seeds: [b.Name, c.Name]);
            DfsVolume v3 = c3.GetVolume(Vol);
            Assert.Equal(inodeJ, (await v3.Files.GetInfoAsync($"{dir}/x", ct)).InodeId);
            await AssertBytesAsync(x3, v3.Files.ReadAllBytesAsync($"{dir}/x", ct), "/x must be inode J's bytes, not I's");
            Assert.False(await v3.Files.ExistsAsync($"{dir}/x-renamed", ct));
            Assert.False(await v3.Files.ExistsAsync($"{dir}/y", ct));
            await AssertBytesAsync(t1, v3.Files.ReadAllBytesAsync($"{dir}/t1", ct));
            await AssertBytesAsync(t2, v3.Files.ReadAllBytesAsync($"{dir}/t2", ct));

            VolumeStateInspection cState = (await c.Inspect.GetVolumeStateAsync(Vol))!;
            FileInspection cx = cState.GetFile($"{dir}/x")!;
            Assert.Equal(inodeJ, cx.Inode);
            Assert.Equal(x3.Length, cx.Length);
            Assert.Null(cState.GetFile($"{dir}/x-renamed"));
            Assert.Null(cState.GetFile($"{dir}/y"));
            Assert.NotNull(cState.GetFile($"{dir}/t1"));
            Assert.NotNull(cState.GetFile($"{dir}/t2"));
            Assert.DoesNotContain(cState.Snapshots, s => s.Name == "cha004-s0");
            Assert.Contains(cState.Snapshots, s => s.Name == "cha004-s1");
            FileInspection w = cState.GetFile($"{dir}/w")!;
            Assert.Equal(writerGeneration, w.WriterGeneration);
            Assert.Contains(cState.Handles, h => h.Inode == w.Inode && h.IsWriter && h.Generation == writerGeneration && !h.Expired);
            await AssertEx.ThrowsDfsAsync<DfsSharingViolationException>(() => v3.Files.OpenAsync($"{dir}/w",
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read }, ct));

            await using (DfsSnapshotView s1 = await v3.Snapshots.OpenAsync("cha004-s1", ct))
            {
                await AssertBytesAsync(x3, s1.Files.ReadAllBytesAsync($"{dir}/x", ct));
                await AssertBytesAsync(t1, s1.Files.ReadAllBytesAsync($"{dir}/t1", ct));
                Assert.False(await s1.Files.ExistsAsync($"{dir}/y", ct));
            }

            await AssertEx.ThrowsDfsAsync<DfsException>(() => v3.Snapshots.GetAsync("cha004-s0", ct), DfsErrorCode.SnapshotNotFound);

            // Replay rejection survived the checkpoint: the retired id is expired, never re-executed.
            await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(() => v3.Files.AppendAllBytesAsync($"{dir}/log", retiredSuffix,
                new DfsMutationOptions { OperationId = retiredId }, ct), DfsErrorCode.OperationExpired);
            try
            {
                Assert.Equal(keptToken, await v3.Files.AppendAllBytesAsync($"{dir}/log", keptSuffix, new DfsMutationOptions { OperationId = keptId }, ct));
            }
            catch (DfsOperationExpiredException)
            {
                // Legal once the client acknowledged the outcome; what matters is that it is never applied twice.
            }

            await AssertBytesAsync(expectedLog, v3.Files.ReadAllBytesAsync($"{dir}/log", ct), "no replayed append");

            await writer.DisposeAsync();
            await a.RestartAsync();
            await cluster.WaitForReplicationAsync(Vol, ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-005
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Which source-side transfer-pin budget the offline backup exceeds.</summary>
    public enum PinBudget
    {
        /// <summary>Pin age budget (MaxPinAge).</summary>
        Age,

        /// <summary>Pinned-bytes budget (MaxPinnedBytes).</summary>
        Bytes,
    }

    /// <summary>
    /// The late-sync target is offline while overwrites and snapshots accumulate source history. Live commits never wait for
    /// it; once a pin budget is exceeded the incremental lineage is abandoned (pins released, source history reclaimed,
    /// target marked for reseed); the destination keeps its last complete snapshot through the outage and the reseed, which
    /// runs under a new lineage and covers the latest commit.
    /// </summary>
    [Theory]
    [InlineData(PinBudget.Age)]
    [InlineData(PinBudget.Bytes)]
    [Catalog("CHA-005", "S", "INV-09", "INV-13", "INV-15", "INV-16")]
    public async Task CHA_005_BackupOfflinePlusSourceGcPressure(PinBudget exceeded)
    {
        CancellationToken ct = Ct;
        DfsBackupBudgets budgets = exceeded == PinBudget.Age
            ? new DfsBackupBudgets(MaxPinAge: TimeSpan.FromSeconds(5), MaxPinnedBytes: 1L << 30)
            : new DfsBackupBudgets(MaxPinAge: TimeSpan.FromHours(1), MaxPinnedBytes: 2L << 20);

        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones with { BackupNodeCount = 1 }, 20), async cluster =>
        {
            TestBackupNode backup = cluster.BackupNodes[0];
            string target = backup.TargetName;
            await backup.RegisterAsync(Vol, new DfsBackupSchedule(OnEverySnapshot: true), budgets);

            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/cha005", ct);
            string[] paths = ["/cha005/a", "/cha005/b", "/cha005/c"];
            var s1State = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < paths.Length; i++)
            {
                s1State[paths[i]] = TestData.Bytes(5000 + (ulong)i, MultiExtentLength);
                await vol.Files.WriteAllBytesAsync(paths[i], s1State[paths[i]], ct);
            }

            await vol.Snapshots.CreateAsync("cha005-s1", ct: ct);
            DfsBackupCheckpoint s1 = await vol.Backups.AwaitBackupSnapshotAsync(target, "cha005-s1", TimeSpan.FromSeconds(60), ct);
            Assert.True(s1.IsExactSnapshot);
            Assert.Equal("cha005-s1", s1.SnapshotName);
            Guid oldLineage = (await vol.Backups.GetStatusAsync(target, ct)).LineageId;
            var s1Objects = new HashSet<ObjectId>();
            foreach (string path in paths)
                s1Objects.UnionWith(await ObjectsOfAsync(cluster, path));

            await backup.StopAsync();

            // Overwrites and snapshots fill source history while the target is offline; commits must not wait for it.
            TimeSpan slowest = TimeSpan.Zero;
            CommitToken last = CommitToken.None;
            var current = new Dictionary<string, byte[]>(s1State, StringComparer.Ordinal);
            for (int round = 2; round <= 7; round++)
            {
                for (int i = 0; i < paths.Length; i++)
                {
                    byte[] bytes = TestData.Bytes(5000 + ((ulong)round * 10) + (ulong)i, MultiExtentLength);
                    long started = cluster.Time.GetTimestamp();
                    last = await vol.Files.WriteAllBytesAsync(paths[i], bytes, ct);
                    TimeSpan took = cluster.Time.GetElapsedTime(started);
                    slowest = took > slowest ? took : slowest;
                    current[paths[i]] = bytes;
                }

                await vol.Snapshots.CreateAsync($"cha005-s{round}", ct: ct);
                await vol.Snapshots.DeleteAsync($"cha005-s{round - 1}", ct);
                await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);
            }

            Log($"slowest live commit while the backup target was offline: {slowest}");
            Assert.True(slowest < TimeSpan.FromSeconds(5), $"live commits must not depend on the offline backup (slowest {slowest})");

            await cluster.WaitUntilAsync(async () => (await vol.Backups.GetStatusAsync(target, ct)).ReseedRequired,
                TimeSpan.FromSeconds(120), "the offline target's incremental lineage is abandoned once its pin budget is exceeded", ct);
            DfsBackupStatus abandoned = await vol.Backups.GetStatusAsync(target, ct);
            Assert.True(abandoned.State is DfsBackupState.ReseedRequired or DfsBackupState.Offline, $"unexpected state {abandoned.State}");
            if (exceeded == PinBudget.Bytes)
                Assert.True(abandoned.PinnedSourceBytes <= budgets.MaxPinnedBytes, $"pinned source bytes {abandoned.PinnedSourceBytes} exceed the budget");

            // Source history is no longer retained for the target: superseded S1 objects are reclaimed.
            await WaitUntilReclaimedAsync(cluster, s1Objects, TimeSpan.FromSeconds(60),
                "objects only the abandoned transfer lineage referenced are garbage collected (live storage stays bounded)");
            CommitToken afterAbandon = await vol.Files.WriteAllBytesAsync("/cha005/after", TestData.Bytes(5099, 4_000), ct);
            Assert.True(afterAbandon.IsAtOrAfter(last));

            // The destination's last complete snapshot is still published and readable after it returns.
            await backup.RestartAsync();
            Assert.Contains("cha005-s1", await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
            await using (DfsClient reader = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "b-reader", Credentials = backup.ReaderCredentials }, ct))
            await using (DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("cha005-s1", ct))
            {
                foreach ((string path, byte[] bytes) in s1State)
                    await AssertBytesAsync(bytes, view.Files.ReadAllBytesAsync(path, ct), $"protected destination history {path}");
            }

            // Explicit reseed under a new lineage covers the latest live commit.
            await vol.Snapshots.CreateAsync("cha005-reseed", ct: ct);
            await vol.Backups.RequestSyncAsync(target, "cha005-reseed", ct);
            DfsBackupCheckpoint reseeded = await vol.Backups.AwaitBackupSnapshotAsync(target, "cha005-reseed", TimeSpan.FromSeconds(120), ct);
            Assert.True(reseeded.IsExactSnapshot);
            Assert.True(reseeded.CoveredCommit.IsAtOrAfter(afterAbandon));
            DfsBackupStatus after = await vol.Backups.GetStatusAsync(target, ct);
            Assert.False(after.ReseedRequired);
            Assert.NotEqual(oldLineage, after.LineageId);
            Assert.Contains("cha005-s1", await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
            await using (DfsClient reader = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "b-reader2", Credentials = backup.ReaderCredentials }, ct))
            await using (DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("cha005-reseed", ct))
            {
                foreach ((string path, byte[] bytes) in current)
                    await AssertBytesAsync(bytes, view.Files.ReadAllBytesAsync(path, ct), $"reseeded {path}");
            }
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-006
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Points at which the next snapshot's transfer is incomplete when the source is lost.</summary>
    public static TheoryData<string, int> Cha006Cases() => new()
    {
        { FaultSites.BackupInventory, 1 },
        { FaultSites.BackupObjectReceived, 1 },
        { FaultSites.BackupObjectReceived, 2 },
    };

    /// <summary>
    /// The backup holds complete S1; S2 (overwrite, delete, new multi-object file) is mid-transfer when every source node
    /// and its storage is destroyed. The backup keeps S1 as its only published snapshot (also after a cold restart),
    /// reports S2 as unavailable and the newer interval (S1 boundary, T2] as missing, and a restore onto clean hosts
    /// reproduces exactly S1; restoring S2 is refused.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha006Cases))]
    [Catalog("CHA-006", "S", "INV-10", "INV-13", "INV-19")]
    public async Task CHA_006_SourceLossDuringPartialBackup(string interruptSite, int occurrence)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones with { BackupNodeCount = 1 }, 20), async cluster =>
        {
            TestBackupNode backup = cluster.BackupNodes[0];
            string target = backup.TargetName;
            await backup.RegisterAsync(Vol, new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30));

            DfsClient client = await ConnectAsync(cluster, "src");   // dies with the source; never disposed
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/cha006", ct);
            var s1State = new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["/cha006/f1"] = TestData.Bytes(6001, MultiExtentLength),
                ["/cha006/f2"] = TestData.Bytes(6002, MultiExtentLength),
                ["/cha006/f3"] = TestData.Bytes(6003, 1_000),
            };
            foreach ((string path, byte[] bytes) in s1State)
                await vol.Files.WriteAllBytesAsync(path, bytes, ct);
            await vol.Snapshots.CreateAsync("cha006-s1", ct: ct);
            await vol.Backups.RequestSyncAsync(target, "cha006-s1", ct);
            DfsBackupCheckpoint cp1 = await vol.Backups.AwaitBackupSnapshotAsync(target, "cha006-s1", TimeSpan.FromSeconds(60), ct);
            Assert.True(cp1.IsExactSnapshot);
            VolumeId sourceVolume = (await vol.GetInfoAsync(ct)).Id;
            string? pointerS1 = await backup.Inspect.GetPublishedPointerAsync(Vol);
            Assert.NotNull(pointerS1);

            await vol.Files.WriteAllBytesAsync("/cha006/f1", TestData.Bytes(6011, MultiExtentLength), ct);
            await vol.Files.DeleteAsync("/cha006/f2", ct);
            CommitToken t2 = await vol.Files.WriteAllBytesAsync("/cha006/f4", TestData.Bytes(6014, 2 * MultiExtentLength), ct);
            DfsSnapshotInfo s2 = await vol.Snapshots.CreateAsync("cha006-s2", ct: ct);
            Assert.True(s2.Boundary.IsAtOrAfter(t2));
            Assert.True(cp1.CoveredCommit.CompareTo(t2) < 0);

            FaultArm interrupt = cluster.Faults.Arm(new FaultMatch(interruptSite, Occurrence: occurrence), FaultAction.Block());
            await vol.Backups.RequestSyncAsync(target, "cha006-s2", ct);
            await interrupt.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
            cluster.Faults.AssertReached(interrupt);

            // Every source node and all of its storage is lost while S2 is incomplete at the destination.
            cluster.KillClient(client);
            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync();
            foreach (TestNode node in cluster.Nodes)
                await node.ReplaceDiskWithEmptyAsync();
            interrupt.Release();
            await cluster.DelayAsync(TimeSpan.FromSeconds(10), ct);

            async Task AssertOnlyS1PublishedAsync(string when)
            {
                IReadOnlyList<string> published = await backup.Inspect.GetPublishedSnapshotsAsync(Vol);
                Assert.Contains("cha006-s1", published);
                Assert.DoesNotContain("cha006-s2", published);
                Assert.Equal(pointerS1, await backup.Inspect.GetPublishedPointerAsync(Vol));
                Log($"{when}: published={string.Join(",", published)} staged={await backup.Inspect.GetStagedBytesAsync()}");
            }

            await AssertOnlyS1PublishedAsync("after source loss");
            await backup.CrashAsync();
            await backup.RestartAsync();
            await AssertOnlyS1PublishedAsync("after backup cold restart");

            await using (DfsClient reader = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "b-reader", Credentials = backup.ReaderCredentials }, ct))
            {
                DfsSnapshotClient snapshots = reader.GetVolume(Vol).Snapshots;
                IReadOnlyList<DfsSnapshotInfo> listed = await snapshots.ListAsync(ct);
                Assert.Contains(listed, s => s.Name == "cha006-s1");
                Assert.DoesNotContain(listed, s => s.Name == "cha006-s2");
                DfsSnapshotInfo s1Info = await snapshots.GetAsync("cha006-s1", ct);
                Assert.Equal(cp1.CoveredCommit, s1Info.Boundary);
                Assert.True(s1Info.Boundary.CompareTo(t2) < 0, "the newest recoverable boundary precedes the lost interval (S1, T2]");
                await AssertEx.ThrowsDfsAsync<DfsException>(() => snapshots.GetAsync("cha006-s2", ct), DfsErrorCode.SnapshotNotFound);
            }

            // Restore on clean hosts from the backup alone.
            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RecoveryOptions(cluster), ct);
            DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(
                new DfsRestoreFromBackupRequest([backup.Endpoint], backup.ReaderCredentials, Vol, "cha006-s1", "restored"), ct);
            Assert.Equal("restored", restored.Name);
            Assert.NotEqual(sourceVolume, restored.Id);
            Assert.Equal(recovery.ClusterId, restored.LastCommit.ClusterId);
            await AssertEx.ThrowsDfsAsync<DfsException>(() => recovery.Admin.RestoreFromBackupAsync(
                new DfsRestoreFromBackupRequest([backup.Endpoint], backup.ReaderCredentials, Vol, "cha006-s2", "restored-s2"), ct), DfsErrorCode.SnapshotNotFound);

            await using DfsClient rc = await recovery.ConnectAsync(new TestClientOptions { Name = "rc" }, ct);
            DfsVolume rv = rc.GetVolume("restored");
            foreach ((string path, byte[] bytes) in s1State)
                await AssertBytesAsync(bytes, rv.Files.ReadAllBytesAsync(path, ct), $"restored {path} equals S1");
            Assert.False(await rv.Files.ExistsAsync("/cha006/f4", ct), "nothing from the incomplete S2 is invented");

            // The namespace record matches S1 exactly (f2 deleted only in the lost interval, f4 created only there).
            var restoredNames = new List<string>();
            await foreach (DfsEntryInfo entry in rv.Directories.EnumerateAsync("/cha006", ct: ct))
            {
                restoredNames.Add(entry.Name);
                Assert.Equal(DfsEntryKind.File, entry.Kind);
                Assert.True(s1State.TryGetValue(entry.Path, out byte[]? expectedBytes), $"restored volume contains {entry.Path}, which is not in S1");
                Assert.Equal(expectedBytes.Length, entry.Length);
            }

            Assert.Equal(["f1", "f2", "f3"], restoredNames.Order(StringComparer.Ordinal).ToArray());
        }, ct);
    }

    private static TestClusterOptions RecoveryOptions(TestCluster parent) => new()
    {
        Nodes = [new TestNodeSpec("r0", "z1"), new TestNodeSpec("r1", "z2"), new TestNodeSpec("r2", "z3")],
        Volumes = [],
        Mode = parent.Mode,
        Transport = parent.Options.Transport,
        Storage = parent.Options.Storage,
        ScenarioTimeout = parent.Options.ScenarioTimeout,
    };

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-007
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Whose data disk is filled down to its reserve.</summary>
    public enum FullDisk
    {
        /// <summary>The volume leader's disk.</summary>
        Leader,

        /// <summary>One follower's disk.</summary>
        Follower,
    }

    /// <summary>Each filled disk × each leader-crash point during the transaction commit (F10 before propose, F10 after local append, F11 after commit).</summary>
    public static TheoryData<FullDisk, string> Cha007Cases()
    {
        var data = new TheoryData<FullDisk, string>();
        foreach (FullDisk full in Enum.GetValues<FullDisk>())
        {
            foreach (string site in new[] { FaultSites.RaftBeforePropose, FaultSites.RaftAfterLocalAppend, FaultSites.RaftAfterCommitBeforeApply })
                data.Add(full, site);
        }

        return data;
    }

    /// <summary>
    /// A multi-file transaction stages data while one disk is full down to its reserve, and the leader crashes during the
    /// commit. The client's report must be a commit, a definite abort, or an unknown outcome; the authoritative resolution
    /// must agree with it (a definite abort never becomes a commit); visibility is all-or-nothing; concurrent read-only
    /// transactions observe a strictly serializable history throughout; and a commit never relies on data below its policy.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha007Cases))]
    [Catalog("CHA-007", "S", "INV-02", "INV-04", "INV-06", "INV-16")]
    public async Task CHA_007_DiskFullDuringTransactionAndLeaderChange(FullDisk full, string crashSite)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones), async cluster =>
        {
            const string dir = "/cha007";
            byte[] cOld = TestData.Bytes(7001, MultiExtentLength);
            byte[] cNew = TestData.Bytes(7002, MultiExtentLength + 1_000);
            await using DfsClient setup = await ConnectAsync(cluster, "setup");
            DfsVolume sv = setup.GetVolume(Vol);
            await sv.Directories.CreateDirectoryAsync(dir, ct);
            await sv.Files.WriteAllTextAsync($"{dir}/a", "a0", ct: ct);
            await sv.Files.WriteAllTextAsync($"{dir}/b", "b0", ct: ct);
            await sv.Files.WriteAllBytesAsync($"{dir}/c", cOld, ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode filled = full == FullDisk.Leader ? leader : cluster.Nodes.First(n => n.Name != leader.Name);
            FillToReserve(cluster, filled);

            var clock = new LogicalClock();
            var records = new List<TxRecord>();
            var observerGate = new ObserverGate();
            using var stopReaders = new CancellationTokenSource();
            Task readers = RunReadOnlyTransactionsAsync(cluster, "observer", dir, clock, records, stopReaders.Token, observerGate);

            await using DfsClient writerClient = await ConnectAsync(cluster, "writer", operationTimeout: TimeSpan.FromSeconds(5));
            DfsTransaction tx = await writerClient.GetVolume(Vol).Transactions.BeginAsync(ct: ct);
            long begin = clock.Tick();
            await tx.Files.WriteAllTextAsync($"{dir}/a", "a1", ct: ct);
            await tx.Files.WriteAllTextAsync($"{dir}/b", "b1", ct: ct);
            await tx.Files.WriteAllBytesAsync($"{dir}/c", cNew, ct);   // staged on the nodes that still have space

            // The crash arm matches TxCommit: hold the observers' (read-only) commits back until the writer's commit has hit
            // it, so the injected crash is the writer's commit and not an observer's; they resume during the in-doubt period.
            observerGate.Paused = true;
            await cluster.WaitUntilAsync(() => Task.FromResult(!observerGate.Busy), TimeSpan.FromSeconds(30), "observers are between transactions", ct);
            FaultArm crash = cluster.Faults.Arm(new FaultMatch(crashSite, Node: leader.Name, Volume: Vol, Operation: DfsOperationKind.TxCommit), FaultAction.CrashNode());
            Task<CommitToken> commit = tx.CommitAsync(ct);
            await crash.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(crash);
            observerGate.Paused = false;
            CommitToken committedToken = CommitToken.None;
            Exception? commitError = null;
            try
            {
                committedToken = await commit;
            }
            catch (Exception ex)
            {
                commitError = ex;
            }

            long end = clock.Tick();
            OpOutcome reported = commitError is null ? OpOutcome.Ok : HistoryRecorder.Classify(commitError);
            if (commitError is not null)
            {
                Assert.True(commitError is DfsCommitOutcomeUnknownException or DfsTransactionConflictException or DfsUnavailableException,
                    $"commit must report unknown, abort/conflict or unavailability, got {commitError}");
            }

            // Recovery: space returns, the crashed leader restarts, the outcome is resolved authoritatively.
            Unfill(filled);
            if (leader.State == TestNodeState.Crashed)
                await leader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
            await using DfsClient resolver = await ConnectAsync(cluster, "resolver");
            DfsVolume rv = resolver.GetVolume(Vol);
            DfsOperationOutcome resolved = await rv.Transactions.QueryOutcomeAsync(tx.Id, resolve: true, ct);
            long resolvedAt = clock.Tick();
            Assert.True(resolved.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolve must be definite, got {resolved.Status}");
            if (reported == OpOutcome.Ok)
            {
                Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
                Assert.Equal(committedToken, resolved.Token);
            }
            else if (reported == OpOutcome.Failed)
            {
                Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);   // a definite abort can never later commit
            }

            bool committed = resolved.Status == DfsOutcomeStatus.Committed;
            async Task AssertAllOrNothingAsync(string when)
            {
                Assert.Equal(committed ? "a1" : "a0", await rv.Files.ReadAllTextAsync($"{dir}/a", ct: ct));
                Assert.Equal(committed ? "b1" : "b0", await rv.Files.ReadAllTextAsync($"{dir}/b", ct: ct));
                await AssertBytesAsync(committed ? cNew : cOld, rv.Files.ReadAllBytesAsync($"{dir}/c", ct), when);
            }

            await AssertAllOrNothingAsync("after resolution");
            if (committed)
            {
                foreach (ObjectId obj in await ObjectsOfAsync(cluster, $"{dir}/c"))
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
                    Assert.True(copies >= 2 && zones >= 2, $"committed data below its ack policy under disk pressure: {copies}/{zones}");
                }
            }

            await cluster.DelayAsync(TimeSpan.FromSeconds(5), ct);
            await AssertAllOrNothingAsync("after further time (resolution is final)");

            // Cancel inline on the simulation thread: CancelAsync runs the token callbacks (cancelled Task.Delay waits, their
            // timers and continuations) on the thread pool, an uncontrolled dependency of the deterministic run (HAR-002).
            stopReaders.Cancel();
            await readers;
            Assert.True(records.Count >= 2, $"observers completed only {records.Count} read-only transactions");
            OpOutcome writerOutcome = committed ? OpOutcome.Ok : OpOutcome.Failed;
            records.Add(new TxRecord("writer", begin, end, writerOutcome,
                new Dictionary<string, string?>(StringComparer.Ordinal),
                new Dictionary<string, string?>(StringComparer.Ordinal) { [$"{dir}/a"] = "a1", [$"{dir}/b"] = "b1" },
                [])
            { ResolveTime = reported == OpOutcome.Unknown && committed ? resolvedAt : null });
            SerializabilityChecker.CheckStrict(records, new Dictionary<string, string?>(StringComparer.Ordinal) { [$"{dir}/a"] = "a0", [$"{dir}/b"] = "b0" })
                .AssertValid();
        }, ct);
    }

    /// <summary>
    /// CHA-007 variant: required storage on both followers is exhausted down to the reserve, so the transaction's data cannot
    /// meet its acknowledgement policy. The transaction fails explicitly (capacity/placement or an unknown outcome), the
    /// leader then crashes so the outcome is resolved by a new leader whose disk is still full (metadata progress uses the
    /// reserve): it resolves to aborted, nothing becomes visible, and once space returns a new transaction commits.
    /// </summary>
    [Fact]
    [Catalog("CHA-007", "S", "INV-02", "INV-06", "INV-08", "INV-16")]
    public async Task CHA_007_DiskFullDuringTransactionAndLeaderChange_RequiredStorageExhausted()
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones), async cluster =>
        {
            const string dir = "/cha007x";
            byte[] cOld = TestData.Bytes(7101, MultiExtentLength);
            await using DfsClient client = await ConnectAsync(cluster, "c0", operationTimeout: TimeSpan.FromSeconds(5));
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync(dir, ct);
            await vol.Files.WriteAllTextAsync($"{dir}/a", "a0", ct: ct);
            await vol.Files.WriteAllBytesAsync($"{dir}/c", cOld, ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();
            foreach (TestNode follower in followers)
                FillToReserve(cluster, follower);

            DfsTransaction tx = await vol.Transactions.BeginAsync(ct: ct);
            Exception failure = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await tx.Files.WriteAllTextAsync($"{dir}/a", "a1", ct: ct);
                await tx.Files.WriteAllBytesAsync($"{dir}/c", TestData.Bytes(7102, 4 * MultiExtentLength), ct);
                await tx.CommitAsync(ct);
            });
            Assert.True(failure is DfsCapacityException or DfsPlacementUnavailableException or DfsCommitOutcomeUnknownException,
                $"exhausted required storage must surface as capacity/placement unavailability (or an unknown outcome), got {failure}");
            try
            {
                await tx.DisposeAsync();
            }
            catch (Exception ex)
            {
                Log($"disposing the failed transaction surfaced {ex.GetType().Name}");
            }

            // Leader change while the followers' disks are still full: the outcome is resolved by the new authority.
            await leader.CrashAsync();
            TestNode newLeader = await WaitForLeaderAmongAsync(cluster, followers, Vol, leader.Name, TimeSpan.FromSeconds(30),
                "a full-disk follower is elected (metadata progress may use the reserve)");
            await using DfsClient resolver = await ConnectAsync(cluster, "resolver", seeds: Names(followers));
            DfsVolume rv = resolver.GetVolume(Vol);
            DfsOperationOutcome resolved = await rv.Transactions.QueryOutcomeAsync(tx.Id, resolve: true, ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
            Assert.Equal("a0", await rv.Files.ReadAllTextAsync($"{dir}/a", ct: ct));
            await AssertBytesAsync(cOld, rv.Files.ReadAllBytesAsync($"{dir}/c", ct), "no partial publication under disk pressure");
            Log($"outcome resolved by {newLeader.Name}");

            await leader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, (await rv.Transactions.QueryOutcomeAsync(tx.Id, resolve: false, ct)).Status);
            foreach (TestNode follower in followers)
                Unfill(follower);
            await using (DfsTransaction retry = await vol.Transactions.BeginAsync(ct: ct))
            {
                await retry.Files.WriteAllTextAsync($"{dir}/a", "a2", ct: ct);
                await retry.CommitAsync(ct);
            }

            Assert.Equal("a2", await vol.Files.ReadAllTextAsync($"{dir}/a", ct: ct));
        }, ct);
    }

    private static void FillToReserve(TestCluster cluster, TestNode node) =>
        node.Disk.Faults.CapacityOverride = node.Disk.UsedBytes + cluster.Options.ReservedBytes;

    private static void Unfill(TestNode node) => node.Disk.Faults.CapacityOverride = null;

    private static async Task RunReadOnlyTransactionsAsync(TestCluster cluster, string name, string dir, LogicalClock clock,
        List<TxRecord> records, CancellationToken stop, ObserverGate? gate = null)
    {
        await using DfsClient reader = await ConnectAsync(cluster, name, operationTimeout: TimeSpan.FromSeconds(2));
        DfsVolume volume = reader.GetVolume(Vol);
        int n = 0;
        while (!stop.IsCancellationRequested)
        {
            if (gate is { Paused: true })
            {
                try
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), stop);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                continue;
            }

            long begin = clock.Tick();
            if (gate is not null)
                gate.Busy = true;
            try
            {
                await using DfsTransaction tx = await volume.Transactions.BeginAsync(ct: Ct);
                string a = await tx.Files.ReadAllTextAsync($"{dir}/a", ct: Ct);
                string b = await tx.Files.ReadAllTextAsync($"{dir}/b", ct: Ct);
                await tx.CommitAsync(Ct);
                records.Add(new TxRecord($"{name}-{n++}", begin, clock.Tick(), OpOutcome.Ok,
                    new Dictionary<string, string?>(StringComparer.Ordinal) { [$"{dir}/a"] = a, [$"{dir}/b"] = b },
                    new Dictionary<string, string?>(StringComparer.Ordinal), []));
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                // A failed/aborted read-only transaction does not constrain the history.
            }
            finally
            {
                if (gate is not null)
                    gate.Busy = false;
            }

            try
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(300), stop);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-008
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>The kind of damage done to one replica.</summary>
    public enum ReplicaDamage
    {
        /// <summary>Flipped payload bytes.</summary>
        CorruptPayload,

        /// <summary>Corrupted identity/length metadata.</summary>
        CorruptHeader,

        /// <summary>Silently missing object.</summary>
        Missing,
    }

    /// <summary>Each damage kind × whether a healthy copy stays reachable.</summary>
    public static TheoryData<ReplicaDamage, bool> Cha008Cases()
    {
        var data = new TheoryData<ReplicaDamage, bool>();
        foreach (ReplicaDamage damage in Enum.GetValues<ReplicaDamage>())
        {
            data.Add(damage, true);
            data.Add(damage, false);
        }

        return data;
    }

    /// <summary>
    /// One copy of every object of a file is damaged and another holder is partitioned away. With the third (healthy) copy
    /// reachable, reads return exactly the written bytes through verified failover; with it also cut off from the data
    /// plane, reads fail explicitly (integrity/unavailable), never returning damaged, zero-filled or truncated data and
    /// never moving the stream position. After healing, repair restores the damaged copy from a verified holder.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha008Cases))]
    [Catalog("CHA-008", "S", "INV-08", "INV-14")]
    public async Task CHA_008_CorruptReplicaPlusNetworkPartition(ReplicaDamage damage, bool healthyCopyReachable)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones), async cluster =>
        {
            const string path = "/cha008/f";
            byte[] bytes = TestData.Bytes(8001, MultiExtentLength);
            await using (DfsClient setup = await ConnectAsync(cluster, "setup"))
            {
                await setup.GetVolume(Vol).Directories.CreateDirectoryAsync("/cha008", ct);
                await setup.GetVolume(Vol).Files.WriteAllBytesAsync(path, bytes, ct);
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), ct);
            IReadOnlyList<ObjectId> objects = await ObjectsOfAsync(cluster, path);
            Assert.True(objects.Count >= 3, "precondition: the file spans several objects");
            foreach (ObjectId obj in objects)
                Assert.Equal(3, (await cluster.Inspect.GetObjectHoldersAsync(obj)).Count);

            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();
            TestNode partitioned = followers[0];
            TestNode damaged = followers[1];
            TestNode healthy = leader;
            foreach (ObjectId obj in objects)
            {
                switch (damage)
                {
                    case ReplicaDamage.CorruptPayload:
                        await damaged.Disk.CorruptObjectAsync(obj, offset: 17);
                        break;
                    case ReplicaDamage.CorruptHeader:
                        await damaged.Disk.CorruptObjectHeaderAsync(obj);
                        break;
                    default:
                        await damaged.Disk.DeleteObjectAsync(obj);
                        break;
                }
            }

            if (damage == ReplicaDamage.CorruptPayload)
            {
                // Also damage a byte inside the range read below, so that read cannot legitimately succeed from verified
                // undamaged units of the damaged copy.
                (ObjectId rangeObject, long rangeOffset) = await LocateAsync(cluster, path, 70_017);
                await damaged.Disk.CorruptObjectAsync(rangeObject, rangeOffset);
            }

            cluster.Network.Isolate(partitioned.Name);
            var dataPlaneCut = new List<MessageRule>();
            if (!healthyCopyReachable)
            {
                foreach (MessageType type in new[] { MessageType.GetObject, MessageType.ReplicateObject, MessageType.CopyObjects, MessageType.LocateObject })
                    dataPlaneCut.Add(cluster.Network.DropNext(new MessageFilter(To: healthy.Name, Type: type), count: int.MaxValue));
            }

            await using DfsClient reader = await ConnectAsync(cluster, "reader", operationTimeout: TimeSpan.FromSeconds(5));
            DfsVolume rv = reader.GetVolume(Vol);
            if (healthyCopyReachable)
            {
                await AssertBytesAsync(bytes, rv.Files.ReadAllBytesAsync(path, ct), "verified failover to the healthy reachable copy");
                await using DfsStream stream = await rv.Files.OpenReadAsync(path, ct);
                stream.Seek(70_000, SeekOrigin.Begin);
                byte[] window = new byte[1_000];
                await stream.ReadExactlyAsync(window, ct);
                AssertEx.BytesEqual(bytes.AsSpan(70_000, 1_000), window, "range read across the damaged replica");
            }
            else
            {
                Exception whole = await Assert.ThrowsAnyAsync<Exception>(() => rv.Files.ReadAllBytesAsync(path, ct));
                Assert.True(IsExplicitReadFailure(whole),
                    $"without a healthy reachable source the read must fail explicitly, got {whole}");
                await using DfsStream stream = await rv.Files.OpenReadAsync(path, ct);
                stream.Seek(70_000, SeekOrigin.Begin);
                byte[] buffer = new byte[1_000];
                Exception partial = await Assert.ThrowsAnyAsync<Exception>(async () =>
                {
                    int read = await stream.ReadAsync(buffer, ct);
                    Log($"range read unexpectedly returned {read} bytes without a healthy source");
                });
                Assert.True(IsExplicitReadFailure(partial), $"range read must fail explicitly, got {partial}");
                Assert.Equal(70_000, stream.Position);   // a failed read does not move the position
            }

            foreach (MessageRule rule in dataPlaneCut)
                rule.Cancel();
            cluster.Network.Heal();
            await AssertBytesAsync(bytes, rv.Files.ReadAllBytesAsync(path, ct), "after healing");

            await cluster.Admin.StartScrubAsync(damaged.Name, ct);
            await cluster.WaitUntilAsync(async () =>
            {
                foreach (ObjectId obj in objects)
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(obj);
                    if (holders.Count < 3 || !holders.Contains(damaged.Name))
                        return false;
                }

                return true;
            }, TimeSpan.FromSeconds(60), "repair restores the damaged copy from a verified holder", ct);
            foreach (ObjectId obj in objects)
                Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(obj));
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-009
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Each maintenance step at which a node crashes × crash persistence mode.</summary>
    public static TheoryData<string, CrashMode> Cha009Cases()
    {
        var data = new TheoryData<string, CrashMode>();
        foreach (string site in new[] { FaultSites.GcSweep, FaultSites.CompactionCopy, FaultSites.CompactionPublish, FaultSites.CompactionDelete })
        {
            data.Add(site, CrashMode.LoseUnflushed);
            data.Add(site, CrashMode.TearLastWrite);
        }

        return data;
    }

    /// <summary>
    /// Snapshot S1 is deleted while a reader still has it open, a version-pinned handle holds an overwritten revision, and
    /// unrelated garbage makes GC sweep and segment compaction do real work. A node crashes at a GC/compaction step and
    /// restarts. While the node is down, after restart and after further maintenance rounds, every still-pinned object
    /// keeps verified copies and reads return exactly the pinned bytes (never reclaimed, never remapped); once the last
    /// pin is released, the pinned-only objects are reclaimed.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha009Cases))]
    [Catalog("CHA-009", "S", "INV-01", "INV-09", "INV-14")]
    public async Task CHA_009_RestartDuringCompactionAndSnapshotDelete(string crashSite, CrashMode crashMode)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones), async cluster =>
        {
            const string dir = "/cha009";
            byte[] a1 = TestData.Bytes(9001, MultiExtentLength);
            byte[] a2 = TestData.Bytes(9002, MultiExtentLength);
            byte[] b1 = TestData.Bytes(9003, MultiExtentLength);
            byte[] b2 = TestData.Bytes(9004, MultiExtentLength);
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync(dir, ct);
            await vol.Files.WriteAllBytesAsync($"{dir}/a", a1, ct);
            await vol.Files.WriteAllBytesAsync($"{dir}/b", b1, ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), ct);
            var pinnedObjects = new HashSet<ObjectId>(await ObjectsOfAsync(cluster, $"{dir}/a"));
            pinnedObjects.UnionWith(await ObjectsOfAsync(cluster, $"{dir}/b"));

            await vol.Snapshots.CreateAsync("cha009-s1", ct: ct);
            DfsStream pinned = await vol.Files.OpenAsync($"{dir}/b", new DfsOpenOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite | FileShare.Delete,
                ReadView = DfsReadView.VersionPinned,
            }, ct);
            await vol.Files.WriteAllBytesAsync($"{dir}/a", a2, ct);
            await vol.Files.WriteAllBytesAsync($"{dir}/b", b2, ct);
            await vol.Snapshots.CreateAsync("cha009-s2", ct: ct);
            DfsSnapshotView view1 = await vol.Snapshots.OpenAsync("cha009-s1", ct);

            // A GC-sweep crash is armed before the snapshot deletion so the sweep interleaves with the deletion and with the
            // garbage created next (arming later could miss every sweep: GC may already have reclaimed everything).
            bool gcCrash = crashSite == FaultSites.GcSweep;
            FaultArm? crash = gcCrash ? cluster.Faults.Arm(new FaultMatch(crashSite), FaultAction.CrashNode(crashMode)) : null;
            await vol.Snapshots.DeleteAsync("cha009-s1", ct);   // retention reference gone; the open reader still pins S1

            var garbage = new HashSet<ObjectId>();
            for (int i = 0; i < 5; i++)
            {
                if (i > 0)
                    garbage.UnionWith(await ObjectsOfAsync(cluster, $"{dir}/g"));
                await vol.Files.WriteAllBytesAsync($"{dir}/g", TestData.Bytes(9100 + (ulong)i, MultiExtentLength), ct);
            }

            if (!gcCrash)
            {
                await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), ct);
                await WaitUntilReclaimedAsync(cluster, garbage, TimeSpan.FromSeconds(60), "GC deleted the unreferenced /g versions, leaving dead space to compact");
                crash = cluster.Faults.Arm(new FaultMatch(crashSite), FaultAction.CrashNode(crashMode));
                foreach (TestNode node in Running(cluster).ToArray())
                {
                    try
                    {
                        await node.CompactStorageAsync(ct);
                    }
                    catch (Exception ex) when (node.State != TestNodeState.Running && ex is not Xunit.Sdk.XunitException)
                    {
                        // The armed crash hit this node mid-pass: an interrupted pass faults instead of reporting completion
                        // (TestNode.CompactStorageAsync). The crash itself is asserted below.
                        Log($"compaction on {node.Name} interrupted by the armed crash: {ex.GetType().Name}");
                    }
                }
            }

            Assert.NotNull(crash);

            await crash.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
            cluster.Faults.AssertReached(crash);
            TestNode crashed = cluster.Node(crash.Contexts[0].Node);
            await cluster.WaitUntilAsync(() => Task.FromResult(crashed.State == TestNodeState.Crashed), TimeSpan.FromSeconds(30),
                $"{crashed.Name} crashed at {crashSite}", ct);

            async Task AssertPinnedHistoryIntactAsync(string when, int minimumCopies)
            {
                await AssertBytesAsync(a1, view1.Files.ReadAllBytesAsync($"{dir}/a", ct), $"{when}: deleted-but-open snapshot /a");
                await AssertBytesAsync(b1, view1.Files.ReadAllBytesAsync($"{dir}/b", ct), $"{when}: deleted-but-open snapshot /b");
                pinned.Seek(0, SeekOrigin.Begin);
                await AssertBytesAsync(b1, ReadToEndAsync(pinned), $"{when}: version-pinned handle");
                await using (DfsSnapshotView view2 = await vol.Snapshots.OpenAsync("cha009-s2", ct))
                {
                    await AssertBytesAsync(a2, view2.Files.ReadAllBytesAsync($"{dir}/a", ct), $"{when}: S2 /a");
                    await AssertBytesAsync(b2, view2.Files.ReadAllBytesAsync($"{dir}/b", ct), $"{when}: S2 /b");
                }

                await AssertBytesAsync(a2, vol.Files.ReadAllBytesAsync($"{dir}/a", ct), $"{when}: live /a");
                foreach (ObjectId obj in pinnedObjects)
                {
                    int copies = (await cluster.Inspect.GetObjectHoldersAsync(obj)).Count;
                    Assert.True(copies >= minimumCopies, $"{when}: pinned object {obj} has only {copies} verified copies");
                }
            }

            await AssertPinnedHistoryIntactAsync("node down mid-maintenance", minimumCopies: 2);
            await crashed.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
            await AssertPinnedHistoryIntactAsync("after restart", minimumCopies: 2);

            cluster.Faults.DisarmAll();
            foreach (TestNode node in Running(cluster))
                await node.CompactStorageAsync(ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), ct);
            await AssertPinnedHistoryIntactAsync("after further GC and compaction rounds", minimumCopies: 3);

            await view1.DisposeAsync();
            await pinned.DisposeAsync();
            await WaitUntilReclaimedAsync(cluster, pinnedObjects, TimeSpan.FromSeconds(60),
                "objects only the released reader pins protected are reclaimed once the last obligation ends");
            await AssertBytesAsync(a2, vol.Files.ReadAllBytesAsync($"{dir}/a", ct), "current state intact after reclamation");
            await AssertBytesAsync(b2, vol.Files.ReadAllBytesAsync($"{dir}/b", ct), "current state intact after reclamation");
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-010
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Where the reseed transfer is interrupted on the backup node.</summary>
    public static TheoryData<string> Cha010Cases() => new()
    {
        FaultSites.BackupObjectReceived,
        FaultSites.BackupBeforeClosureVerify,
        FaultSites.BackupBeforePointer,
    };

    /// <summary>
    /// The backup exceeds its pin budget while offline and must reseed; the reseed is interrupted by a backup crash and
    /// the volume data key is rotated before it resumes; new data is written under the new key. Every completed snapshot
    /// (pre-reseed, reseeded, post-rotation) survives a backup cold restart, contains no plaintext at rest, and restores
    /// byte-exactly onto clean hosts with every source node unreachable, i.e. its dependency closure, including key
    /// material, is self-contained.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha010Cases))]
    [Catalog("CHA-010", "S", "INV-10", "INV-12", "INV-13")]
    public async Task CHA_010_KeyRotationDuringBackupReseed(string interruptSite)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones with { BackupNodeCount = 1, EncryptAtRest = true }, 20), async cluster =>
        {
            TestBackupNode backup = cluster.BackupNodes[0];
            string target = backup.TargetName;
            await backup.RegisterAsync(Vol, new DfsBackupSchedule(), new DfsBackupBudgets(MaxPinAge: TimeSpan.FromSeconds(3), MaxPinnedBytes: 1L << 30));

            const string markerText = "CHA010-PLAINTEXT-MARKER;";
            byte[] marker = Encoding.ASCII.GetBytes(string.Concat(Enumerable.Repeat(markerText, 400)));
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/cha010", ct);
            var s1 = new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["/cha010/f1"] = TestData.Bytes(10_001, MultiExtentLength),
                ["/cha010/marker"] = marker,
            };
            foreach ((string path, byte[] bytes) in s1)
                await vol.Files.WriteAllBytesAsync(path, bytes, ct);
            await vol.Snapshots.CreateAsync("cha010-s1", ct: ct);
            await vol.Backups.RequestSyncAsync(target, "cha010-s1", ct);
            await vol.Backups.AwaitBackupSnapshotAsync(target, "cha010-s1", TimeSpan.FromSeconds(60), ct);
            Guid lineage1 = (await vol.Backups.GetStatusAsync(target, ct)).LineageId;

            await backup.StopAsync();
            var s2 = new Dictionary<string, byte[]>(s1, StringComparer.Ordinal)
            {
                ["/cha010/f1"] = TestData.Bytes(10_011, MultiExtentLength),
                ["/cha010/f2"] = TestData.Bytes(10_012, MultiExtentLength),
            };
            await vol.Files.WriteAllBytesAsync("/cha010/f1", s2["/cha010/f1"], ct);
            await vol.Files.WriteAllBytesAsync("/cha010/f2", s2["/cha010/f2"], ct);
            await vol.Snapshots.CreateAsync("cha010-s2", ct: ct);
            await cluster.WaitUntilAsync(async () => (await vol.Backups.GetStatusAsync(target, ct)).ReseedRequired,
                TimeSpan.FromSeconds(60), "the offline target exceeded its pin age budget and needs a reseed", ct);

            FaultArm interrupt = cluster.Faults.Arm(new FaultMatch(interruptSite, Node: backup.Name), FaultAction.CrashNode());
            await backup.RestartAsync();
            await vol.Backups.RequestSyncAsync(target, "cha010-s2", ct);
            await interrupt.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
            cluster.Faults.AssertReached(interrupt);
            await cluster.WaitUntilAsync(() => Task.FromResult(backup.State == TestNodeState.Crashed), TimeSpan.FromSeconds(30),
                "the backup crashed at the injected reseed point (the rotation happens while the reseed is interrupted)", ct);

            FaultArm rotation = cluster.Faults.Arm(new FaultMatch(FaultSites.KeyRotation), FaultAction.Delay(TimeSpan.FromMilliseconds(100)));
            await cluster.Admin.RotateDataKeyAsync(Vol, ct);
            cluster.Faults.AssertReached(rotation);
            var s3 = new Dictionary<string, byte[]>(s2, StringComparer.Ordinal) { ["/cha010/f3"] = TestData.Bytes(10_013, MultiExtentLength) };
            await vol.Files.WriteAllBytesAsync("/cha010/f3", s3["/cha010/f3"], ct);   // encrypted under the new key version
            await vol.Snapshots.CreateAsync("cha010-s3", ct: ct);

            await backup.RestartAsync();
            await vol.Backups.RequestSyncAsync(target, "cha010-s2", ct);
            await vol.Backups.AwaitBackupSnapshotAsync(target, "cha010-s2", TimeSpan.FromSeconds(120), ct);
            await vol.Backups.RequestSyncAsync(target, "cha010-s3", ct);
            await vol.Backups.AwaitBackupSnapshotAsync(target, "cha010-s3", TimeSpan.FromSeconds(120), ct);
            Assert.NotEqual(lineage1, (await vol.Backups.GetStatusAsync(target, ct)).LineageId);

            await backup.CrashAsync(CrashMode.LoseUnflushed);
            await backup.RestartAsync();
            IReadOnlyList<string> published = await backup.Inspect.GetPublishedSnapshotsAsync(Vol);
            foreach (string name in new[] { "cha010-s1", "cha010-s2", "cha010-s3" })
                Assert.Contains(name, published);

            byte[] needle = Encoding.ASCII.GetBytes(markerText);
            foreach (string file in backup.Disk.Files())
                Assert.True(backup.Disk.ReadRaw(file).AsSpan().IndexOf(needle) < 0, $"plaintext found at rest in backup file {file}");

            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RecoveryOptions(cluster), ct);
            foreach (TestNode node in cluster.Nodes)
                cluster.Network.Isolate(node.Name);   // the restore may not depend on any source service
            await using DfsClient rc = await recovery.ConnectAsync(new TestClientOptions { Name = "rc" }, ct);
            var expected = new (string Snapshot, Dictionary<string, byte[]> State)[] { ("cha010-s1", s1), ("cha010-s2", s2), ("cha010-s3", s3) };
            foreach ((string snapshot, Dictionary<string, byte[]> state) in expected)
            {
                string restoredName = "r-" + snapshot;
                await recovery.Admin.RestoreFromBackupAsync(new DfsRestoreFromBackupRequest([backup.Endpoint], backup.ReaderCredentials, Vol, snapshot, restoredName), ct);
                DfsVolume rv = rc.GetVolume(restoredName);
                foreach ((string path, byte[] bytes) in state)
                    await AssertBytesAsync(bytes, rv.Files.ReadAllBytesAsync(path, ct), $"{snapshot} restored {path}");
                foreach (string path in s3.Keys.Where(p => !state.ContainsKey(p)))
                    Assert.False(await rv.Files.ExistsAsync(path, ct), $"{snapshot} must not contain {path}");
            }

            cluster.Network.Heal();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-011
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Five nodes under continuous register load: one follower is stopped (old build), one node (the leader in one variant)
    /// is upgraded, the current leader fails, and the old-build node reconnects. Feature activation is refused while any
    /// member cannot interpret it; authority remains safe (election safety sampled throughout, linearizable history,
    /// converged replicas); after a complete rolling upgrade the feature activates, and an old build returning afterwards
    /// never leads or silently deactivates it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("CHA-011", "S", "INV-05", "INV-08", "INV-18")]
    public async Task CHA_011_RollingUpgradeDuringFailover(bool upgradeLeaderFirst)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.FiveNodes, 20), async cluster =>
        {
            const string register = "/cha011-reg";
            var leaders = new Dictionary<(string Group, ulong Term), string>();
            SafetyMonitor election = SafetyMonitor.Start(cluster, "cha011-election-safety", TimeSpan.FromMilliseconds(200),
                () => SampleLeadersAsync(cluster, leaders));
            using var stopLoad = new CancellationTokenSource();
            var loadProgress = new System.Runtime.CompilerServices.StrongBox<int>();
            Task<int> load = RunRegisterLoadAsync(cluster, "load", register, stopLoad.Token, loadProgress);

            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();
            TestNode oldBuild = followers[0];
            TestNode upgraded = upgradeLeaderFirst ? leader : followers[1];
            ulong oldIncarnation = oldBuild.Incarnation;
            await oldBuild.StopAsync();
            await upgraded.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Next);
            Assert.Equal(SoftwareVersionProfile.Next.Name, upgraded.SoftwareVersion.Name);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);

            // Unsupported/InvalidArgument may surface as NotSupportedException/ArgumentException (API.md A.2), hence DfsErrors.ToCode.
            Exception refused = await Assert.ThrowsAnyAsync<Exception>(() => cluster.Admin.SetFeatureAsync(SoftwareVersionProfile.NextFeature, true, ct));
            DfsErrorCode refusedCode = DfsErrors.ToCode(refused);
            Assert.True(refusedCode is DfsErrorCode.Unsupported or DfsErrorCode.InvalidArgument,
                $"activation before every member supports the feature must be rejected explicitly, got {refusedCode}: {refused}");
            Assert.DoesNotContain(SoftwareVersionProfile.NextFeature, (await cluster.Admin.GetClusterStatusAsync(ct)).ActiveFeatures);

            TestNode failing = await cluster.GetLeaderAsync(Vol, ct);
            SoftwareVersionProfile failingVersion = failing.SoftwareVersion;
            await failing.CrashAsync();
            TestNode successor = await cluster.GetLeaderAsync(Vol, ct);
            Assert.NotEqual(failing.Name, successor.Name);

            await oldBuild.RestartAsync();
            Assert.True(oldBuild.Incarnation > oldIncarnation, "a returning member re-establishes a new incarnation before being trusted");
            Assert.Equal(SoftwareVersionProfile.Current.Name, oldBuild.SoftwareVersion.Name);
            await failing.RestartAsSoftwareVersionAsync(failingVersion);
            await cluster.WaitForReplicationAsync(Vol, ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            Assert.DoesNotContain(SoftwareVersionProfile.NextFeature, (await cluster.Admin.GetClusterStatusAsync(ct)).ActiveFeatures);

            foreach (TestNode node in cluster.Nodes.Where(n => n.SoftwareVersion.Name != SoftwareVersionProfile.Next.Name).ToArray())
            {
                await node.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Next);
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
            }

            await cluster.Admin.SetFeatureAsync(SoftwareVersionProfile.NextFeature, true, ct);
            Assert.Contains(SoftwareVersionProfile.NextFeature, (await cluster.Admin.GetClusterStatusAsync(ct)).ActiveFeatures);

            TestNode currentLeader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode downgraded = cluster.Nodes.First(n => n.Name != currentLeader.Name);
            await downgraded.StopAsync();
            bool refusedStart = false;
            try
            {
                await downgraded.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Current);
            }
            catch (DfsException ex)
            {
                refusedStart = true;
                Log($"old build refused to start on activated state: {ex.ErrorCode}");
            }

            if (!refusedStart)
            {
                for (int i = 0; i < 10; i++)
                {
                    if (downgraded.State == TestNodeState.Running)
                    {
                        GroupInspection? g = await downgraded.Inspect.GetGroupAsync(Vol);
                        Assert.True(g is null || g.Role != RaftRole.Leader, "an old build must not lead a group whose state it cannot interpret");
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), ct);
                }
            }

            Assert.Contains(SoftwareVersionProfile.NextFeature, (await cluster.Admin.GetClusterStatusAsync(ct)).ActiveFeatures);

            // The evidence bound is a number of completed operations, not a duration: when the steps above finish quickly
            // (an old build refused at start) the load keeps running until it has done enough work (or stalls, and fails).
            await cluster.WaitUntilAsync(() => Task.FromResult(Volatile.Read(ref loadProgress.Value) >= 10 || load.IsCompleted), TimeSpan.FromSeconds(60),
                "the register load completes at least 10 operations", ct);

            // Cancel inline on the simulation thread: CancelAsync runs the token callbacks (cancelled Task.Delay waits, their
            // timers and continuations) on the thread pool, an uncontrolled dependency of the deterministic run (HAR-002).
            stopLoad.Cancel();
            int completed = await load;
            Assert.True(completed >= 10, $"the register load completed only {completed} operations");
            await election.StopAndAssertAsync(minSamples: 10);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            LinearizabilityChecker.Check(cluster.History.Operations.Where(o => o.Key == register).ToArray(), new RegisterModel()).AssertValid();

            if (downgraded.State is TestNodeState.Running or TestNodeState.Paused)
                await downgraded.StopAsync();   // a refused (crashed/stopped) start needs no stop before the restart
            await downgraded.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Next);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
        }, ct);
    }

    /// <summary>Sequential writes and reads of one register file, recorded in the cluster history; returns the number of Ok operations.</summary>
    private static async Task<int> RunRegisterLoadAsync(TestCluster cluster, string name, string path, CancellationToken stop,
        System.Runtime.CompilerServices.StrongBox<int>? progress = null)
    {
        await using DfsClient client = await ConnectAsync(cluster, name, operationTimeout: TimeSpan.FromSeconds(2));
        DfsFileClient files = client.GetVolume(Vol).Files;
        int ok = 0;
        for (int i = 0; !stop.IsCancellationRequested; i++)
        {
            byte[] value = TestData.Bytes(11_000 + (ulong)i, 32);
            try
            {
                await cluster.History.RecordAsync(name, "write", path, value, () => files.WriteAllBytesAsync(path, value, Ct));
                ok++;
                if (progress is not null)
                    Volatile.Write(ref progress.Value, ok);
                await cluster.History.RecordAsync(name, "read", path, null, () => TryReadAllAsync(files, path));
                ok++;
                if (progress is not null)
                    Volatile.Write(ref progress.Value, ok);
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                // Recorded as failed/unknown; unavailability during failover is legal.
            }

            try
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(150), stop);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return ok;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-012
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>How metadata recovery is in progress while the retry storm arrives.</summary>
    public enum MetadataRecovery
    {
        /// <summary>A lagging follower installs a metadata checkpoint.</summary>
        FollowerSnapshotInstall,

        /// <summary>All nodes crash and recover from their local checkpoints.</summary>
        ColdRestart,
    }

    /// <summary>Client counts × recovery kind.</summary>
    public static TheoryData<int, MetadataRecovery> Cha012Cases()
    {
        var data = new TheoryData<int, MetadataRecovery>();
        foreach (int clients in new[] { 8, 24 })
        {
            foreach (MetadataRecovery recovery in Enum.GetValues<MetadataRecovery>())
                data.Add(clients, recovery);
        }

        return data;
    }

    private const int StormAttempts = 5;

    /// <summary>
    /// Many clients each append one record under a saved operation id; half commit with the reply lost, half never arrive;
    /// all clients die. While metadata recovery is in progress (checkpoint install held open), every client process
    /// restarts and retries with its saved id at once. Each id takes effect exactly once (no duplicate, no loss),
    /// incompatible reuse is rejected, append requests stay within the SDK's retry bound and per-session outcome state
    /// stays within its configured bound.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha012Cases))]
    [Catalog("CHA-012", "S", "INV-06", "INV-16")]
    public async Task CHA_012_RetryStormDuringMetadataRecovery(int clientCount, MetadataRecovery recovery)
    {
        CancellationToken ct = Ct;

        // The saved ids must be retried inside their retained outcome window whatever the recovery takes: this scenario is
        // about deduplication and bounds under a storm, not about window expiry (CHA-001/CHA-004/MUT-013 cover expiry).
        TestClusterOptions options = Sim(TestClusterOptions.ThreeZones, 20) with
        {
            Timings = NodeTimings.Fast with { SessionTimeout = TimeSpan.FromSeconds(120), OutcomeRetention = TimeSpan.FromSeconds(120) },
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            const string log = "/cha012/log";
            const int recordLength = 64;
            static byte[] Record(int i)
            {
                byte[] record = new byte[recordLength];
                Encoding.ASCII.GetBytes($"rec{i:D5}:").CopyTo(record, 0);
                TestData.Bytes(12_000 + (ulong)i, recordLength - 9).CopyTo(record, 9);
                return record;
            }

            await using DfsClient setup = await ConnectAsync(cluster, "setup");
            DfsVolume sv = setup.GetVolume(Vol);
            await sv.Directories.CreateDirectoryAsync("/cha012", ct);
            await sv.Files.WriteAllBytesAsync(log, ReadOnlyMemory<byte>.Empty, ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode follower = cluster.Nodes.First(n => n.Name != leader.Name);
            if (recovery == MetadataRecovery.FollowerSnapshotInstall)
                await follower.StopAsync();
            await FillLogAsync(sv, "/cha012/fill", RaftSnapshotThreshold + 30);

            var ids = new DfsOperationId[clientCount];
            var historyIds = new long[clientCount];
            var originals = new List<DfsClient>();
            var networkNames = new string[clientCount];
            var rules = new List<MessageRule>();
            var attempts = new Task<CommitToken>[clientCount];
            for (int i = 0; i < clientCount; i++)
            {
                var options = new TestClientOptions { Name = $"orig{i}", OperationTimeout = TimeSpan.FromSeconds(3) };
                DfsClient original = await cluster.ConnectAsync(options, ct);
                originals.Add(original);
                networkNames[i] = options.NetworkName;
                ids[i] = await original.GetVolume(Vol).NewOperationIdAsync(ct);
                var filter = new MessageFilter(From: options.NetworkName, Type: MessageType.Append);
                rules.Add(i % 2 == 0 ? cluster.Network.DropReplies(filter, 10_000) : cluster.Network.DropNext(filter, 10_000));
            }

            // All appends start together, once every client is connected, so none reaches its deadline before all are sent.
            for (int i = 0; i < clientCount; i++)
            {
                historyIds[i] = cluster.History.Invoke($"orig{i}", "append", log, Record(i), ids[i]);
                attempts[i] = originals[i].GetVolume(Vol).Files.AppendAllBytesAsync(log, Record(i), new DfsMutationOptions { OperationId = ids[i] }, ct);
            }

            // Every client dies while its append is outstanding (transmitted; the reply lost or the request dropped): the
            // process never learns the outcome, so it never resolves (and so never fences) the saved id its restarted
            // incarnation retries below. Awaiting the attempts instead would let each SDK resolve its id after the deadline.
            for (int i = 0; i < clientCount; i++)
            {
                var transmitted = new MessageFilter(From: networkNames[i], Type: MessageType.Append, Direction: LinkDirection.Request);
                await cluster.WaitUntilAsync(() => Task.FromResult(cluster.Network.GetMessageAttempts(transmitted).Count > 0), TimeSpan.FromSeconds(30),
                    $"client orig{i} transmitted its append", ct);
            }

            foreach (DfsClient original in originals)
                cluster.KillClient(original);
            for (int i = 0; i < clientCount; i++)
            {
                if (attempts[i].IsCompletedSuccessfully)
                    cluster.History.Complete(historyIds[i], OpOutcome.Ok, attempts[i].Result);
                else if (attempts[i].IsCompleted)
                    cluster.History.Complete(historyIds[i], HistoryRecorder.Classify(attempts[i].Exception!.InnerException!), attempts[i].Exception);
                else
                    cluster.History.Complete(historyIds[i], OpOutcome.Unknown);
            }
            foreach (MessageRule rule in rules)
                rule.Cancel();

            FaultArm recoveryPoint;
            Task? heldRestart = null;
            if (recovery == MetadataRecovery.FollowerSnapshotInstall)
            {
                recoveryPoint = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftSnapshotInstall, Node: follower.Name), FaultAction.Block());
                await follower.RestartAsync();
            }
            else
            {
                foreach (TestNode node in cluster.Nodes)
                    await node.CrashAsync();
                recoveryPoint = cluster.Faults.Arm(new FaultMatch(FaultSites.RecoveryCheckpointInstall, Node: cluster[0].Name), FaultAction.Block());
                heldRestart = cluster[0].RestartAsync();
                foreach (TestNode node in cluster.Nodes.Skip(1))
                    await node.RestartAsync();
            }

            await recoveryPoint.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
            cluster.Faults.AssertReached(recoveryPoint);

            var appendRequests = new MessageFilter(Type: MessageType.Append, Direction: LinkDirection.Request);
            long before = cluster.Network.Log.CountMessages(appendRequests);
            Task<CommitToken>[] storm = Enumerable.Range(0, clientCount)
                .Select(i => RetryAppendAsync(cluster, $"storm{i}", log, Record(i), ids[i]))
                .ToArray();
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), ct);   // the storm is under way while recovery is held
            recoveryPoint.Release();
            CommitToken[] tokens = await Task.WhenAll(storm);

            // Incompatible reuse of a saved id is rejected (checked right after the storm, well inside the outcome window
            // of the dead clients' sessions); the content check below also proves it did not append.
            await using DfsClient check = await ConnectAsync(cluster, "check");
            DfsFileClient files = check.GetVolume(Vol).Files;
            await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(() => files.AppendAllBytesAsync(log, Record(clientCount + 1),
                new DfsMutationOptions { OperationId = ids[0] }, ct), DfsErrorCode.OperationIdMismatch);

            if (heldRestart is not null)
                await heldRestart;
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);

            long sent = cluster.Network.Log.CountMessages(appendRequests) - before;
            int sdkAttempts = cluster.CreateClientOptions(new TestClientOptions { Name = "bound-probe" }).MaxRetryAttempts + 1;
            long bound = (long)clientCount * sdkAttempts * StormAttempts;
            Log($"retry storm: {clientCount} clients sent {sent} append requests (bound {bound})");
            Assert.True(sent <= bound, $"retry amplification unbounded: {sent} append requests for {clientCount} retried operations");
            Assert.All(tokens, t => Assert.False(t.IsNone));

            for (int i = 0; i < clientCount; i++)
            {
                if (cluster.History.Get(historyIds[i]).Outcome == OpOutcome.Unknown)
                    cluster.History.Resolve(historyIds[i], OpOutcome.Ok, tokens[i]);
            }

            byte[] content = await files.ReadAllBytesAsync(log, ct);
            long readId = cluster.History.Invoke("check", "read", log, null);
            cluster.History.Complete(readId, OpOutcome.Ok, content);
            Assert.Equal(clientCount * recordLength, content.Length);
            var counts = new int[clientCount];
            for (int offset = 0; offset < content.Length; offset += recordLength)
            {
                int i = int.Parse(Encoding.ASCII.GetString(content, offset + 3, 5), System.Globalization.CultureInfo.InvariantCulture);
                Assert.InRange(i, 0, clientCount - 1);
                AssertEx.BytesEqual(Record(i), content.AsSpan(offset, recordLength), $"record {i}");
                counts[i]++;
            }

            Assert.All(counts, c => Assert.Equal(1, c));
            LinearizabilityChecker.Check(cluster.History.Operations.Where(o => o.Key == log).ToArray(), new AppendLogModel()).AssertValid();

            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            Assert.All(state.Sessions, s => Assert.True(s.RetainedOutcomes <= NodeTimings.Fast.MaxOutcomesPerSession,
                $"session {s.Id} retains {s.RetainedOutcomes} outcomes"));
            int liveSessions = state.Sessions.Count(s => !s.Expired);
            Assert.True(liveSessions <= (2 * clientCount * StormAttempts) + 8, $"{liveSessions} live sessions after the storm");
        }, ct);
    }

    private static async Task<CommitToken> RetryAppendAsync(TestCluster cluster, string name, string path, byte[] record, DfsOperationId id)
    {
        Exception? last = null;
        for (int attempt = 0; attempt < StormAttempts; attempt++)
        {
            try
            {
                await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = $"{name}-{attempt}", OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
                return await client.GetVolume(Vol).Files.AppendAllBytesAsync(path, record, new DfsMutationOptions { OperationId = id }, Ct);
            }
            catch (Exception ex) when (IsExplicitUnavailability(ex))
            {
                last = ex;
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            }
        }

        Assert.Fail($"retry with saved id {id} did not complete after {StormAttempts} application attempts: {last}");
        return CommitToken.None;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-013
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The old leader and writer A are cut off with all their traffic captured (old-term appends/heartbeats/votes, A's
    /// renewals and commit; A's staged chunks were uploaded before the cut, so its stale receipts travel inside the captured
    /// commit). The majority elects a new leader, A's lease expires, writer B commits two revisions.
    /// The captured traffic is then delivered in adversarial order and replayed again as delayed duplicates. Old votes,
    /// leases, receipts and outcomes never overwrite the new state: election safety and commit-index monotonicity hold
    /// throughout, A's commit is rejected, B's revision and generation stay authoritative on every replica.
    /// </summary>
    [Theory]
    [InlineData(3, false)]
    [InlineData(3, true)]
    [InlineData(5, false)]
    [InlineData(5, true)]
    [Catalog("CHA-013", "S", "INV-05", "INV-06", "INV-18")]
    public async Task CHA_013_PartitionHealWithDelayedOldMessages(int nodeCount, bool reverseOrder)
    {
        CancellationToken ct = Ct;
        TestClusterOptions options = nodeCount == 5 ? TestClusterOptions.FiveNodes : TestClusterOptions.ThreeZones;
        await ClusterTest.RunAsync(Sim(options), async cluster =>
        {
            const string path = "/cha013/f";
            byte[] r1 = TestData.Bytes(13_001, MultiExtentLength);
            byte[] r2 = TestData.Bytes(13_003, MultiExtentLength - 7_000);
            byte[] r3 = TestData.Bytes(13_004, MultiExtentLength - 9_000);

            TestNode oldLeader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] rest = cluster.Nodes.Where(n => n.Name != oldLeader.Name).ToArray();
            var aOptions = new TestClientOptions { Name = "wa", SeedNodes = [oldLeader.Name], OperationTimeout = TimeSpan.FromSeconds(3) };
            var bOptions = new TestClientOptions { Name = "wb", SeedNodes = Names(rest) };
            DfsClient a = await cluster.ConnectAsync(aOptions, ct);
            DfsVolume av = a.GetVolume(Vol);

            // Whole logical update units: a stream uploads complete chunks early (ARCHITECTURE §9.3) and keeps a partial tail
            // buffered until its publication, so only whole chunks make "uploaded, unpublished" true and leave the commit
            // itself as the stale message the cut captures (a buffered tail would have to be uploaded through the cut first,
            // and that upload, not the commit, would fail).
            byte[] aStaged = TestData.Bytes(13_002, 4 * a.Capabilities.ChunkSize);
            await av.Directories.CreateDirectoryAsync("/cha013", ct);
            DfsStream writerA = await av.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.Read }, ct);
            ulong generationA = writerA.WriterGeneration;
            await writerA.WriteAsync(r1, ct);
            CommitToken t1 = await writerA.CommitAsync(ct);
            writerA.Seek(0, SeekOrigin.Begin);
            await writerA.WriteAsync(aStaged, ct);   // uploaded, unpublished

            MessageHold fromOld = cluster.Network.Hold(new MessageFilter(From: oldLeader.Name));
            MessageHold toOld = cluster.Network.Hold(new MessageFilter(To: oldLeader.Name));
            MessageHold fromA = cluster.Network.Hold(new MessageFilter(From: aOptions.NetworkName));
            Task<CommitToken> staleCommit = writerA.CommitAsync(ct);

            var leaders = new Dictionary<(string Group, ulong Term), string>();
            var commitIndexes = new Dictionary<string, ulong>(StringComparer.Ordinal);
            SafetyMonitor monitor = SafetyMonitor.Start(cluster, "cha013-old-messages", TimeSpan.FromMilliseconds(250), async () =>
            {
                string? violation = await SampleLeadersAsync(cluster, leaders);
                if (violation is not null)
                    return violation;
                foreach (TestNode node in Running(cluster))
                {
                    GroupInspection? g = await node.Inspect.GetGroupAsync(Vol);
                    if (g is null)
                        continue;
                    if (commitIndexes.TryGetValue(node.Name, out ulong previous) && g.CommitIndex < previous)
                        return $"commit index on {node.Name} regressed from {previous} to {g.CommitIndex}";
                    commitIndexes[node.Name] = g.CommitIndex;
                    FileInspection? f = (await node.Inspect.GetVolumeStateAsync(Vol))?.GetFile(path);
                    if (f is not null && f.Length == aStaged.Length)
                        return $"{node.Name} applied the stale writer's publication";
                }

                return null;
            });

            TestNode newLeader = await WaitForLeaderAmongAsync(cluster, rest, Vol, oldLeader.Name, TimeSpan.FromSeconds(30),
                "the majority elects a new leader while the old one's traffic is captured");
            await cluster.DelayAsync(Lease * 4, ct);
            DfsClient b = await cluster.ConnectAsync(bOptions, ct);
            DfsVolume bv = b.GetVolume(Vol);
            DfsStream writerB = await OpenWriterEventuallyAsync(cluster, bv, path, TimeSpan.FromSeconds(30));
            ulong generationB = writerB.WriterGeneration;
            Assert.True(generationB > generationA);
            await writerB.WriteAsync(r2, ct);
            await writerB.SetLengthAsync(r2.Length, ct);
            CommitToken t2 = await writerB.CommitAsync(ct);
            writerB.Seek(0, SeekOrigin.Begin);
            await writerB.WriteAsync(r3, ct);
            await writerB.SetLengthAsync(r3.Length, ct);
            CommitToken t3 = await writerB.CommitAsync(ct);
            Assert.True(t2.CompareTo(t1) > 0 && t3.CompareTo(t2) > 0);
            Assert.True(fromOld.Count > 0, "old-term traffic from the deposed leader was captured");
            Assert.True(fromA.Count > 0, "the stale writer's renewals/uploads/commit were captured");
            Log($"captured: fromOld={fromOld.Count} toOld={toOld.Count} fromA={fromA.Count}; new leader {newLeader.Name}");

            CapturedMessage[] captured = [.. fromA.Captured, .. fromOld.Captured, .. toOld.Captured];
            Assert.Contains(captured, m => m.From == aOptions.NetworkName && m.Type == MessageType.CommitFile);   // the stale commit itself is delayed
            if (reverseOrder)
            {
                fromA.ReleaseAll(reverseOrder: true);
                fromOld.ReleaseAll(reverseOrder: true);
                toOld.ReleaseAll(reverseOrder: true);
            }
            else
            {
                toOld.ReleaseAll();
                fromOld.ReleaseAll();
                fromA.ReleaseAll();
            }

            fromA.Dispose();
            fromOld.Dispose();
            toOld.Dispose();
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), ct);
            int replayed = 0;
            foreach (CapturedMessage message in reverseOrder ? Enumerable.Reverse(captured) : captured)
            {
                await cluster.Network.ReplayAsync(message);
                replayed++;
            }

            Log($"replayed {replayed} delayed duplicates of old traffic");
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);

            Task finished = await Task.WhenAny(staleCommit, cluster.DelayAsync(TimeSpan.FromSeconds(30), ct));
            Assert.True(ReferenceEquals(finished, staleCommit), "the stale writer's commit must terminate once connectivity returns");
            Exception staleFailure = await Assert.ThrowsAnyAsync<Exception>(() => staleCommit);
            await AssertStalePublicationRejectedAsync(bv, staleFailure);
            Assert.True(writerA.IsFaulted);

            await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);
            await monitor.StopAndAssertAsync(minSamples: 5);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await AssertBytesAsync(r3, ReadSharedAsync(bv.Files, path), "B's latest commit stays authoritative");   // B is still open
            foreach (FileInspection? view in await ReplicaViewsAsync(cluster, path))
            {
                Assert.NotNull(view);
                Assert.Equal(generationB, view.WriterGeneration);
                Assert.Equal(r3.Length, view.Length);
            }

            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            Assert.DoesNotContain(state.Handles, h => h.IsWriter && !h.Expired && h.Generation == generationA);
            Assert.Contains(state.Handles, h => h.IsWriter && !h.Expired && h.Generation == generationB);

            await writerB.DisposeAsync();
            try
            {
                await writerA.DisposeAsync();
            }
            catch (Exception ex)
            {
                Log($"disposing the stale writer surfaced {ex.GetType().Name} (expected)");
            }

            await b.DisposeAsync();
            await a.DisposeAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-014
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Which obligation ends last.</summary>
    public enum LastObligation
    {
        /// <summary>Snapshot retention expires first; the reader pin ends last.</summary>
        ReaderPin,

        /// <summary>The reader pin ends first; snapshot retention ends last.</summary>
        Retention,
    }

    /// <summary>The kind of reader pin.</summary>
    public enum PinKind
    {
        /// <summary>An open snapshot view.</summary>
        SnapshotView,

        /// <summary>A version-pinned live read handle.</summary>
        VersionPinnedHandle,
    }

    /// <summary>
    /// The old revision of a file is protected only by a snapshot with expiring retention and by a reader pin, while one of
    /// its holders is lost and repair runs. Throughout repair and retention expiry the pinned bytes stay exact and at least
    /// one verified copy exists; repair re-protects pinned-only data; the data stays until the last valid obligation ends
    /// (whichever ends last) and is reclaimed afterwards.
    /// </summary>
    [Theory]
    [InlineData(LastObligation.ReaderPin, PinKind.SnapshotView)]
    [InlineData(LastObligation.ReaderPin, PinKind.VersionPinnedHandle)]
    [InlineData(LastObligation.Retention, PinKind.SnapshotView)]
    [InlineData(LastObligation.Retention, PinKind.VersionPinnedHandle)]
    [Catalog("CHA-014", "S", "INV-09", "INV-14", "INV-17")]
    public async Task CHA_014_PinnedReaderAndRetentionExpiryDuringRepair(LastObligation last, PinKind pinKind)
    {
        CancellationToken ct = Ct;
        TestClusterOptions options = Sim(new TestClusterOptions
        {
            Nodes = [new TestNodeSpec("n0", "z1"), new TestNodeSpec("n1", "z2"), new TestNodeSpec("n2", "z3"), new TestNodeSpec("n3", "z1")],
            ControlVoters = 3,
        });
        await ClusterTest.RunAsync(options, async cluster =>
        {
            const string path = "/cha014/f";
            byte[] f1 = TestData.Bytes(14_001, MultiExtentLength);
            byte[] f2 = TestData.Bytes(14_002, MultiExtentLength);
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/cha014", ct);
            await vol.Files.WriteAllBytesAsync(path, f1, ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), ct);
            IReadOnlyList<ObjectId> oldObjects = await ObjectsOfAsync(cluster, path);

            DfsStream? pinnedHandle = pinKind == PinKind.VersionPinnedHandle
                ? await vol.Files.OpenAsync(path, new DfsOpenOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.ReadWrite | FileShare.Delete,
                    ReadView = DfsReadView.VersionPinned,
                }, ct)
                : null;
            // Retention-last: long enough that repair completes and several checks run before the deadline.
            DateTimeOffset expires = cluster.Time.GetUtcNow() + (last == LastObligation.ReaderPin ? TimeSpan.FromSeconds(4) : TimeSpan.FromSeconds(60));
            await vol.Snapshots.CreateAsync("cha014-s", new DfsSnapshotOptions { Retention = new DfsRetention(expires) }, ct);
            DfsSnapshotView? view = pinKind == PinKind.SnapshotView ? await vol.Snapshots.OpenAsync("cha014-s", ct) : null;
            await vol.Files.WriteAllBytesAsync(path, f2, ct);   // the old objects are now protected only by the snapshot and the pin

            async Task<byte[]> ReadPinnedAsync()
            {
                if (view is not null)
                    return await view.Files.ReadAllBytesAsync(path, ct);
                pinnedHandle!.Seek(0, SeekOrigin.Begin);
                return await ReadToEndAsync(pinnedHandle);
            }

            async Task ReleasePinAsync()
            {
                if (view is not null)
                    await view.DisposeAsync();
                if (pinnedHandle is not null)
                    await pinnedHandle.DisposeAsync();
            }

            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(oldObjects[0]);
            TestNode victim = cluster.Nodes.First(n => n.Labels.Zone == "z1" && holders.Contains(n.Name));
            await victim.CrashAsync();
            await cluster.Admin.StartRepairAsync(Vol, ct);

            SafetyMonitor monitor = SafetyMonitor.Start(cluster, "cha014-pinned-data-safe", TimeSpan.FromMilliseconds(500), async () =>
            {
                byte[] seen = await ReadPinnedAsync();
                if (!seen.AsSpan().SequenceEqual(f1))
                    return "the pinned reader returned bytes that are not its pinned revision";
                foreach (ObjectId obj in oldObjects)
                {
                    if ((await cluster.Inspect.GetObjectHoldersAsync(obj)).Count == 0)
                        return $"pinned object {obj} has no verified copy";
                }

                return null;
            });

            if (last == LastObligation.ReaderPin)
            {
                await cluster.WaitUntilAsync(async () => !(await vol.Snapshots.ListAsync(ct)).Any(s => s.Name == "cha014-s"),
                    TimeSpan.FromSeconds(30), "the snapshot expires at its retention deadline", ct);
                await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), ct);
                await cluster.DelayAsync(TimeSpan.FromSeconds(3), ct);
                await monitor.StopAndAssertAsync(minSamples: 5);
                foreach (ObjectId obj in oldObjects)
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
                    Assert.True(copies >= 2 && zones >= 2, $"repair must keep pinned-only data at its policy: {obj} {copies}/{zones}");
                }

                await AssertBytesAsync(f1, ReadPinnedAsync(), "the reader pin outlives the snapshot's retention");
                await victim.RestartAsync();
                await ReleasePinAsync();
                await WaitUntilReclaimedAsync(cluster, oldObjects, TimeSpan.FromSeconds(60),
                    "pinned-only objects are reclaimed once the reader pin, the last obligation, ends");
            }
            else
            {
                await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), ct);
                await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);
                await monitor.StopAndAssertAsync(minSamples: 3);
                await victim.RestartAsync();
                await ReleasePinAsync();
                int checks = 0;
                while (cluster.Time.GetUtcNow() < expires - TimeSpan.FromSeconds(3))
                {
                    await using (DfsSnapshotView fresh = await vol.Snapshots.OpenAsync("cha014-s", ct))
                        await AssertBytesAsync(f1, fresh.Files.ReadAllBytesAsync(path, ct), "retention still protects the old revision");
                    foreach (ObjectId obj in oldObjects)
                        Assert.NotEmpty(await cluster.Inspect.GetObjectHoldersAsync(obj));
                    checks++;
                    await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);
                }

                Assert.True(checks >= 2, $"only {checks} checks ran before the retention deadline");
                await WaitUntilReclaimedAsync(cluster, oldObjects, TimeSpan.FromSeconds(60),
                    "objects are reclaimed once retention, the last obligation, expires");
                Assert.DoesNotContain(await vol.Snapshots.ListAsync(ct), s => s.Name == "cha014-s");
            }

            await AssertBytesAsync(f2, vol.Files.ReadAllBytesAsync(path, ct), "the current revision is unaffected");
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-015
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Every crash persistence mode for the simultaneous power loss.</summary>
    public static TheoryData<CrashMode> Cha015Cases() => new()
    {
        CrashMode.LoseUnflushed,
        CrashMode.KeepUnflushed,
        CrashMode.RandomSubset,
        CrashMode.TearLastWrite,
    };

    /// <summary>
    /// With staged writer data, a committed append and a committed transaction whose replies were lost, a transaction whose
    /// commit never arrived, and an append appended only to the leader's log, every node loses power at once and the client
    /// process dies. After the cold start: committed decisions are recovered, the never-delivered transaction resolves to
    /// aborted and a delayed copy of it cannot commit, the leader-local append resolves definitely and the content agrees,
    /// the dead session is fenced (its writer reservation is released by server expiry), staged objects survive until
    /// definitively abandoned and are reclaimed afterwards, and every committed object remains.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cha015Cases))]
    [Catalog("CHA-015", "S", "INV-01", "INV-02", "INV-05", "INV-06", "INV-09")]
    public async Task CHA_015_FullColdRestartWithUnknownTransactions(CrashMode crashMode)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones, 20), async cluster =>
        {
            const string dir = "/cha015";
            byte[] p1 = TestData.Bytes(15_002, 2_000);
            byte[] p2 = TestData.Bytes(15_004, 2_000);
            await using (DfsClient setup = await ConnectAsync(cluster, "setup"))
            {
                DfsVolume sv = setup.GetVolume(Vol);
                await sv.Directories.CreateDirectoryAsync(dir, ct);
                await sv.Files.WriteAllTextAsync($"{dir}/t1", "t1-old", ct: ct);
                await sv.Files.WriteAllTextAsync($"{dir}/t2", "t2-old", ct: ct);
                await sv.Files.WriteAllTextAsync($"{dir}/u1", "u1-old", ct: ct);
                await sv.Files.WriteAllBytesAsync($"{dir}/log", ReadOnlyMemory<byte>.Empty, ct);
                await sv.Files.WriteAllBytesAsync($"{dir}/log2", ReadOnlyMemory<byte>.Empty, ct);
                await sv.Files.WriteAllBytesAsync($"{dir}/s1", TestData.Bytes(15_000, MultiExtentLength), ct);
            }

            var oldOptions = new TestClientOptions { Name = "old", OperationTimeout = TimeSpan.FromSeconds(3) };
            DfsClient old = await cluster.ConnectAsync(oldOptions, ct);   // its process dies in the power loss
            DfsVolume ov = old.GetVolume(Vol);
            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);

            // (1) Staged, never-published writer data.
            HashSet<StagingId> beforeStaging = await OpenStagingSessionsAsync(cluster);
            DfsStream staged = await ov.Files.OpenAsync($"{dir}/s1", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read }, ct);
            ulong stagedGeneration = staged.WriterGeneration;
            await staged.WriteAsync(TestData.Bytes(15_001, 3 * MultiExtentLength), ct);
            var abandonedSessions = new HashSet<StagingId>(await OpenStagingSessionsAsync(cluster));
            abandonedSessions.ExceptWith(beforeStaging);
            Assert.NotEmpty(abandonedSessions);

            // (2) Append committed, every reply lost.
            DfsOperationId a1 = await ov.NewOperationIdAsync(ct);
            MessageRule lostAppend = cluster.Network.DropReplies(new MessageFilter(From: oldOptions.NetworkName, Type: MessageType.Append), 10_000);
            MessageRule lostQueries = cluster.Network.DropReplies(new MessageFilter(From: oldOptions.NetworkName, Type: MessageType.QueryOperation), 10_000);
            Exception a1Error = await Assert.ThrowsAnyAsync<Exception>(() => ov.Files.AppendAllBytesAsync($"{dir}/log", p1, new DfsMutationOptions { OperationId = a1 }, ct));
            Assert.Equal(OpOutcome.Unknown, HistoryRecorder.Classify(a1Error));

            // (3) Transaction committed, reply lost.
            DfsTransaction committedTx = await ov.Transactions.BeginAsync(ct: ct);
            await committedTx.Files.WriteAllTextAsync($"{dir}/t1", "t1-new", ct: ct);
            await committedTx.Files.WriteAllTextAsync($"{dir}/t2", "t2-new", ct: ct);
            MessageRule lostCommit = cluster.Network.DropReplies(new MessageFilter(From: oldOptions.NetworkName, Type: MessageType.TxCommit), 10_000);
            Exception committedError = await Assert.ThrowsAnyAsync<Exception>(() => committedTx.CommitAsync(ct));
            Assert.Equal(OpOutcome.Unknown, HistoryRecorder.Classify(committedError));
            lostCommit.Cancel();

            // (4) Transaction whose commit request never arrives (captured for a later delayed delivery).
            HashSet<StagingId> beforeTu = await OpenStagingSessionsAsync(cluster);
            DfsTransaction undeliveredTx = await ov.Transactions.BeginAsync(ct: ct);
            await undeliveredTx.Files.WriteAllBytesAsync($"{dir}/u1", TestData.Bytes(15_003, MultiExtentLength), ct);
            var undeliveredSessions = new HashSet<StagingId>(await OpenStagingSessionsAsync(cluster));
            undeliveredSessions.ExceptWith(beforeTu);
            MessageHold heldCommit = cluster.Network.Hold(new MessageFilter(From: oldOptions.NetworkName, Type: MessageType.TxCommit));
            Task<CommitToken> undeliveredCommit = undeliveredTx.CommitAsync(ct);
            await heldCommit.WaitForAsync(1, TimeSpan.FromSeconds(30));
            CapturedMessage[] undeliveredFrames = heldCommit.Captured.ToArray();

            // (5) Append present only in the leader's local log.
            DfsOperationId a2 = await ov.NewOperationIdAsync(ct);
            FaultArm localOnly = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: leader.Name, Volume: Vol, Operation: DfsOperationKind.Append), FaultAction.Block());
            Task<CommitToken> a2Task = ov.Files.AppendAllBytesAsync($"{dir}/log2", p2, new DfsMutationOptions { OperationId = a2 }, ct);
            await localOnly.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(localOnly);

            // Simultaneous power loss of every node; the client process dies with them.
            cluster.KillClient(old);
            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync(crashMode);
            heldCommit.DropAll();
            heldCommit.Dispose();
            localOnly.Release();
            cluster.Faults.DisarmAll();
            lostAppend.Cancel();
            lostQueries.Cancel();
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
            Log($"abandoned calls of the dead client: undelivered commit {undeliveredCommit.Status}, leader-local append {a2Task.Status}");

            // Possibly-needed staged data is not reclaimed by the restart or by time alone before resolution.
            HashSet<ObjectId> stagedObjects = await ObjectsOfStagingAsync(cluster, abandonedSessions);
            HashSet<ObjectId> undeliveredObjects = await ObjectsOfStagingAsync(cluster, undeliveredSessions);
            Assert.NotEmpty(stagedObjects);
            Assert.NotEmpty(undeliveredObjects);

            await using DfsClient fresh = await ConnectAsync(cluster, "fresh");
            DfsVolume fv = fresh.GetVolume(Vol);
            Assert.Equal(DfsOutcomeStatus.Committed, (await fv.QueryOperationAsync(a1, resolve: true, ct)).Status);
            Assert.Equal(DfsOutcomeStatus.Committed, (await fv.Transactions.QueryOutcomeAsync(committedTx.Id, resolve: true, ct)).Status);
            Assert.Equal(DfsOutcomeStatus.Aborted, (await fv.Transactions.QueryOutcomeAsync(undeliveredTx.Id, resolve: true, ct)).Status);
            DfsOperationOutcome a2Outcome = await fv.QueryOperationAsync(a2, resolve: true, ct);
            Assert.True(a2Outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolution must be definite, got {a2Outcome.Status}");

            async Task AssertRecoveredStateAsync(string when)
            {
                await AssertBytesAsync(p1, fv.Files.ReadAllBytesAsync($"{dir}/log", ct), $"{when}: committed append exactly once");
                Assert.Equal("t1-new", await fv.Files.ReadAllTextAsync($"{dir}/t1", ct: ct));
                Assert.Equal("t2-new", await fv.Files.ReadAllTextAsync($"{dir}/t2", ct: ct));
                Assert.Equal("u1-old", await fv.Files.ReadAllTextAsync($"{dir}/u1", ct: ct));
                byte[] expectedLog2 = a2Outcome.Status == DfsOutcomeStatus.Committed ? p2 : [];
                await AssertBytesAsync(expectedLog2, fv.Files.ReadAllBytesAsync($"{dir}/log2", ct), $"{when}: leader-local append agrees with its resolution");
            }

            await AssertRecoveredStateAsync("after cold start");

            // A delayed copy of the never-delivered commit is fenced by the authoritative abort.
            foreach (CapturedMessage frame in undeliveredFrames)
                Log($"delayed delivery of the undelivered commit answered {await cluster.Network.ReplayAsync(frame)}");
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, (await fv.Transactions.QueryOutcomeAsync(undeliveredTx.Id, resolve: false, ct)).Status);
            await AssertRecoveredStateAsync("after the delayed duplicate");

            // The dead client's session is fenced: its writer reservation is released by server-side expiry only.
            await using (DfsStream reopened = await OpenWriterEventuallyAsync(cluster, fv, $"{dir}/s1", SessionTimeout * 4))
                Assert.True(reopened.WriterGeneration > stagedGeneration);

            // The old session itself is expired by the server (not by time on any client), and is then fenced: an id from it
            // that was never used is rejected as expired rather than executed as a new operation.
            await cluster.WaitUntilAsync(async () =>
                    (await cluster.Inspect.GetVolumeStateAsync(Vol)).Sessions.All(s => s.Id != a1.Session || s.Expired),
                SessionTimeout * 4, "the dead client's session is expired by the server after the cold start", ct);
            var neverUsed = new DfsOperationId(a1.Session, Math.Max(a1.Sequence, a2.Sequence) + 1_000);
            await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(() => fv.Files.AppendAllBytesAsync($"{dir}/log", p2,
                new DfsMutationOptions { OperationId = neverUsed }, ct), DfsErrorCode.OperationExpired);
            await AssertRecoveredStateAsync("after an operation from the fenced session was attempted");

            // Only definitively abandoned data is reclaimed; every committed object remains.
            await WaitUntilReclaimedAsync(cluster, stagedObjects, TimeSpan.FromSeconds(60), "staged objects of the expired session are reclaimed");
            await WaitUntilReclaimedAsync(cluster, undeliveredObjects, TimeSpan.FromSeconds(60), "staged objects of the aborted transaction are reclaimed");
            IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            foreach (ObjectId reachable in (await cluster.Inspect.GetVolumeStateAsync(Vol)).ReachableObjects())
                Assert.Contains(reachable, stored);
            await AssertRecoveredStateAsync("after reclamation");
        }, ct);
    }

    private static async Task<HashSet<StagingId>> OpenStagingSessionsAsync(TestCluster cluster) =>
        (await cluster.Inspect.GetVolumeStateAsync(Vol)).Staging.Where(s => s.Open).Select(s => s.Id).ToHashSet();

    private static async Task<HashSet<ObjectId>> ObjectsOfStagingAsync(TestCluster cluster, IReadOnlySet<StagingId> sessions)
    {
        var objects = new HashSet<ObjectId>();
        foreach (TestNode node in Running(cluster))
        {
            foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Vol))
            {
                if (sessions.Contains(record.Staging))
                    objects.Add(record.Id);
            }
        }

        return objects;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // CHA-016
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>Where the explicit restore (promotion) creates the new authority.</summary>
    public enum RestoreTarget
    {
        /// <summary>A new volume in the source cluster.</summary>
        SameCluster,

        /// <summary>A new volume in an independent cluster on clean hosts, with the source isolated.</summary>
        RecoveryCluster,
    }

    /// <summary>
    /// The backup holds S1; S2's export is delayed in the network. No automatic promotion happens and the backup stays
    /// read-only; an explicit restore creates a new authority (new volume identity, incomparable tokens, restore over an
    /// existing identity refused). Old-authority tokens, operation ids and (cross-cluster) clients are rejected by the new
    /// authority. The old source then returns and its delayed exports arrive: lineages stay separate in both directions,
    /// S1 at the backup is unchanged, S2 is published only if complete, and neither side's writes leak into the other.
    /// </summary>
    [Theory]
    [InlineData(RestoreTarget.SameCluster)]
    [InlineData(RestoreTarget.RecoveryCluster)]
    [Catalog("CHA-016", "S", "INV-13", "INV-15", "INV-18", "INV-19")]
    public async Task CHA_016_BackupPromotionAndOldSourceReturn(RestoreTarget restoreTarget)
    {
        CancellationToken ct = Ct;
        await ClusterTest.RunAsync(Sim(TestClusterOptions.ThreeZones with { BackupNodeCount = 1 }, 20), async cluster =>
        {
            TestBackupNode backup = cluster.BackupNodes[0];
            string target = backup.TargetName;
            await backup.RegisterAsync(Vol, new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30));
            await using DfsClient src = await ConnectAsync(cluster, "src");
            DfsVolume sv = src.GetVolume(Vol);
            await sv.Directories.CreateDirectoryAsync("/cha016", ct);
            var s1 = new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["/cha016/f1"] = TestData.Bytes(16_001, MultiExtentLength),
                ["/cha016/f2"] = TestData.Bytes(16_002, 3_000),
            };
            foreach ((string path, byte[] bytes) in s1)
                await sv.Files.WriteAllBytesAsync(path, bytes, ct);
            await sv.Snapshots.CreateAsync("cha016-s1", ct: ct);
            await sv.Backups.RequestSyncAsync(target, "cha016-s1", ct);
            await sv.Backups.AwaitBackupSnapshotAsync(target, "cha016-s1", TimeSpan.FromSeconds(60), ct);

            var s2 = new Dictionary<string, byte[]>(s1, StringComparer.Ordinal)
            {
                ["/cha016/f1"] = TestData.Bytes(16_011, MultiExtentLength),
                ["/cha016/f3"] = TestData.Bytes(16_013, MultiExtentLength),
            };
            await sv.Files.WriteAllBytesAsync("/cha016/f1", s2["/cha016/f1"], ct);
            await sv.Files.WriteAllBytesAsync("/cha016/f3", s2["/cha016/f3"], ct);
            DfsOperationId sourceOp = await sv.NewOperationIdAsync(ct);
            await sv.Files.WriteAllBytesAsync("/cha016/op", TestData.Bytes(16_014, 100), new DfsMutationOptions { OperationId = sourceOp }, ct);
            s2["/cha016/op"] = TestData.Bytes(16_014, 100);
            await sv.Snapshots.CreateAsync("cha016-s2", ct: ct);
            DfsVolumeInfo sourceInfo = await sv.GetInfoAsync(ct);
            CommitToken sourceToken = sourceInfo.LastCommit;

            // Delay the S2 export (all export message kinds to the backup are captured).
            var exportHolds = new[] { MessageType.BackupBegin, MessageType.BackupInventory, MessageType.BackupPutObject, MessageType.BackupPutManifest, MessageType.BackupPublish }
                .Select(t => cluster.Network.Hold(new MessageFilter(To: backup.Name, Type: t)))
                .ToArray();
            await sv.Backups.RequestSyncAsync(target, "cha016-s2", ct);
            await cluster.WaitUntilAsync(() => Task.FromResult(exportHolds.Sum(h => h.Count) > 0), TimeSpan.FromSeconds(30),
                "the S2 export started and is being delayed", ct);

            TestCluster? recovery = null;
            try
            {
                DfsAdminClient restoreAdmin = cluster.Admin;
                if (restoreTarget == RestoreTarget.RecoveryCluster)
                {
                    foreach (TestNode node in cluster.Nodes)
                        cluster.Network.Isolate(node.Name);   // the source site is gone from the recovery site's view
                    recovery = await cluster.StartRecoveryClusterAsync(RecoveryOptions(cluster), ct);
                    restoreAdmin = recovery.Admin;
                    Assert.DoesNotContain("promoted", await restoreAdmin.ListVolumesAsync(ct));
                }

                // No automatic promotion: the backup remains a read-only late-sync host.
                await using (DfsClient reader = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "b-reader", Credentials = backup.ReaderCredentials }, ct))
                {
                    await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(() => reader.GetVolume(Vol).Files.WriteAllBytesAsync("/cha016/f1", new byte[] { 1, 2, 3 }, ct),
                        DfsErrorCode.ReadOnly);
                }

                DfsVolumeInfo promoted = await restoreAdmin.RestoreFromBackupAsync(
                    new DfsRestoreFromBackupRequest([backup.Endpoint], backup.ReaderCredentials, Vol, "cha016-s1", "promoted"), ct);
                Assert.Equal("promoted", promoted.Name);
                Assert.NotEqual(sourceInfo.Id, promoted.Id);
                Assert.False(promoted.LastCommit.IsComparableWith(sourceToken), "the new authority's tokens are not ordered against the old source's");
                Assert.Throws<ArgumentException>(() => promoted.LastCommit.CompareTo(sourceToken));
                await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(() => restoreAdmin.RestoreFromBackupAsync(
                    new DfsRestoreFromBackupRequest([backup.Endpoint], backup.ReaderCredentials, Vol, "cha016-s1", "promoted"), ct), DfsErrorCode.AlreadyExists);

                await using DfsClient pc = recovery is not null
                    ? await recovery.ConnectAsync(new TestClientOptions { Name = "pc" }, ct)
                    : await ConnectAsync(cluster, "pc");
                DfsVolume pv = pc.GetVolume("promoted");
                foreach ((string path, byte[] bytes) in s1)
                    await AssertBytesAsync(bytes, pv.Files.ReadAllBytesAsync(path, ct), $"promoted {path} equals S1");
                Assert.False(await pv.Files.ExistsAsync("/cha016/f3", ct));
                byte[] x = TestData.Bytes(16_020, 5_000);
                await pv.Files.WriteAllBytesAsync("/cha016/x", x, ct);

                // Old-authority identity cannot act on the new authority.
                // InvalidArgument surfaces as ArgumentException under the canonical mapping (API.md A.2), hence DfsErrors.ToCode.
                Exception boundary = await Assert.ThrowsAnyAsync<Exception>(() => pv.Snapshots.CreateAsync("cha016-old-boundary",
                    new DfsSnapshotOptions { MinimumCommit = sourceToken }, ct));
                DfsErrorCode boundaryCode = DfsErrors.ToCode(boundary);
                Assert.True(boundaryCode is DfsErrorCode.EpochMismatch or DfsErrorCode.ClusterMismatch or DfsErrorCode.CrossVolume or DfsErrorCode.InvalidArgument,
                    $"an old-authority commit token must be rejected as incomparable, got {boundaryCode}: {boundary}");
                Assert.DoesNotContain(await pv.Snapshots.ListAsync(ct), s => s.Name == "cha016-old-boundary");
                try
                {
                    DfsOperationOutcome foreign = await pv.QueryOperationAsync(sourceOp, resolve: false, ct);
                    Assert.NotEqual(DfsOutcomeStatus.Committed, foreign.Status);
                }
                catch (DfsException ex)
                {
                    Log($"old-authority operation id rejected by the new authority: {ex.ErrorCode}");
                }

                if (recovery is { } recoveryCluster)
                {
                    await Assert.ThrowsAnyAsync<DfsClusterIdentityException>(() => recoveryCluster.ConnectAsync(
                        new TestClientOptions { Name = "old-identity", ExpectedClusterId = cluster.ClusterId }, ct));
                }

                // The old source returns; its delayed exports arrive in adversarial order.
                cluster.Network.Heal();
                foreach (MessageHold hold in exportHolds)
                {
                    hold.ReleaseAll(reverseOrder: true);
                    hold.Dispose();
                }

                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
                byte[] laterSource = TestData.Bytes(16_030, 4_000);
                await sv.Files.WriteAllBytesAsync("/cha016/f2", laterSource, ct);   // the old source keeps its own identity
                await cluster.DelayAsync(TimeSpan.FromSeconds(10), ct);

                await AssertBytesAsync(s1["/cha016/f2"], pv.Files.ReadAllBytesAsync("/cha016/f2", ct), "old-source writes never reach the new authority");
                Assert.False(await pv.Files.ExistsAsync("/cha016/f3", ct));
                await AssertBytesAsync(x, pv.Files.ReadAllBytesAsync("/cha016/x", ct));
                Assert.False(await sv.Files.ExistsAsync("/cha016/x", ct), "new-authority writes never reach the old source");

                IReadOnlyList<string> published = await backup.Inspect.GetPublishedSnapshotsAsync(Vol);
                Assert.Contains("cha016-s1", published);
                await using (DfsClient reader = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "b-reader2", Credentials = backup.ReaderCredentials }, ct))
                {
                    await using (DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("cha016-s1", ct))
                    {
                        foreach ((string path, byte[] bytes) in s1)
                            await AssertBytesAsync(bytes, view.Files.ReadAllBytesAsync(path, ct), $"S1 at the backup is unchanged: {path}");
                        Assert.False(await view.Files.ExistsAsync("/cha016/x", ct));
                    }

                    if (published.Contains("cha016-s2"))
                    {
                        await using DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("cha016-s2", ct);
                        foreach ((string path, byte[] bytes) in s2)
                            await AssertBytesAsync(bytes, view.Files.ReadAllBytesAsync(path, ct), $"S2 published only when complete: {path}");
                        Assert.False(await view.Files.ExistsAsync("/cha016/x", ct), "the new authority's data never enters the old lineage");
                    }
                }

                DfsBackupStatus sourceStatus = await sv.Backups.GetStatusAsync(target, ct);
                if (sourceStatus.LatestPublished is { } latest)
                    Assert.True(latest.CoveredCommit.IsComparableWith(sourceToken), "the source's backup checkpoints stay in the source lineage");
            }
            finally
            {
                if (recovery is not null)
                    await recovery.DisposeAsync();
                cluster.Network.Heal();
            }
        }, ct);
    }
}
