using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Off.OffHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Off;

/// <summary>
/// OFF — offline nodes, partitions and restart scenarios in deterministic whole-cluster simulation
/// (FSD §§9, 13, 14.1; INV-01, INV-03, INV-05, INV-08, INV-11, INV-18). Expected availability is always computed from the
/// configured voter set and the acknowledgement policy, never from the number of reachable servers.
/// </summary>
public sealed class OffSimulationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ================================================================================================================
    // OFF-001 FollowerOfflineBeforeWrite
    // ================================================================================================================

    public static TheoryData<int, int, OfflineMode> FollowerOfflineCases => new()
    {
        { 3, 1, OfflineMode.Stop },
        { 3, 1, OfflineMode.Crash },
        { 5, 1, OfflineMode.Stop },
        { 5, 2, OfflineMode.Stop },
        { 5, 2, OfflineMode.Crash },
    };

    /// <summary>
    /// OFF-001: with followers offline (disks preserved) under a feasible quorum/ack policy, inline, multi-extent,
    /// append and stream commits succeed; the offline nodes are never counted as current replicas (holders, durable
    /// domains, replica deficits) and stay configured voters; once back they are repaired and converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(FollowerOfflineCases))]
    [Catalog("OFF-001", "S", "INV-01", "INV-11", "INV-08")]
    public Task OFF_001_FollowerOfflineBeforeWrite(int nodeCount, int offlineFollowers, OfflineMode mode) =>
        ClusterTest.RunAsync(Sim(nodeCount), async cluster =>
        {
            IReadOnlyList<string> voters = await GetVolumeVotersAsync(cluster);
            var offline = new List<TestNode>();
            for (int i = 0; i < offlineFollowers; i++)
                offline.Add(await GetFollowerAsync(cluster, Ct, i));
            foreach (TestNode node in offline)
                await TakeOfflineAsync(cluster, node, mode);
            var offlineNames = offline.Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
            Assert.True(HasConfiguredMajority(voters, offlineNames), "test precondition: a configured majority remains");

            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct);
            await using var own = client;
            await v.Directories.CreateDirectoryAsync("/o1", Ct);
            CommitToken small = await v.Files.WriteAllBytesAsync("/o1/small", V1, Ct);
            CommitToken big = await v.Files.WriteAllBytesAsync("/o1/big", Big1, Ct);
            CommitToken appended = await v.Files.AppendAllBytesAsync("/o1/big", V2, Ct);
            CommitToken streamed;
            await using (DfsStream s = await v.Files.OpenAsync("/o1/stream", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct))
            {
                await s.WriteAsync(Big2, Ct);
                streamed = await s.CommitAsync(Ct);
            }

            Assert.True(small.CompareTo(big) < 0 && big.CompareTo(appended) < 0 && appended.CompareTo(streamed) < 0);
            await ExpectContentAsync(Concat(Big1, V2), v.Files.ReadAllBytesAsync("/o1/big", Ct), "multi-extent + append");
            await ExpectContentAsync(Big2, v.Files.ReadAllBytesAsync("/o1/stream", Ct), "stream commit");

            // Offline nodes are not counted as current: never holders, never in durable domain counts.
            DfsStoragePolicy policy = await GetVolumePolicyAsync(v, Ct);
            foreach (string path in new[] { "/o1/big", "/o1/stream" })
            {
                foreach (ObjectId obj in await GetFileObjectsAsync(cluster, path))
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(obj);
                    Assert.DoesNotContain(holders, h => offlineNames.Contains(h));
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
                    Assert.True(copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck,
                        $"{obj}: {copies} copies / {zones} zones below the acknowledged policy");
                    Assert.True(copies <= nodeCount - offlineFollowers);
                }
            }

            // Still configured voters (never implicitly removed); reported unreachable rather than current.
            Assert.Equal(voters.OrderBy(x => x, StringComparer.Ordinal), (await GetVolumeVotersAsync(cluster)).OrderBy(x => x, StringComparer.Ordinal));
            // Reachability is learned from missed heartbeats, so wait for it (bounded) instead of sampling once.
            await cluster.WaitUntilAsync(async () =>
                (await cluster.Admin.ListNodesAsync(Ct)).Where(n => offlineNames.Contains(n.Name)).All(n => !n.IsReachable),
                Bounded, "the offline followers are reported unreachable", Ct);
            IReadOnlyList<DfsNodeInfo> nodes = await cluster.Admin.ListNodesAsync(Ct);
            Assert.Equal(offlineNames.Count, nodes.Count(n => offlineNames.Contains(n.Name)));
            foreach (DfsNodeInfo info in nodes.Where(n => offlineNames.Contains(n.Name)))
            {
                Assert.False(info.IsReachable, $"{info.Name} must not be reported reachable while offline");
                Assert.NotEqual(DfsNodeStatus.Removed, info.Status);
            }

            if (nodeCount >= 3)
            {
                DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
                if (policy.TargetCopies > nodeCount - offlineFollowers)
                    Assert.True(health.UnderReplicatedObjects > 0, "the deficit caused by the offline replica must be exposed");
            }

            // Return: catch-up and repair restore the configured copies; replicas converge.
            foreach (TestNode node in offline)
                await BringOnlineAsync(cluster, node, mode);
            await cluster.WaitForStableAsync(Bounded, Ct);
            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            Assert.Equal(0, (await cluster.Inspect.GetReplicaHealthAsync(Volume)).UnderReplicatedObjects);
            await ExpectContentAsync(Big2, v.Files.ReadAllBytesAsync("/o1/stream", Ct), "after return");
        }, Ct);

    /// <summary>
    /// OFF-001 / §9.1: durable flush with a temporarily offline replica. A, B, C one per zone; C offline with preserved
    /// disk. A multi-extent replacement with a length change cannot succeed while B's data flush is held; once released it
    /// succeeds, survives a hard stop of A and B (restart from durable images only), and C is reported stale until
    /// repaired. No intermediate revision is ever visible. With ack-three the absence of C prevents success.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [Catalog("OFF-001", "S", "INV-01", "INV-11")]
    public Task OFF_001_FollowerOfflineBeforeWrite_DurableFlushWithOfflineReplica(int ackCopies)
    {
        DfsStoragePolicy policy = new() { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = ackCopies, DurableZonesBeforeAck = ackCopies };
        return ClusterTest.RunAsync(Sim(3, policy: policy), async cluster =>
        {
            const string path = "/s91/f";
            TestNode a = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode b = await GetFollowerAsync(cluster, Ct, 0);
            TestNode c = await GetFollowerAsync(cluster, Ct, 1);
            await c.StopAsync();

            (DfsClient writer, DfsVolume v) = await ConnectAsync(cluster, "writer", Ct);
            (DfsClient reader, DfsVolume r) = await ConnectAsync(cluster, "reader", Ct);
            await using var own0 = writer;
            await using var own1 = reader;
            await v.Directories.CreateDirectoryAsync("/s91", Ct);
            byte[] initial = TestData.Bytes(0x91_0001, 100 * 1024);
            if (ackCopies == 3)
            {
                // The stronger policy cannot even publish the initial file while C is absent.
                DfsPlacementUnavailableException blocked = await Assert.ThrowsAsync<DfsPlacementUnavailableException>(
                    () => v.Files.WriteAllBytesAsync(path, initial, Ct));
                Assert.Equal(DfsErrorCode.PlacementUnavailable, blocked.ErrorCode);
                Assert.Equal(3, blocked.RequiredCopies);
                Assert.True(blocked.AchievedCopies < 3, $"achieved {blocked.AchievedCopies} copies with C offline");
                Assert.False(await r.Files.ExistsAsync(path, Ct));
                await c.RestartAsync();
                await cluster.WaitForStableAsync(Bounded, Ct);
                await v.Files.WriteAllBytesAsync(path, initial, Ct);
                await c.StopAsync();
            }
            else
            {
                await v.Files.WriteAllBytesAsync(path, initial, Ct);
            }

            ulong initialRevision = (await r.Files.GetInfoAsync(path, Ct)).Revision;
            byte[] replacement = TestData.Bytes(0x91_0002, 5 * (64 << 10));
            long newLength = replacement.Length - 4321;
            byte[] expected = replacement[..(int)newLength];
            var history = new HistoryRecorder();
            history.Complete(history.Invoke("writer", "write", path, initial), OpOutcome.Ok);

            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            if (ackCopies == 3)
            {
                // Variant: the stronger policy cannot publish the staged replacement while C is absent.
                DfsStream strict = await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
                OffAttempt<CommitToken> blocked = await TryAsync(async () =>
                {
                    await strict.WriteAsync(replacement, Ct);
                    await strict.SetLengthAsync(newLength, Ct);
                    return await strict.CommitAsync(new DfsMutationOptions { OperationId = k }, Ct);
                });
                Assert.False(blocked.Succeeded, "ack-three cannot succeed while C is absent");
                Assert.True(blocked.Error is DfsPlacementUnavailableException || blocked.Classification == OpOutcome.Unknown,
                    $"expected placement unavailability, got {blocked.Error}");
                DfsOperationOutcome resolved = await r.QueryOperationAsync(k, resolve: true, Ct);
                Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
                await ExpectContentAsync(initial, r.Files.ReadAllBytesAsync(path, Ct), "old revision remains authoritative");
                Assert.Equal(initialRevision, (await r.Files.GetInfoAsync(path, Ct)).Revision);
                await TryAsync(() => strict.AbortAsync(Ct));
                return;
            }

            DfsStream stream = await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
            // B's data flush is held from the start of staging: complete chunks may be uploaded while writing (memory
            // bound), so arming only before the final commit could miss every B flush and test nothing.
            FaultArm holdFlushOnB = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Node: b.Name, Count: int.MaxValue), FaultAction.Block());
            long writeOp = history.Invoke("writer", "write", path, expected, k);
            async Task<CommitToken> StageAndCommitAsync()
            {
                await stream.WriteAsync(replacement, Ct);
                await stream.SetLengthAsync(newLength, Ct);
                return await stream.CommitAsync(new DfsMutationOptions { OperationId = k }, Ct);
            }

            Task<CommitToken> commit = StageAndCommitAsync();

            // Success must not happen before B's copy is durable: the flush is held, readers still see the old revision.
            await holdFlushOnB.WaitTriggeredAsync(Bounded);
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            Assert.False(commit.IsCompleted, "FlushAsync/commit completed before the second durable copy existed");
            // The concurrent reader uses a compatible share mode (writer Share.Read, reader Share.ReadWrite), test plan §5.
            byte[] midRead = await history.RecordAsync("reader", "read", path, null, () => ReadSharedAsync(r, path, Ct));
            AssertEx.BytesEqual(initial, midRead, "no intermediate revision is exposed while the commit is pending");

            holdFlushOnB.Release();
            CommitToken token = await commit;
            history.Complete(writeOp, OpOutcome.Ok, token);
            cluster.Faults.AssertReached(holdFlushOnB);
            await stream.DisposeAsync();

            IReadOnlyList<ObjectId> objects = await GetFileObjectsAsync(cluster, path);
            foreach (ObjectId obj in objects)
                Assert.DoesNotContain(c.Name, await cluster.Inspect.GetObjectHoldersAsync(obj));

            // Hard-stop A and B; restart from durable images only.
            await a.CrashAsync(CrashMode.LoseUnflushed);
            await b.CrashAsync(CrashMode.LoseUnflushed);
            await a.RestartAsync();
            await b.RestartAsync();
            await cluster.WaitForStableAsync(Bounded, Ct);
            byte[] afterCrash = await history.RecordAsync("reader", "read", path, null, () => r.Files.ReadAllBytesAsync(path, Ct));
            AssertEx.BytesEqual(expected, afterCrash, "the acknowledged revision survives a hard stop of both acknowledging hosts");
            DfsOperationOutcome outcome = await r.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(token, outcome.Token);

            // C returns: it is stale (not a holder, deficit reported) until repaired.
            await c.RestartAsync();
            await cluster.WaitForStableAsync(Bounded, Ct);
            foreach (ObjectId obj in objects)
            {
                // C answering is not a copy: it may only be counted as a holder of objects it really stores (repair may be
                // copying concurrently, so only this direction is race-free).
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(obj);
                if (holders.Contains(c.Name))
                    Assert.True(await c.Inspect.HasObjectAsync(obj), $"{obj}: C is counted as a holder without storing the object");
            }

            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            foreach (ObjectId obj in objects)
                Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(obj));
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            byte[] final = await history.RecordAsync("reader", "read", path, null, () => r.Files.ReadAllBytesAsync(path, Ct));
            AssertEx.BytesEqual(expected, final, "after repair");
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid();
        }, Ct);
    }

    // ================================================================================================================
    // OFF-002 LeaderOfflineBeforeWrite
    // ================================================================================================================

    public static TheoryData<int, OfflineMode> LeaderOfflineCases
    {
        get
        {
            var data = new TheoryData<int, OfflineMode>();
            foreach (int nodes in new[] { 3, 5 })
            {
                foreach (OfflineMode mode in Enum.GetValues<OfflineMode>())
                    data.Add(nodes, mode);
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-002: the current metadata leader goes offline (crash, stop, pause, disconnect); an eligible majority elects a
    /// successor at a higher term, new operations resume, every acknowledged write is intact, and after the old leader
    /// returns there is never more than one leader per term and replicas converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(LeaderOfflineCases))]
    [Catalog("OFF-002", "S", "INV-01", "INV-03", "INV-18")]
    public Task OFF_002_LeaderOfflineBeforeWrite(int nodeCount, OfflineMode mode) =>
        ClusterTest.RunAsync(Sim(nodeCount), async cluster =>
        {
            // Generous deadline: failing over from a paused/disconnected (unresponsive) leader is required, not a schedule.
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ProgressOperation);
            await using var own = client;
            IReadOnlyList<(string Path, byte[] Bytes)> acknowledged = await WriteDataSetAsync(v, "/o2", Ct);
            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            CommitToken lastAck = await v.Files.AppendAllBytesAsync("/o2/small", V1, new DfsMutationOptions { OperationId = k }, Ct);

            TestNode oldLeader = await cluster.GetLeaderAsync(Volume, Ct);
            GroupInspection before = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            await TakeOfflineAsync(cluster, oldLeader, mode);

            TestNode successor = await WaitForNewLeaderAsync(cluster, oldLeader.Name, before.Term, Ct);
            Assert.NotEqual(oldLeader.Name, successor.Name);

            // Operations resume through the successor; nothing acknowledged was lost.
            CommitToken resumed = await v.Files.WriteAllBytesAsync("/o2/after-failover", V2, Ct);
            Assert.True(resumed.CompareTo(lastAck) > 0, "the successor continues the same history");
            var expected = acknowledged.Select(f => f.Path == "/o2/small" ? (f.Path, Concat(f.Bytes, V1)) : f).ToList();
            await AssertDataSetAsync(v, expected, "acknowledged before the leader went offline", Ct);
            DfsOperationOutcome outcome = await v.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(lastAck, outcome.Token);
            await cluster.Inspect.AssertNoSplitBrainAsync();

            await BringOnlineAsync(cluster, oldLeader, mode);
            await cluster.WaitForStableAsync(Bounded, Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            GroupInspection oldView = (await oldLeader.Inspect.GetGroupAsync(Volume))!;
            Assert.True(oldView.Term > before.Term, "the returning leader adopts the newer term");
            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);

            // A client that can only reach the old leader still sees the newest committed state.
            (DfsClient pinned, DfsVolume pv) = await ConnectAsync(cluster, "via-old", Ct, seeds: [oldLeader.Name]);
            await using var own2 = pinned;
            await ExpectContentAsync(V2, pv.Files.ReadAllBytesAsync("/o2/after-failover", Ct), "read through the former leader");
        }, Ct);

    // ================================================================================================================
    // OFF-003 DataNodeOfflineBeforeRead
    // ================================================================================================================

    public enum ReadPath
    {
        LiveReadAllBytes,
        LiveStreamOpenedBefore,
        PinnedStreamOpenedBefore,
        LiveStreamOpenedAfter,
    }

    public static TheoryData<int, OfflineMode, ReadPath> DataNodeOfflineCases
    {
        get
        {
            var data = new TheoryData<int, OfflineMode, ReadPath>();
            foreach (int nodes in new[] { 3, 5 })
            {
                foreach (OfflineMode mode in Enum.GetValues<OfflineMode>())
                {
                    foreach (ReadPath path in Enum.GetValues<ReadPath>())
                        data.Add(nodes, mode, path);
                }
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-003: the preferred (first rendezvous-placement) replica of the file's objects goes offline; reads return the
    /// verified bytes from another replica without changing the selected revision.
    /// </summary>
    [Theory]
    [MemberData(nameof(DataNodeOfflineCases))]
    [Catalog("OFF-003", "S", "INV-03", "INV-11", "INV-01")]
    public Task OFF_003_DataNodeOfflineBeforeRead(int nodeCount, OfflineMode mode, ReadPath readPath) =>
        ClusterTest.RunAsync(Sim(nodeCount), async cluster =>
        {
            const string path = "/o3/f";
            // Generous deadline: failing over from a paused/disconnected (unresponsive) replica is required, not a schedule.
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ProgressOperation);
            await using var own = client;
            await v.Directories.CreateDirectoryAsync("/o3", Ct);
            await v.Files.WriteAllBytesAsync(path, Big1, Ct);
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            DfsFileInfo info = await v.Files.GetInfoAsync(path, Ct);
            IReadOnlyList<ObjectId> objects = await GetFileObjectsAsync(cluster, path);

            // The preferred replica of the first extent is the first rendezvous target under the current placement epoch.
            DfsClusterStatus status = await cluster.Admin.GetClusterStatusAsync(Ct);
            DfsVolumeInfo volumeInfo = await v.GetInfoAsync(Ct);
            PlacementNode[] placementNodes = status.Nodes
                .Select(n => new PlacementNode(n.Id, n.Name, n.Labels, Eligible: true, FreeBytes: Math.Max(1, n.CapacityBytes - n.UsedBytes)))
                .ToArray();
            string preferred = Placement.SelectTargets(objects[0], status.PlacementEpoch, volumeInfo.DefaultPolicy, placementNodes)[0].Name;
            Assert.Contains(preferred, await cluster.Inspect.GetObjectHoldersAsync(objects[0]));
            TestNode victim = cluster.Node(preferred);

            DfsStream? before = readPath switch
            {
                ReadPath.LiveStreamOpenedBefore => await v.Files.OpenAsync(path, new DfsOpenOptions { Access = FileAccess.Read, Share = FileShare.ReadWrite }, Ct),
                ReadPath.PinnedStreamOpenedBefore => await v.Files.OpenAsync(path, new DfsOpenOptions { Access = FileAccess.Read, Share = FileShare.ReadWrite, ReadView = DfsReadView.VersionPinned }, Ct),
                _ => null,
            };

            (long Reads, long ChecksumFailures) otherReadsBefore = await SumCountersAsync(cluster.Nodes.Where(n => n.Name != victim.Name));
            await TakeOfflineAsync(cluster, victim, mode);

            byte[] read;
            ulong revisionUsed;
            switch (readPath)
            {
                case ReadPath.LiveReadAllBytes:
                    read = await v.Files.ReadAllBytesAsync(path, Ct);
                    revisionUsed = (await v.Files.GetInfoAsync(path, Ct)).Revision;
                    break;
                case ReadPath.LiveStreamOpenedAfter:
                {
                    await using DfsStream s = await v.Files.OpenReadAsync(path, Ct);
                    read = await ReadToEndAsync(s);
                    revisionUsed = (await s.GetInfoAsync(Ct)).Revision;
                    break;
                }

                default:
                    read = await ReadToEndAsync(before!);
                    Assert.Equal(info.Revision, before!.OpenRevision);
                    Assert.Equal(info.InodeId, before.InodeId);
                    revisionUsed = before.OpenRevision;
                    if (readPath == ReadPath.PinnedStreamOpenedBefore)
                        Assert.Equal(DfsReadView.VersionPinned, before.ReadView);
                    await before.DisposeAsync();
                    break;
            }

            AssertEx.BytesEqual(Big1, read, $"verified bytes from a surviving replica ({victim.Name} offline via {mode})");
            Assert.Equal(info.Revision, revisionUsed);
            (long Reads, long ChecksumFailures) otherReadsAfter = await SumCountersAsync(cluster.Nodes.Where(n => n.Name != victim.Name));
            Assert.True(otherReadsAfter.Reads > otherReadsBefore.Reads, "the bytes must have been served by another replica");
            Assert.Equal(otherReadsBefore.ChecksumFailures, otherReadsAfter.ChecksumFailures);

            await BringOnlineAsync(cluster, victim, mode);
            await cluster.WaitForStableAsync(Bounded, Ct);
            Assert.Equal(info.Revision, (await v.Files.GetInfoAsync(path, Ct)).Revision);
        }, Ct);

    private static async Task<(long Reads, long ChecksumFailures)> SumCountersAsync(IEnumerable<TestNode> nodes)
    {
        long reads = 0, failures = 0;
        foreach (TestNode node in nodes.Where(n => n.State == TestNodeState.Running))
        {
            NodeCounters c = await node.GetCountersAsync();
            reads += c.ObjectReads;
            failures += c.ChecksumFailures;
        }

        return (reads, failures);
    }

    private static async Task<byte[]> ReadToEndAsync(Stream stream)
    {
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, Ct);
        return ms.ToArray();
    }

    // ================================================================================================================
    // OFF-004 NodeOfflineDuringEveryMutationPhase
    // ================================================================================================================

    public enum MutationPhase
    {
        RequestAccepted,
        StagingRegistered,
        ObjectWrite,
        ObjectBeforeFlush,
        ObjectAfterFlushBeforeReceipt,
        ReceiptValidated,
        BeforePropose,
        AfterLocalAppend,
        AfterCommitBeforeApply,
        AfterApplyBeforeReply,
        ReplySend,
        AfterReplyReceived,
    }

    public enum PhaseRole
    {
        MetadataLeader,
        Follower,
    }

    public enum PhaseMutation
    {
        StreamCommitMultiExtentWithLengthChange,
        WriteAllMultiExtent,
        AppendMultiExtent,
    }

    public enum ChunkOccurrence
    {
        First = 1,
        Interior = 2,
        Last = 4,
    }

    private static bool IsDataPhase(MutationPhase phase) =>
        phase is MutationPhase.ObjectWrite or MutationPhase.ObjectBeforeFlush or MutationPhase.ObjectAfterFlushBeforeReceipt;

    /// <summary>
    /// Applicable (role, phase, chunk, mutation) cases. Followers persist and apply log entries too, so the follower is
    /// crashed after its local append and after commit before its own apply, besides the data stages and after the reply.
    /// Excluded with reason: request acceptance for the commit, staging registration, receipt validation, proposal,
    /// application-before-reply and reply transmission are leader responsibilities; chunk occurrences apply only to
    /// per-object data stages.
    /// </summary>
    public static TheoryData<PhaseRole, MutationPhase, ChunkOccurrence, PhaseMutation> MutationPhaseCases
    {
        get
        {
            var data = new TheoryData<PhaseRole, MutationPhase, ChunkOccurrence, PhaseMutation>();
            foreach (PhaseMutation mutation in Enum.GetValues<PhaseMutation>())
            {
                foreach (MutationPhase phase in Enum.GetValues<MutationPhase>())
                {
                    ChunkOccurrence[] occurrences = IsDataPhase(phase) ? Enum.GetValues<ChunkOccurrence>() : [ChunkOccurrence.First];
                    foreach (ChunkOccurrence occurrence in occurrences)
                    {
                        data.Add(PhaseRole.MetadataLeader, phase, occurrence, mutation);
                        if (IsDataPhase(phase) || phase is MutationPhase.AfterReplyReceived or MutationPhase.AfterLocalAppend or MutationPhase.AfterCommitBeforeApply)
                            data.Add(PhaseRole.Follower, phase, occurrence, mutation);
                    }
                }
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-004: crash each relevant node immediately before/after every persistence and publication stage (F04–F13 and
    /// after the reply) of multi-extent publications. During the outage and after recovery the outcome obeys the rules:
    /// success ⇒ committed and durable; definite failure ⇒ old state; unknown ⇒ resolves to exactly one of them; never a
    /// partial revision; committed objects regain full placement after repair.
    /// </summary>
    [Theory]
    [MemberData(nameof(MutationPhaseCases))]
    [Catalog("OFF-004", "S", "INV-01", "INV-02", "INV-08", "INV-11", "INV-18")]
    public Task OFF_004_NodeOfflineDuringEveryMutationPhase(PhaseRole role, MutationPhase phase, ChunkOccurrence occurrence, PhaseMutation mutation) =>
        ClusterTest.RunAsync(Sim(3), async cluster =>
        {
            const string path = "/o4/f";
            byte[] old = TestData.Bytes(0x0404_0001, 100 * 1024);
            byte[] fresh = TestData.Bytes(0x0404_0002, 4 * cluster.Options.Timings.ChunkSize);
            long streamLength = fresh.Length - 999;
            byte[] expectedNew = mutation switch
            {
                PhaseMutation.AppendMultiExtent => Concat(old, fresh),
                PhaseMutation.StreamCommitMultiExtentWithLengthChange => fresh[..(int)streamLength],
                _ => fresh,
            };
            DfsOperationKind kind = mutation switch
            {
                PhaseMutation.AppendMultiExtent => DfsOperationKind.Append,
                PhaseMutation.WriteAllMultiExtent => DfsOperationKind.WriteAll,
                _ => DfsOperationKind.CommitFile,
            };

            (DfsClient c0, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct);
            (DfsClient c1, DfsVolume r) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            await v.Directories.CreateDirectoryAsync("/o4", Ct);
            await v.Files.WriteAllBytesAsync(path, old, Ct);
            // Let the asynchronous third copies of the old revision land first, so per-node object-write occurrences
            // count the publication's own chunks rather than background repair of earlier objects.
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            var history = new HistoryRecorder();
            history.Complete(history.Invoke("c0", "write", path, old), OpOutcome.Ok);

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode target = role == PhaseRole.MetadataLeader ? leader : await GetFollowerAsync(cluster, Ct);
            FaultArm? crashArm = phase switch
            {
                MutationPhase.AfterReplyReceived => null,
                _ => cluster.Faults.Arm(PhaseMatch(phase, target.Name, kind, occurrence), FaultAction.CrashNode(CrashMode.LoseUnflushed)),
            };

            long op = history.Invoke("c0", "write", path, expectedNew, k);
            DfsStream? stream = null;
            OffAttempt<CommitToken> result = await TryAsync(async () =>
            {
                var withK = new DfsMutationOptions { OperationId = k };
                switch (mutation)
                {
                    case PhaseMutation.WriteAllMultiExtent:
                        return await v.Files.WriteAllBytesAsync(path, fresh, withK, Ct);
                    case PhaseMutation.AppendMultiExtent:
                        return await v.Files.AppendAllBytesAsync(path, fresh, withK, Ct);
                    default:
                        stream = await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
                        await stream.WriteAsync(fresh, Ct);
                        await stream.SetLengthAsync(streamLength, Ct);
                        return await stream.CommitAsync(withK, Ct);
                }
            });

            // The commit outcome is judged on its own; releasing the handle afterwards is not part of the publication.
            if (stream is not null)
                await TryAsync(() => stream.DisposeAsync().AsTask());

            if (crashArm is null)
            {
                Assert.True(result.Succeeded, $"undisturbed publication must succeed: {result}");
                await target.CrashAsync(CrashMode.LoseUnflushed);
            }
            else
            {
                // Data stages on a follower may be reached by the publication itself or by the asynchronous third copy.
                await crashArm.WaitTriggeredAsync(Bounded);
                cluster.Faults.AssertReached(crashArm);
            }

            Assert.Equal(TestNodeState.Crashed, target.State);

            // During the outage (a majority remains): resolve and check the legal outcome branch.
            DfsOperationOutcome during = await r.QueryOperationAsync(k, resolve: true, Ct);
            Assert.True(during.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolve:true must be definite, got {during.Status}");
            if (result.Succeeded)
            {
                Assert.Equal(DfsOutcomeStatus.Committed, during.Status);
                Assert.Equal(result.Value, during.Token);
                history.Complete(op, OpOutcome.Ok, result.Value);
            }
            else if (result.Classification == OpOutcome.Unknown)
            {
                history.Complete(op, OpOutcome.Unknown, result.Error);
                history.Resolve(op, during.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);
            }
            else
            {
                Assert.Equal(DfsOutcomeStatus.Aborted, during.Status);
                history.Complete(op, OpOutcome.Failed, result.Error);
            }

            byte[] expectedNow = during.Status == DfsOutcomeStatus.Committed ? expectedNew : old;
            // Reads use a writer-compatible share mode: the faulted stream's handle may still be held until it expires.
            OffAttempt<byte[]> duringRead = await TryAsync(() => history.RecordAsync("c1", "read", path, null, () => ReadSharedAsync(r, path, Ct)));
            if (duringRead.Succeeded)
                AssertEx.BytesEqual(expectedNow, duringRead.Value, "exactly one complete revision during the outage");
            else
                Assert.True(IsAvailabilityFailure(duringRead.Error!), $"a read during the outage may only fail honestly: {duringRead.Error}");

            // Recovery.
            await target.RestartAsync();
            await cluster.WaitForStableAsync(Bounded, Ct);
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            byte[] afterRecovery = await history.RecordAsync("c1", "read", path, null, () => ReadSharedAsync(r, path, Ct));
            AssertEx.BytesEqual(expectedNow, afterRecovery, "exactly one complete revision after recovery");
            DfsOperationOutcome after = await r.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(during.Status, after.Status);
            Assert.Equal(during.Token, after.Token);
            if (during.Status == DfsOutcomeStatus.Committed)
            {
                foreach (ObjectId obj in await GetFileObjectsAsync(cluster, path))
                    Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(obj));
            }

            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid();
        }, Ct);

    private static FaultMatch PhaseMatch(MutationPhase phase, string node, DfsOperationKind kind, ChunkOccurrence occurrence) => phase switch
    {
        MutationPhase.RequestAccepted => new FaultMatch(FaultSites.RpcRequestAccepted, Node: node, Operation: kind),
        MutationPhase.StagingRegistered => new FaultMatch(FaultSites.StagingRegistered, Node: node, Volume: Volume),
        MutationPhase.ObjectWrite => new FaultMatch(FaultSites.ObjectWrite, Node: node, Occurrence: (int)occurrence),
        MutationPhase.ObjectBeforeFlush => new FaultMatch(FaultSites.ObjectBeforeFlush, Node: node, Occurrence: (int)occurrence),
        MutationPhase.ObjectAfterFlushBeforeReceipt => new FaultMatch(FaultSites.ObjectAfterFlushBeforeReceipt, Node: node, Occurrence: (int)occurrence),
        MutationPhase.ReceiptValidated => new FaultMatch(FaultSites.CommitReceiptValidated, Node: node, Volume: Volume),
        MutationPhase.BeforePropose => new FaultMatch(FaultSites.RaftBeforePropose, Node: node, Operation: kind),
        MutationPhase.AfterLocalAppend => new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: node, Operation: kind),
        MutationPhase.AfterCommitBeforeApply => new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: node, Operation: kind),
        MutationPhase.AfterApplyBeforeReply => new FaultMatch(FaultSites.ApplyAfterBeforeReply, Node: node, Operation: kind),
        MutationPhase.ReplySend => new FaultMatch(FaultSites.RpcReplySend, Node: node, Operation: kind),
        _ => throw new ArgumentOutOfRangeException(nameof(phase), phase, null),
    };

    // ================================================================================================================
    // OFF-005 NodeOfflineDuringOpenAndLease
    // ================================================================================================================

    public enum LeasePhase
    {
        Grant,
        Renew,
        Release,
    }

    public enum LeaseOfflineRole
    {
        MetadataLeader,
        Follower,
        WriterClient,
    }

    public static TheoryData<LeasePhase, LeaseOfflineRole, bool> LeaseCases
    {
        get
        {
            var data = new TheoryData<LeasePhase, LeaseOfflineRole, bool>();
            foreach (LeasePhase phase in Enum.GetValues<LeasePhase>())
            {
                foreach (LeaseOfflineRole role in Enum.GetValues<LeaseOfflineRole>())
                {
                    data.Add(phase, role, false);
                    data.Add(phase, role, true);
                }
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-005: a node (leader/follower) or the writer client is disconnected while a writer lease is granted, renewed or
    /// released. At every observation there is at most one valid writer per inode, at most one of two contending writers
    /// can publish, and after safe recovery the reservation is not leaked: a new writer obtains it within a bounded time.
    /// </summary>
    [Theory]
    [MemberData(nameof(LeaseCases))]
    [Catalog("OFF-005", "S", "INV-05", "INV-18", "INV-08")]
    public Task OFF_005_NodeOfflineDuringOpenAndLease(LeasePhase phase, LeaseOfflineRole role, bool outageExceedsLease) =>
        ClusterTest.RunAsync(Sim(3), async cluster =>
        {
            const string path = "/o5/f";
            TimeSpan lease = cluster.Options.Timings.LeaseDuration;
            TimeSpan outage = outageExceedsLease ? lease + lease : lease / 4;
            (DfsClient c0, DfsVolume w) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            (DfsClient c1, DfsVolume contender) = await ConnectAsync(cluster, "c1", Ct, operationTimeout: ShortOperation);
            await using var own0 = c0;
            await using var own1 = c1;
            await w.Directories.CreateDirectoryAsync("/o5", Ct);
            await w.Files.WriteAllBytesAsync(path, V1, Ct);
            InodeId inode = (await w.Files.GetInfoAsync(path, Ct)).InodeId;
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            string offlineName = role switch
            {
                LeaseOfflineRole.MetadataLeader => leader.Name,
                LeaseOfflineRole.Follower => (await GetFollowerAsync(cluster, Ct)).Name,
                _ => ClientName("c0"),
            };
            var writerOpen = new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read };

            async Task AssertAtMostOneValidWriterAsync(string when)
            {
                OffAttempt<VolumeStateInspection> state = await TryAsync(() => cluster.Inspect.GetVolumeStateAsync(Volume));
                if (!state.Succeeded)
                    return;   // no reachable authority to inspect at this instant
                int writers = state.Value!.Handles.Count(h => h.Inode == inode && h.IsWriter && !h.Expired);
                Assert.True(writers <= 1, $"{when}: {writers} valid writable handles on one inode");
            }

            DfsStream? first = null;
            OffAttempt<DfsStream> firstOpen;
            switch (phase)
            {
                case LeasePhase.Grant:
                {
                    FaultArm disconnect = cluster.Faults.Arm(
                        new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: leader.Name, Operation: DfsOperationKind.Open),
                        FaultAction.Custom(_ =>
                        {
                            cluster.Network.Isolate(offlineName);
                            return Task.CompletedTask;
                        }));
                    firstOpen = await TryAsync(() => w.Files.OpenAsync(path, writerOpen, Ct));
                    cluster.Faults.AssertReached(disconnect);
                    break;
                }

                case LeasePhase.Renew:
                    firstOpen = await TryAsync(() => w.Files.OpenAsync(path, writerOpen, Ct));
                    Assert.True(firstOpen.Succeeded, $"the initial grant must succeed: {firstOpen}");
                    cluster.Network.Isolate(offlineName);
                    break;
                default:
                {
                    firstOpen = await TryAsync(() => w.Files.OpenAsync(path, writerOpen, Ct));
                    Assert.True(firstOpen.Succeeded, $"the initial grant must succeed: {firstOpen}");
                    await firstOpen.Value!.WriteAsync(V2, Ct);
                    await firstOpen.Value.CommitAsync(Ct);
                    FaultArm disconnect = cluster.Faults.Arm(
                        new FaultMatch(FaultSites.RpcRequestAccepted, Operation: DfsOperationKind.CloseHandle),
                        FaultAction.Custom(_ =>
                        {
                            cluster.Network.Isolate(offlineName);
                            return Task.CompletedTask;
                        }));
                    OffAttempt<bool> closed = await TryAsync(() => firstOpen.Value.DisposeAsync().AsTask());
                    cluster.Faults.AssertReached(disconnect);
                    Assert.True(closed.Succeeded || IsNoSuccessFailure(closed.Error!), $"close during disconnect: {closed}");
                    break;
                }
            }

            first = phase == LeasePhase.Release ? null : firstOpen.Value;
            await AssertAtMostOneValidWriterAsync("during the outage");
            await cluster.DelayAsync(outage, Ct);
            await AssertAtMostOneValidWriterAsync("after the outage window");

            // Contend for the writer while the first may still hold it.
            OffAttempt<DfsStream> second = await TryAsync(() => contender.Files.OpenAsync(path, writerOpen, Ct));
            await AssertAtMostOneValidWriterAsync("after the contending open");
            if (second.Succeeded && first is not null && phase == LeasePhase.Renew && !outageExceedsLease)
                Assert.Fail($"a second writer was granted while the first writer's lease ({lease}) could not have expired ({role} offline for {outage})");
            if (!second.Succeeded)
                Assert.True(second.Error is DfsSharingViolationException || IsAvailabilityFailure(second.Error!),
                    $"a contending open must fail with a typed sharing conflict or honest unavailability: {second.Error}");

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(Bounded, Ct);
            await AssertAtMostOneValidWriterAsync("after healing");

            // At most one of the two writers may publish once both are back.
            OffAttempt<CommitToken> firstCommit = first is null
                ? new OffAttempt<CommitToken>(default, new InvalidOperationException("no first writer"))
                : await TryAsync(async () =>
                {
                    await first.WriteAsync(TestData.Bytes(0x0505_0001, 333), Ct);
                    return await first.CommitAsync(Ct);
                });
            OffAttempt<CommitToken> secondCommit = !second.Succeeded
                ? new OffAttempt<CommitToken>(default, new InvalidOperationException("no second writer"))
                : await TryAsync(async () =>
                {
                    await second.Value!.WriteAsync(TestData.Bytes(0x0505_0002, 444), Ct);
                    return await second.Value.CommitAsync(Ct);
                });
            if (first is not null && second.Succeeded)
                Assert.False(firstCommit.Succeeded && secondCommit.Succeeded, "two writers both published: duplicate writable authority");
            if (first is not null && second.Succeeded && !firstCommit.Succeeded)
                Assert.True(first.IsFaulted || firstCommit.Error is DfsLeaseLostException || firstCommit.Classification == OpOutcome.Unknown,
                    $"the superseded writer must be fenced: {firstCommit.Error}");

            if (first is not null)
                await TryAsync(() => first.DisposeAsync().AsTask());
            if (second.Succeeded)
                await TryAsync(() => second.Value!.DisposeAsync().AsTask());

            // No permanently leaked reservation: a fresh writer gets the inode within a bounded time.
            (DfsClient c2, DfsVolume fresh) = await ConnectAsync(cluster, "c2", Ct);
            await using var own2 = c2;
            DfsStream? reacquired = null;
            await cluster.WaitUntilAsync(async () =>
            {
                OffAttempt<DfsStream> open = await TryAsync(() => fresh.Files.OpenAsync(path, writerOpen, Ct));
                reacquired = open.Value;
                return open.Succeeded;
            }, Bounded, "the writer reservation is released or safely expired after recovery", Ct);
            await AssertAtMostOneValidWriterAsync("after reacquisition");
            await reacquired!.DisposeAsync();
        }, Ct);

    // ================================================================================================================
    // OFF-006 OneWholeZoneOffline
    // ================================================================================================================

    public enum ZoneTopology
    {
        ThreeZonesOneEach,
        TwoPlusOne,
        FiveNodesTwoTwoOne,
        SixNodesTwoPerZone,
        SixNodesTwoZonesThreeEach,
    }

    private static TestClusterOptions ZoneOptions(ZoneTopology topology) => topology switch
    {
        ZoneTopology.ThreeZonesOneEach => Sim(3),
        ZoneTopology.TwoPlusOne => Sim(3) with { Topology = TestTopology.TwoPlusOne },
        ZoneTopology.FiveNodesTwoTwoOne => Custom(
            [new("n0", "z1"), new("n1", "z1"), new("n2", "z2"), new("n3", "z2"), new("n4", "z3")],
            new DfsMetadataPolicy { Voters = 5 }, controlVoters: 5),
        ZoneTopology.SixNodesTwoPerZone => Custom(
            [new("n0", "z1"), new("n1", "z1"), new("n2", "z2"), new("n3", "z2"), new("n4", "z3"), new("n5", "z3")],
            new DfsMetadataPolicy { Voters = 3 }),
        ZoneTopology.SixNodesTwoZonesThreeEach => Custom(
            [new("n0", "z1"), new("n1", "z1"), new("n2", "z1"), new("n3", "z2"), new("n4", "z2"), new("n5", "z2")],
            new DfsMetadataPolicy { Voters = 3 }),
        _ => throw new ArgumentOutOfRangeException(nameof(topology)),
    };

    public static TheoryData<ZoneTopology, string> ZoneCases
    {
        get
        {
            var data = new TheoryData<ZoneTopology, string>();
            foreach (ZoneTopology topology in Enum.GetValues<ZoneTopology>())
            {
                foreach (string zone in ZoneOptions(topology).GetNodeSpecs().Select(s => s.Zone).Distinct(StringComparer.Ordinal))
                    data.Add(topology, zone);
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-006: every role in one zone is stopped. Writes succeed exactly when the surviving configured volume voters form a
    /// majority and the surviving hosts/zones satisfy the acknowledgement policy (not when "enough servers" are up);
    /// otherwise they fail honestly (quorum vs placement distinguished); live reads require the metadata majority.
    /// </summary>
    [Theory]
    [MemberData(nameof(ZoneCases))]
    [Catalog("OFF-006", "S", "INV-11", "INV-08", "INV-01")]
    public Task OFF_006_OneWholeZoneOffline(ZoneTopology topology, string zone) =>
        ClusterTest.RunAsync(ZoneOptions(topology), async cluster =>
        {
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            await using var own = client;
            await v.Directories.CreateDirectoryAsync("/o6", Ct);
            await v.Files.WriteAllBytesAsync("/o6/f", Big1, Ct);
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);

            IReadOnlyList<string> volumeVoters = await GetVolumeVotersAsync(cluster);
            IReadOnlyList<string> controlVoters = (await cluster.Inspect.GetControlGroupAsync()).Voters;
            TestNode[] zoneNodes = cluster.Nodes.Where(n => n.Labels.Zone == zone).ToArray();
            var offline = zoneNodes.Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
            DfsStoragePolicy policy = await GetVolumePolicyAsync(v, Ct);
            bool metadata = HasConfiguredMajority(volumeVoters, offline);
            bool control = HasConfiguredMajority(controlVoters, offline);
            bool placement = CanMeetAckPolicy(policy, cluster.Nodes.Where(n => !offline.Contains(n.Name)).Select(n => n.Labels));
            int liveServers = cluster.Nodes.Count - zoneNodes.Length;

            // The id is reserved while the whole cluster is up (reservation may need the volume leader). The probing
            // client gets a generous deadline where progress is required (failover is required, a retry schedule is
            // not) and a short one where only an honest failure is expected.
            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            (DfsClient prober, DfsVolume pv) = await ConnectAsync(cluster, "c-zone", Ct,
                operationTimeout: metadata && placement ? ProgressOperation : ShortOperation);
            await using var ownProber = prober;

            foreach (TestNode node in zoneNodes)
                await node.StopAsync();

            OffAttempt<CommitToken> write = await TryAsync(() => pv.Files.WriteAllBytesAsync("/o6/f", Big2, new DfsMutationOptions { OperationId = k }, Ct));
            OffAttempt<byte[]> read = await TryAsync(() => pv.Files.ReadAllBytesAsync("/o6/f", Ct));

            string context = $"{topology}, zone {zone} offline ({liveServers} live servers): metadata={metadata}, placement={placement}, control={control}";
            if (metadata && placement && control)
            {
                Assert.True(write.Succeeded, $"{context}: write must succeed: {write}");
            }
            else if (metadata && placement)
            {
                // Control-group loss SHOULD not stop established operations; either outcome is legal but never a fabricated one.
                Assert.True(write.Succeeded || IsNoSuccessFailure(write.Error!), $"{context}: {write}");
            }
            else
            {
                Assert.False(write.Succeeded, $"{context}: write must not succeed");
                Assert.True(IsNoSuccessFailure(write.Error!), $"{context}: honest failure expected, got {write.Error}");
                if (metadata && !placement)
                    Assert.True(write.Error is DfsPlacementUnavailableException || write.Classification == OpOutcome.Unknown,
                        $"{context}: placement loss must be reported as placement unavailability, got {write.Error}");
                if (!metadata)
                    Assert.False(write.Error is DfsPlacementUnavailableException, $"{context}: quorum loss must not be reported as a placement problem");
            }

            if (metadata)
            {
                if (read.Succeeded)
                {
                    // One complete revision: the new one if the write succeeded, the old one if it definitely failed.
                    if (write.Succeeded)
                        AssertEx.BytesEqual(Big2, read.Value, context);
                    else if (write.Classification == OpOutcome.Failed)
                        AssertEx.BytesEqual(Big1, read.Value, context);
                    else
                        Assert.True(read.Value!.AsSpan().SequenceEqual(Big1) || read.Value.AsSpan().SequenceEqual(Big2), $"{context}: partial revision read");
                }
                else
                    Assert.True(!control && IsAvailabilityFailure(read.Error!), $"{context}: live read must be served by the surviving majority: {read.Error}");
            }
            else
            {
                Assert.False(read.Succeeded, $"{context}: no live read without a metadata majority");
                Assert.True(IsAvailabilityFailure(read.Error!), $"{context}: {read.Error}");
            }

            foreach (TestNode node in zoneNodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(Bounded, Ct);
            DfsOperationOutcome resolved = await v.QueryOperationAsync(k, resolve: true, Ct);
            if (write.Succeeded)
                Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
            else if (write.Classification != OpOutcome.Unknown)
                Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
            await ExpectContentAsync(resolved.Status == DfsOutcomeStatus.Committed ? Big2 : Big1, v.Files.ReadAllBytesAsync("/o6/f", Ct), "after the zone returns");
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);

    // ================================================================================================================
    // OFF-007 QuorumLostNoFalseSuccess — every host-offline subset of the compact 3- and 5-node models
    // ================================================================================================================

    public enum SubsetMode
    {
        Crash,
        Pause,
        Disconnect,
        Mixed,
    }

    public static TheoryData<int, int, SubsetMode> OfflineSubsets
    {
        get
        {
            var data = new TheoryData<int, int, SubsetMode>();
            foreach (int n in new[] { 3, 5 })
            {
                foreach (int mask in AllSubsets(n))
                {
                    if (mask == 0)
                    {
                        data.Add(n, mask, SubsetMode.Crash);
                        continue;
                    }

                    // Every incarnation (crash, pause, disconnect, mixed) for every nonempty subset of both models (test plan §5).
                    foreach (SubsetMode mode in Enum.GetValues<SubsetMode>())
                        data.Add(n, mask, mode);
                }
            }

            return data;
        }
    }

    private static OfflineMode ModeFor(SubsetMode mode, int nodeIndex, int mask) => mode switch
    {
        SubsetMode.Crash => OfflineMode.Crash,
        SubsetMode.Pause => OfflineMode.Pause,
        SubsetMode.Disconnect => OfflineMode.Disconnect,
        _ => (OfflineMode)new[] { (int)OfflineMode.Crash, (int)OfflineMode.Pause, (int)OfflineMode.Disconnect }[(nodeIndex + System.Numerics.BitOperations.PopCount((uint)mask)) % 3],
    };

    /// <summary>
    /// OFF-007: for every host-offline subset (none..all) of the 3- and 5-node models and each offline incarnation (crash,
    /// pause, disconnect, mixed), new live mutations succeed iff a majority of the configured voters and the ack policy's
    /// domains remain; otherwise every mutation fails or stays unknown, a buffered stream write is only private acceptance
    /// (its commit never succeeds), and after recovery unknowns resolve consistently with the visible state. The history
    /// (writes + reads) is linearizable.
    /// </summary>
    [Theory]
    [MemberData(nameof(OfflineSubsets))]
    [Catalog("OFF-007", "S", "INV-01", "INV-08", "INV-11", "INV-03")]
    public Task OFF_007_QuorumLostNoFalseSuccess(int nodeCount, int offlineMask, SubsetMode mode) =>
        ClusterTest.RunAsync(Sim(nodeCount), async cluster =>
        {
            const string path = "/o7/f";
            IReadOnlyList<string> voters = await GetVolumeVotersAsync(cluster);
            IReadOnlyList<int> members = Members(offlineMask, nodeCount);
            var offline = members.Select(i => cluster[i].Name).ToHashSet(StringComparer.Ordinal);
            DfsStoragePolicy policy;
            await using (DfsClient probe = await cluster.ConnectAsync(new TestClientOptions { Name = "policy-probe" }, Ct))
                policy = await GetVolumePolicyAsync(probe.GetVolume(Volume), Ct);
            bool expectProgress = HasConfiguredMajority(voters, offline)
                && CanMeetAckPolicy(policy, cluster.Nodes.Where(n => !offline.Contains(n.Name)).Select(n => n.Labels));

            // With progress expected, the client gets a bounded but generous deadline (failing over from a paused or
            // disconnected leader/storage target is required, a particular retry schedule is not); without it, a short
            // deadline keeps the honest-failure checks fast.
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: expectProgress ? ProgressOperation : ShortOperation);
            await using var own = client;
            await v.Directories.CreateDirectoryAsync("/o7", Ct);
            var history = new HistoryRecorder();
            await history.RecordAsync("c0", "write", path, V1, () => v.Files.WriteAllBytesAsync(path, V1, Ct));
            DfsStream stream = await v.Files.OpenAsync("/o7/s", new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);

            DfsOperationId kWrite = await v.NewOperationIdAsync(Ct);
            var withId = new DfsMutationOptions { OperationId = kWrite };
            var modes = members.ToDictionary(i => i, i => ModeFor(mode, i, offlineMask));
            foreach ((int index, OfflineMode m) in modes)
                await TakeOfflineAsync(cluster, cluster[index], m);
            string context = $"{nodeCount} nodes, offline {{{string.Join(",", offline.Order(StringComparer.Ordinal))}}} ({mode}), expect progress={expectProgress}";

            // Buffered acceptance is private and never reported as durable.
            await stream.WriteAsync(V2, Ct);

            // A multi-extent replacement, so that new objects exist whose holders can be checked.
            long writeOp = history.Invoke("c0", "write", path, Big2);
            OffAttempt<CommitToken> write = await TryAsync(() => v.Files.WriteAllBytesAsync(path, Big2, withId, Ct));
            OffAttempt<CommitToken> streamCommit = await TryAsync(() => stream.CommitAsync(Ct));
            OffAttempt<byte[]> read = await TryAsync(() => history.RecordAsync("c0", "read", path, null, () => v.Files.ReadAllBytesAsync(path, Ct)));

            if (expectProgress)
            {
                Assert.True(write.Succeeded, $"{context}: write must succeed: {write}");
                Assert.True(streamCommit.Succeeded, $"{context}: stream commit must succeed: {streamCommit}");
                history.Complete(writeOp, OpOutcome.Ok, write.Value);
                Assert.True(read.Succeeded, $"{context}: live read must succeed: {read}");
                foreach (ObjectId obj in await GetFileObjectsAsync(cluster, path))
                {
                    // Offline hosts are never counted as current holders of the new objects.
                    Assert.DoesNotContain(await cluster.Inspect.GetObjectHoldersAsync(obj), h => offline.Contains(h));
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
                    Assert.True(copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck, $"{context}: {copies}/{zones}");
                }
            }
            else
            {
                Assert.False(write.Succeeded, $"{context}: no write may succeed without configured quorum/placement");
                Assert.True(IsNoSuccessFailure(write.Error!), $"{context}: {write.Error}");
                Assert.False(streamCommit.Succeeded, $"{context}: buffered data must not be reported as a durable commit");
                Assert.True(IsNoSuccessFailure(streamCommit.Error!), $"{context}: {streamCommit.Error}");
                history.Complete(writeOp, write.Classification, write.Error);
                if (!HasConfiguredMajority(voters, offline))
                {
                    Assert.False(read.Succeeded, $"{context}: no live read without a configured majority");
                    Assert.True(IsAvailabilityFailure(read.Error!), $"{context}: {read.Error}");
                }
            }

            // Safety during the outage: never two leaders in one term.
            await cluster.Inspect.AssertNoSplitBrainAsync();

            // Recovery.
            foreach ((int index, OfflineMode m) in modes)
                await BringOnlineAsync(cluster, cluster[index], m);
            await cluster.WaitForStableAsync(Bounded, Ct);
            if (!write.Succeeded)
            {
                DfsOperationOutcome resolved = await v.QueryOperationAsync(kWrite, resolve: true, Ct);
                Assert.True(resolved.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"{context}: the write's outcome must resolve after recovery, got {resolved.Status}");
                if (write.Classification == OpOutcome.Unknown)
                    history.Resolve(writeOp, resolved.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);
                else
                    Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
            }

            await history.RecordAsync("c0", "read", path, null, () => v.Files.ReadAllBytesAsync(path, Ct));
            await TryAsync(() => stream.DisposeAsync().AsTask());
            await cluster.Inspect.AssertNoSplitBrainAsync();
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid();
        }, Ct);

    // ================================================================================================================
    // OFF-008 DataPolicyLostWithMetadataQuorum
    // ================================================================================================================

    public enum DataLoss
    {
        AckThreeOneHostOffline,
        TwoPlusOneMinorityZoneOffline,
        DataRoleFailsOnTwoNodes,
        AckThreeZonesOneZoneOfThreeOffline,
    }

    public static TheoryData<DataLoss, PhaseMutation> DataLossCases
    {
        get
        {
            var data = new TheoryData<DataLoss, PhaseMutation>();
            foreach (DataLoss loss in Enum.GetValues<DataLoss>())
            {
                foreach (PhaseMutation m in Enum.GetValues<PhaseMutation>())
                    data.Add(loss, m);
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-008: the metadata majority is healthy (metadata-only operations commit, the group reports quorum) but the
    /// required data-copy domains are lost: new data publication is blocked with a placement/capacity error (never a
    /// quorum error, never success), nothing partial is published, and publication resumes once the domains return.
    /// </summary>
    [Theory]
    [MemberData(nameof(DataLossCases))]
    [Catalog("OFF-008", "S", "INV-08", "INV-11", "INV-01")]
    public Task OFF_008_DataPolicyLostWithMetadataQuorum(DataLoss loss, PhaseMutation mutation)
    {
        var strict = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 };
        TestClusterOptions options = loss switch
        {
            DataLoss.AckThreeOneHostOffline => Sim(3, policy: strict),
            DataLoss.TwoPlusOneMinorityZoneOffline => Sim(3) with { Topology = TestTopology.TwoPlusOne },
            DataLoss.DataRoleFailsOnTwoNodes => Sim(3),
            _ => Custom([new("n0", "z1"), new("n1", "z1"), new("n2", "z2"), new("n3", "z2"), new("n4", "z3")],
                new DfsMetadataPolicy { Voters = 5 }, controlVoters: 5, policy: strict),
        };
        return ClusterTest.RunAsync(options, async cluster =>
        {
            const string path = "/o8/f";
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            await using var own = client;
            await v.Directories.CreateDirectoryAsync("/o8", Ct);
            await v.Files.WriteAllBytesAsync(path, Big1, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);

            var stopped = new List<TestNode>();
            var diskArms = new List<FaultArm>();
            switch (loss)
            {
                case DataLoss.AckThreeOneHostOffline:
                    stopped.Add(await GetFollowerAsync(cluster, Ct));
                    break;
                case DataLoss.TwoPlusOneMinorityZoneOffline:
                case DataLoss.AckThreeZonesOneZoneOfThreeOffline:
                {
                    string lonelyZone = cluster.Nodes.GroupBy(n => n.Labels.Zone).Single(g => g.Count() == 1).Key;
                    stopped.AddRange(cluster.Nodes.Where(n => n.Labels.Zone == lonelyZone));
                    break;
                }

                case DataLoss.DataRoleFailsOnTwoNodes:
                    foreach (TestNode node in cluster.Nodes.Where(n => n.Name != leader.Name))
                        diskArms.Add(cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectWrite, Node: node.Name, Count: int.MaxValue), FaultAction.DiskFull()));
                    break;
            }

            foreach (TestNode node in stopped)
                await node.StopAsync();

            // Consensus is healthy: metadata-only work commits and the group reports quorum.
            await v.Directories.CreateDirectoryAsync("/o8/metadata-only", Ct);
            DfsVolumeStatus volumeStatus = await cluster.Admin.GetVolumeStatusAsync(Volume, Ct);
            Assert.True(volumeStatus.Group.HasQuorum, "the metadata group keeps its configured majority");

            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            var withK = new DfsMutationOptions { OperationId = k };
            DfsStream? stream = null;
            OffAttempt<CommitToken> publish = await TryAsync(async () =>
            {
                switch (mutation)
                {
                    case PhaseMutation.WriteAllMultiExtent:
                        return await v.Files.WriteAllBytesAsync(path, Big2, withK, Ct);
                    case PhaseMutation.AppendMultiExtent:
                        return await v.Files.AppendAllBytesAsync(path, Big2, withK, Ct);
                    default:
                        stream = await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
                        await stream.WriteAsync(Big2, Ct);
                        return await stream.CommitAsync(withK, Ct);
                }
            });
            if (stream is not null)
            {
                await TryAsync(() => stream.AbortAsync(Ct));
                await TryAsync(() => stream.DisposeAsync().AsTask());
            }

            if (diskArms.Count > 0)
            {
                // At least one data-role failure was actually hit by the upload (every placement target is tried).
                FaultArm reached = Assert.Single(diskArms.Where(a => a.Triggered).Take(1));
                cluster.Faults.AssertReached(reached);
            }
            Assert.False(publish.Succeeded, $"{loss}: publication must be blocked while the data policy cannot be met");
            Assert.False(publish.Error is DfsQuorumUnavailableException, "placement loss must not be reported as quorum loss");
            Assert.True(publish.Error is DfsPlacementUnavailableException or DfsCapacityException,
                $"{loss}: expected a placement/capacity failure, got {publish.Error}");
            if (publish.Error is DfsPlacementUnavailableException placementError)
            {
                Assert.Equal(DfsErrorCode.PlacementUnavailable, placementError.ErrorCode);
                DfsStoragePolicy policy = await GetVolumePolicyAsync(v, Ct);
                Assert.Equal(policy.DurableCopiesBeforeAck, placementError.RequiredCopies);
                Assert.Equal(policy.DurableZonesBeforeAck, placementError.RequiredZones);
                Assert.True(placementError.AchievedCopies < placementError.RequiredCopies || placementError.AchievedZones < placementError.RequiredZones,
                    $"achieved {placementError.AchievedCopies}/{placementError.AchievedZones} must be below the requirement");
            }

            DfsOperationOutcome resolved = await v.QueryOperationAsync(k, resolve: true, Ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
            await ExpectContentAsync(Big1, v.Files.ReadAllBytesAsync(path, Ct), "no partial publication");

            // Restore the domains: the same publication now succeeds.
            cluster.Faults.DisarmAll();
            foreach (TestNode node in stopped)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(Bounded, Ct);
            CommitToken ok = await v.Files.WriteAllBytesAsync(path, Big2, Ct);
            Assert.False(ok.IsNone);
            await ExpectContentAsync(Big2, v.Files.ReadAllBytesAsync(path, Ct), "after the domains return");
        }, Ct);
    }

    // ================================================================================================================
    // OFF-009 MetadataQuorumLostWithAllPayloads
    // ================================================================================================================

    /// <summary>How the volume voters n1/n2 are lost in OFF-009.</summary>
    public enum MetadataLossMode
    {
        Crash,
        Pause,
        Disconnect,

        /// <summary>
        /// Only the links between the three volume voters are cut: every node process, including n1 and n2, stays up and
        /// reachable by clients and by the other storage nodes, so every payload copy remains available.
        /// </summary>
        VoterLinksCut,
    }

    private static OfflineMode AsOfflineMode(MetadataLossMode mode) => mode switch
    {
        MetadataLossMode.Crash => OfflineMode.Crash,
        MetadataLossMode.Pause => OfflineMode.Pause,
        MetadataLossMode.Disconnect => OfflineMode.Disconnect,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "not a whole-node outage"),
    };

    public static TheoryData<int, MetadataLossMode> MetadataLossCases
    {
        get
        {
            var data = new TheoryData<int, MetadataLossMode>();
            foreach (int nodes in new[] { 5, 7 })
            {
                foreach (MetadataLossMode mode in Enum.GetValues<MetadataLossMode>())
                    data.Add(nodes, mode);
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-009: volume voters n0..n2 (explicit) lose their majority while the data nodes stay healthy (in the
    /// voter-links-cut variant every storage node, holding every payload copy, stays up and reachable). No new live read
    /// or write is authorized from data availability alone (reads, existence, metadata, enumeration and writes fail
    /// honestly), storage nodes refuse direct object reads without a capability, and refused mutations resolve
    /// consistently once the majority returns.
    /// </summary>
    [Theory]
    [MemberData(nameof(MetadataLossCases))]
    [Catalog("OFF-009", "S", "INV-03", "INV-08", "INV-12")]
    public Task OFF_009_MetadataQuorumLostWithAllPayloads(int nodeCount, MetadataLossMode mode) =>
        ClusterTest.RunAsync(ExplicitVoters(nodeCount), async cluster =>
        {
            const string path = "/o9/f";
            string[] dataOnly = cluster.Nodes.Skip(3).Select(n => n.Name).ToArray();
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, seeds: dataOnly, operationTimeout: ShortOperation);
            await using var own = client;
            await v.Directories.CreateDirectoryAsync("/o9", Ct);
            await v.Files.WriteAllBytesAsync(path, Big1, Ct);
            await v.Files.WriteAllBytesAsync("/o9/other", V1, Ct);
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            IReadOnlyList<ObjectId> objects = await GetFileObjectsAsync(cluster, path);
            Assert.Equal(new[] { "n0", "n1", "n2" }, (await GetVolumeVotersAsync(cluster)).Order(StringComparer.Ordinal));

            DfsOperationId kWrite = await v.NewOperationIdAsync(Ct);
            DfsOperationId kAppend = await v.NewOperationIdAsync(Ct);
            DfsOperationId kMkdir = await v.NewOperationIdAsync(Ct);
            TestNode[] lost = mode == MetadataLossMode.VoterLinksCut ? [] : [cluster.Node("n1"), cluster.Node("n2")];
            if (mode == MetadataLossMode.VoterLinksCut)
            {
                cluster.Network.CutBoth("n0", "n1");
                cluster.Network.CutBoth("n0", "n2");
                cluster.Network.CutBoth("n1", "n2");
            }
            else
            {
                foreach (TestNode node in lost)
                    await TakeOfflineAsync(cluster, node, AsOfflineMode(mode));
            }

            // Every payload is still stored on healthy nodes ...
            foreach (ObjectId obj in objects)
            {
                int healthyHolders = 0;
                foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running && !lost.Contains(n)))
                    healthyHolders += await node.Inspect.HasObjectAsync(obj) ? 1 : 0;
                Assert.True(healthyHolders >= 1, $"{obj} must remain stored on a healthy node for this scenario");
                if (mode == MetadataLossMode.VoterLinksCut)
                    Assert.True(healthyHolders >= 3, $"{obj}: every repaired copy stays on a running, client-reachable node ({healthyHolders})");
            }

            // ... but nothing live is authorized without the metadata majority. Mutations carry ids (reserved while the
            // majority was available) so that an uncertain failure, which may legally commit once the majority returns,
            // is resolved instead of assumed to be absent.
            var probes = new (string Name, DfsOperationId? Id, Func<Task> Probe)[]
            {
                ("ReadAllBytes", null, () => v.Files.ReadAllBytesAsync(path, Ct)),
                ("OpenRead", null, async () =>
                {
                    await using DfsStream s = await v.Files.OpenReadAsync(path, Ct);
                    await ReadToEndAsync(s);
                }),
                ("Exists", null, async () => Assert.Fail($"Exists returned {await v.Files.ExistsAsync(path, Ct)} without authority")),
                ("GetInfo", null, () => v.Files.GetInfoAsync(path, Ct)),
                ("Enumerate", null, async () => await v.Directories.EnumerateAsync("/o9", null, Ct).ToListAsync(Ct)),
                ("WriteAll", kWrite, () => v.Files.WriteAllBytesAsync(path, Big2, new DfsMutationOptions { OperationId = kWrite }, Ct)),
                ("Append", kAppend, () => v.Files.AppendAllBytesAsync("/o9/other", V2, new DfsMutationOptions { OperationId = kAppend }, Ct)),
                ("CreateDirectory", kMkdir, () => v.Directories.CreateDirectoryAsync("/o9/new", new DfsMutationOptions { OperationId = kMkdir }, Ct)),
            };
            var mutationClass = new Dictionary<DfsOperationId, OpOutcome>();
            foreach ((string name, DfsOperationId? id, Func<Task> probe) in probes)
            {
                OffAttempt<bool> attempt = await TryAsync(probe);
                Assert.False(attempt.Succeeded, $"{name} succeeded without a metadata majority");
                Assert.True(IsNoSuccessFailure(attempt.Error!), $"{name}: expected honest unavailability, got {attempt.Error}");
                Assert.False(attempt.Error is FileNotFoundException or DirectoryNotFoundException, $"{name}: unavailability must not be reported as absence");
                if (id is not null)
                    mutationClass[id.Value] = attempt.Classification;
            }

            // Storage nodes do not serve payloads on object identity alone.
            await using (RawProtocolClient raw = await cluster.ConnectRawAsync(dataOnly[0], Ct))
            {
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
                DfsErrorCode direct = await raw.GetObjectDirectAsync(Volume, objects[0]);
                Assert.Equal(DfsErrorCode.AccessDenied, direct);
            }

            if (mode == MetadataLossMode.VoterLinksCut)
                cluster.Network.Heal();
            foreach (TestNode node in lost)
                await BringOnlineAsync(cluster, node, AsOfflineMode(mode));
            await cluster.WaitForStableAsync(Bounded, Ct);

            // Each refused mutation resolves definitely; a definite failure must resolve as aborted, and the visible state
            // matches the resolution exactly (no partial or invented effect, no silent loss of a committed one).
            async Task<bool> ResolveCommittedAsync(DfsOperationId id)
            {
                DfsOperationOutcome outcome = await v.QueryOperationAsync(id, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"{id}: {outcome.Status}");
                if (mutationClass[id] == OpOutcome.Failed)
                    Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
                return outcome.Status == DfsOutcomeStatus.Committed;
            }

            bool writeCommitted = await ResolveCommittedAsync(kWrite);
            bool appendCommitted = await ResolveCommittedAsync(kAppend);
            bool mkdirCommitted = await ResolveCommittedAsync(kMkdir);
            await ExpectContentAsync(writeCommitted ? Big2 : Big1, v.Files.ReadAllBytesAsync(path, Ct), "after the metadata majority returns");
            await ExpectContentAsync(appendCommitted ? Concat(V1, V2) : V1, v.Files.ReadAllBytesAsync("/o9/other", Ct), "no write leaked through");
            Assert.Equal(mkdirCommitted, await v.Directories.ExistsAsync("/o9/new", Ct));
        }, Ct);

    // ================================================================================================================
    // OFF-010 AllLiveNodesTemporarilyOffline
    // ================================================================================================================

    public static TheoryData<OfflineMode> AllOfflineModes => new(OfflineMode.Crash, OfflineMode.Stop, OfflineMode.Pause, OfflineMode.Disconnect);

    /// <summary>
    /// OFF-010: every live node is taken offline with disks preserved. Live clients report unavailability (never absence
    /// or invented data), the independent backup keeps serving its published snapshot, and restarting the live nodes
    /// recovers every acknowledged commit and resolves outcomes.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllOfflineModes))]
    [Catalog("OFF-010", "S", "INV-01", "INV-08", "INV-15")]
    public Task OFF_010_AllLiveNodesTemporarilyOffline(OfflineMode mode) =>
        ClusterTest.RunAsync(Sim(3, backups: 1), async cluster =>
        {
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            await using var own = client;
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: true), new DfsBackupBudgets(TimeSpan.FromMinutes(10), 1L << 30));
            IReadOnlyList<(string Path, byte[] Bytes)> files = await WriteDataSetAsync(v, "/o10", Ct);
            await v.Snapshots.CreateAsync("s1", null, Ct);
            await v.Backups.RequestSyncAsync(backup.TargetName, "s1", Ct);
            await v.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "s1", Bounded, Ct);
            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            CommitToken liveOnly = await v.Files.WriteAllBytesAsync("/o10/live-only", V2, new DfsMutationOptions { OperationId = k }, Ct);
            DfsOperationId kDuring = await v.NewOperationIdAsync(Ct);

            foreach (TestNode node in cluster.Nodes)
                await TakeOfflineAsync(cluster, node, mode);

            // Live service: honest unavailability.
            OffAttempt<byte[]> read = await TryAsync(() => v.Files.ReadAllBytesAsync("/o10/small", Ct));
            Assert.False(read.Succeeded);
            Assert.True(IsAvailabilityFailure(read.Error!), $"live read with every node offline: {read.Error}");
            OffAttempt<bool> exists = await TryAsync(() => v.Files.ExistsAsync("/o10/small", Ct));
            Assert.False(exists.Succeeded, $"Exists must not answer {exists.Value} without any live node");
            Assert.True(IsAvailabilityFailure(exists.Error!), $"Exists must propagate unavailability: {exists.Error}");
            OffAttempt<CommitToken> write = await TryAsync(() => v.Files.WriteAllBytesAsync("/o10/during", V1,
                new DfsMutationOptions { OperationId = kDuring }, Ct));
            Assert.False(write.Succeeded);
            Assert.True(IsNoSuccessFailure(write.Error!), $"write with every node offline: {write.Error}");

            // Backup reads continue, from the published snapshot only.
            await using (DfsClient backupClient = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk", Credentials = backup.ReaderCredentials }, Ct))
            {
                DfsVolume bv = backupClient.GetVolume(Volume);
                Assert.Contains(await bv.Snapshots.ListAsync(Ct), s => s.Name == "s1");
                await using DfsSnapshotView view = await bv.Snapshots.OpenAsync("s1", Ct);
                Assert.True(view.Files.IsReadOnly);
                await AssertFilesAsync(view.Files, files, "backup snapshot while every live node is offline", Ct);
                await Assert.ThrowsAnyAsync<FileNotFoundException>(() => view.Files.ReadAllBytesAsync("/o10/live-only", Ct));
            }

            // Restart: durable state recovered.
            foreach (TestNode node in cluster.Nodes)
                await BringOnlineAsync(cluster, node, mode);
            await cluster.WaitForStableAsync(Bounded, Ct);
            await AssertDataSetAsync(v, files, "after restarting every live node", Ct);
            await ExpectContentAsync(V2, v.Files.ReadAllBytesAsync("/o10/live-only", Ct), "acknowledged live-only commit");
            DfsOperationOutcome outcome = await v.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(liveOnly, outcome.Token);
            DfsOperationOutcome during = await v.QueryOperationAsync(kDuring, resolve: true, Ct);
            Assert.True(during.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted);
            if (mode is OfflineMode.Crash or OfflineMode.Stop || write.Classification == OpOutcome.Failed)
                Assert.Equal(DfsOutcomeStatus.Aborted, during.Status);
            Assert.Equal(during.Status == DfsOutcomeStatus.Committed, await v.Files.ExistsAsync("/o10/during", Ct));

            CommitToken resumed = await v.Files.WriteAllBytesAsync("/o10/after", V1, Ct);
            Assert.True(resumed.CompareTo(liveOnly) > 0);
        }, Ct);

    // ================================================================================================================
    // OFF-011 AllLiveNodesPermanentlyLost
    // ================================================================================================================

    /// <summary>
    /// OFF-011: every live disk is destroyed. Live data is reported unavailable (never absent or invented); the
    /// independent backup still serves exactly its retained snapshot; restoring onto clean re-bootstrapped hosts (new
    /// cluster identity) recovers exactly that snapshot — later unreplicated updates are not invented — under a new
    /// volume identity whose tokens do not order against the lost history.
    /// </summary>
    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    [Catalog("OFF-011", "S", "INV-01", "INV-08", "INV-18")]
    public Task OFF_011_AllLiveNodesPermanentlyLost(int nodeCount) =>
        ClusterTest.RunAsync(Sim(nodeCount, backups: 1), async cluster =>
        {
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            await using var own = client;
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: true), new DfsBackupBudgets(TimeSpan.FromMinutes(10), 1L << 30));
            IReadOnlyList<(string Path, byte[] Bytes)> files = await WriteDataSetAsync(v, "/o11", Ct);
            await v.Snapshots.CreateAsync("s1", null, Ct);
            await v.Backups.RequestSyncAsync(backup.TargetName, "s1", Ct);
            DfsBackupCheckpoint retained = await v.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "s1", Bounded, Ct);
            Assert.True(retained.IsExactSnapshot);
            VolumeId lostVolume = (await v.GetInfoAsync(Ct)).Id;

            // Updates after the last retained snapshot exist only on the live disks.
            await v.Files.WriteAllBytesAsync("/o11/small", V2, Ct);
            await v.Files.WriteAllBytesAsync("/o11/unreplicated", V1, Ct);
            await v.Files.DeleteAsync("/o11/empty", Ct);
            CommitToken lastLive = await v.Files.AppendAllBytesAsync("/o11/multi", V1, Ct);

            foreach (TestNode node in cluster.Nodes)
            {
                await node.CrashAsync();
                await node.ReplaceDiskWithEmptyAsync();
            }

            OffAttempt<byte[]> liveRead = await TryAsync(() => v.Files.ReadAllBytesAsync("/o11/small", Ct));
            Assert.False(liveRead.Succeeded, "no live data can be served after every live disk is destroyed");
            Assert.True(IsAvailabilityFailure(liveRead.Error!), $"live loss must be reported as unavailability: {liveRead.Error}");
            OffAttempt<bool> exists = await TryAsync(() => v.Files.ExistsAsync("/o11/unreplicated", Ct));
            Assert.False(exists.Succeeded, $"Exists must not answer {exists.Value} for destroyed live state");

            // The backup is independent and still serves exactly the retained snapshot.
            await using (DfsClient bc = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk", Credentials = backup.ReaderCredentials }, Ct))
            {
                await using DfsSnapshotView view = await bc.GetVolume(Volume).Snapshots.OpenAsync("s1", Ct);
                await AssertFilesAsync(view.Files, files, "retained backup snapshot", Ct);
                await Assert.ThrowsAnyAsync<FileNotFoundException>(() => view.Files.ReadAllBytesAsync("/o11/unreplicated", Ct));
            }

            // Recovery runbook: the clean hosts are bootstrapped as a new cluster identity, then the snapshot is restored.
            var recoveredClusterId = new Guid("0ff011aa-5a5a-4c4c-9e9e-000000000011");
            foreach (TestNode node in cluster.Nodes)
            {
                await node.RestartAsync(o =>
                {
                    Assert.NotNull(o.Bootstrap);
                    return o with { Bootstrap = o.Bootstrap with { ClusterId = recoveredClusterId, ClusterName = "recovered" } };
                });
            }

            DfsClient? admin = null;
            await cluster.WaitUntilAsync(async () =>
            {
                OffAttempt<DfsClient> attempt = await TryAsync(() => cluster.ConnectAsync(new TestClientOptions
                {
                    Name = "recovery-admin",
                    Credentials = cluster.AdminCredentials,
                    ExpectedClusterId = recoveredClusterId,
                    OperationTimeout = ShortOperation,
                }, Ct));
                admin = attempt.Value;
                return attempt.Succeeded;
            }, Bounded, "the re-bootstrapped hosts form the recovery cluster", Ct);
            await using var ownAdmin = admin!;
            Assert.Equal(recoveredClusterId, admin!.Cluster.ClusterId);
            Assert.DoesNotContain(Volume, await admin.Admin.ListVolumesAsync(Ct));

            DfsVolumeInfo restored = await admin.Admin.RestoreFromBackupAsync(new DfsRestoreFromBackupRequest(
                [backup.Endpoint], backup.ReaderCredentials, Volume, "s1", "vol-restored"), Ct);
            Assert.NotEqual(lostVolume, restored.Id);
            Assert.True(restored.Epoch >= 1);
            DfsVolume rv = admin.GetVolume("vol-restored");
            await AssertFilesAsync(rv.Files, files, "restored exactly from the retained snapshot", Ct);
            Assert.False(await rv.Files.ExistsAsync("/o11/unreplicated", Ct), "unreplicated updates must not be invented");
            Assert.False(lastLive.IsComparableWith(restored.LastCommit), "tokens of the lost history are not ordered against the restored volume");

            CommitToken fresh = await rv.Files.WriteAllBytesAsync("/o11/after-restore", V2, Ct);
            Assert.Equal(restored.Id, fresh.VolumeId);
            Assert.Equal(recoveredClusterId, fresh.ClusterId);
        }, Ct);

    // ================================================================================================================
    // OFF-012 ClientOfflineWithDirtyBuffer
    // ================================================================================================================

    public enum ClientCut
    {
        IsolateClient,
        BlackHoleClientRequests,
    }

    public enum DirtyBuffer
    {
        BufferedOnly,
        AutoStagedUploads,
    }

    public enum ClientOutage
    {
        ShorterThanLease,
        BeyondLeaseWithReplacementWriter,
        BeyondLeaseNoReplacement,
        BeyondSessionTimeout,
    }

    public static TheoryData<ClientCut, DirtyBuffer, ClientOutage> DirtyBufferCases
    {
        get
        {
            var data = new TheoryData<ClientCut, DirtyBuffer, ClientOutage>();
            foreach (ClientCut cut in Enum.GetValues<ClientCut>())
            {
                foreach (DirtyBuffer dirty in Enum.GetValues<DirtyBuffer>())
                {
                    foreach (ClientOutage outage in Enum.GetValues<ClientOutage>())
                        data.Add(cut, dirty, outage);
                }
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-012: a writer with dirty (buffered or auto-staged) data loses connectivity before flushing. Its private staging
    /// is never visible or reported as committed; after a short outage within its lease it may still publish, but after its
    /// lease/session expired (with or without a replacement writer) its publication is rejected, the stream faults, the
    /// stale buffer is never silently republished, and its staged objects are eventually reclaimed.
    /// </summary>
    [Theory]
    [MemberData(nameof(DirtyBufferCases))]
    [Catalog("OFF-012", "S", "INV-05", "INV-01", "INV-08")]
    public Task OFF_012_ClientOfflineWithDirtyBuffer(ClientCut cut, DirtyBuffer dirtyKind, ClientOutage outage) =>
        ClusterTest.RunAsync(Sim(3), async cluster =>
        {
            const string path = "/o12/f";
            NodeTimings t = cluster.Options.Timings;
            (DfsClient c0, DfsVolume w) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            (DfsClient c1, DfsVolume r) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            await w.Directories.CreateDirectoryAsync("/o12", Ct);
            byte[] v0 = Big1;
            await w.Files.WriteAllBytesAsync(path, v0, Ct);
            ulong revision0 = (await r.Files.GetInfoAsync(path, Ct)).Revision;
            var objects0 = (await GetFileObjectsAsync(cluster, path)).ToHashSet();

            byte[] dirty = dirtyKind == DirtyBuffer.BufferedOnly ? TestData.Bytes(0x0C12_0001, 1000) : TestData.Bytes(0x0C12_0002, 200 * 1024);
            byte[] committedIfPublished = Concat(dirty, v0[dirty.Length..]);
            DfsOperationId k = await w.NewOperationIdAsync(Ct);
            DfsStream stream = await w.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
            await stream.WriteAsync(dirty, Ct);   // private acceptance only

            async Task<byte[]> ReadSharedAsync()
            {
                await using DfsStream s = await r.Files.OpenAsync(path, new DfsOpenOptions { Access = FileAccess.Read, Share = FileShare.ReadWrite }, Ct);
                return await ReadToEndAsync(s);
            }

            await ExpectContentAsync(v0, ReadSharedAsync(), "dirty data is invisible to other clients before publication");

            // Disconnect the writer.
            if (cut == ClientCut.IsolateClient)
            {
                cluster.Network.Isolate(ClientName("c0"));
            }
            else
            {
                foreach (TestNode node in cluster.Nodes)
                    cluster.Network.BlackHole(ClientName("c0"), node.Name);
            }

            async Task AssertNothingPublishedAsync(byte[] expected, string because)
            {
                await ExpectContentAsync(expected, ReadSharedAsync(), because);
                FileInspection file = (await cluster.Inspect.GetFileAsync(Volume, path))!;
                if (expected == v0)
                {
                    Assert.Equal(revision0, file.Revision);
                    Assert.All(file.Extents, e => Assert.Contains(e.Object, objects0));
                }
            }

            byte[] finalExpected = v0;
            switch (outage)
            {
                case ClientOutage.ShorterThanLease:
                {
                    await cluster.DelayAsync(t.LeaseDuration / 4, Ct);
                    await AssertNothingPublishedAsync(v0, "while the writer is disconnected");
                    cluster.Network.Heal();
                    CommitToken token = await stream.CommitAsync(new DfsMutationOptions { OperationId = k }, Ct);
                    Assert.False(stream.IsFaulted);
                    finalExpected = committedIfPublished;
                    Assert.Equal(token.Sequence, (await r.Files.GetInfoAsync(path, Ct)).Revision);
                    break;
                }

                default:
                {
                    // While disconnected, the writer cannot claim durability.
                    OffAttempt<CommitToken> offlineCommit = await TryAsync(() => stream.CommitAsync(new DfsMutationOptions { OperationId = k }, Ct));
                    Assert.False(offlineCommit.Succeeded, "a disconnected writer must not report a durable commit");
                    Assert.True(IsNoSuccessFailure(offlineCommit.Error!), $"{offlineCommit.Error}");
                    TimeSpan total = outage == ClientOutage.BeyondSessionTimeout ? t.SessionTimeout + TimeSpan.FromSeconds(2) : t.LeaseDuration * 2;
                    await cluster.DelayAsync(total, Ct);
                    await AssertNothingPublishedAsync(v0, "after the writer's lease expired");

                    if (outage == ClientOutage.BeyondLeaseWithReplacementWriter)
                    {
                        (DfsClient c2, DfsVolume b) = await ConnectAsync(cluster, "c2", Ct);
                        await using var own2 = c2;
                        DfsStream? replacement = null;
                        await cluster.WaitUntilAsync(async () =>
                        {
                            OffAttempt<DfsStream> open = await TryAsync(() => b.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct));
                            replacement = open.Value;
                            return open.Succeeded;
                        }, Bounded, "the expired writer's reservation is released to a replacement writer", Ct);
                        Assert.True(replacement!.WriterGeneration > stream.WriterGeneration);
                        await replacement.WriteAsync(V2, Ct);
                        await replacement.SetLengthAsync(V2.Length, Ct);
                        await replacement.CommitAsync(Ct);
                        await replacement.DisposeAsync();
                        finalExpected = V2;
                    }

                    cluster.Network.Heal();
                    OffAttempt<CommitToken> lateCommit = await TryAsync(() => stream.CommitAsync(new DfsMutationOptions { OperationId = k }, Ct));
                    Assert.False(lateCommit.Succeeded, "the stale buffer must never be published after the lease was lost");
                    Assert.True(lateCommit.Error is DfsLeaseLostException or DfsPinExpiredException || stream.IsFaulted,
                        $"expected a lease-lost fault, got {lateCommit.Error}");
                    Assert.True(stream.IsFaulted, "lease loss or an indeterminate publication must fault the stream");
                    await Assert.ThrowsAnyAsync<Exception>(() => stream.WriteAsync(V1, Ct).AsTask());
                    DfsOperationOutcome resolved = await r.QueryOperationAsync(k, resolve: true, Ct);
                    Assert.NotEqual(DfsOutcomeStatus.Committed, resolved.Status);
                    break;
                }
            }

            await TryAsync(() => stream.DisposeAsync().AsTask());
            await ExpectContentAsync(finalExpected, ReadSharedAsync(), "final contents");
            await cluster.WaitUntilAsync(async () => await GetStagedBytesAsync(cluster) == 0, Bounded,
                "the disconnected writer's private staging is eventually reclaimed or published", Ct);
        }, Ct);

    // ================================================================================================================
    // OFF-013 ClientOfflineAfterCommitBeforeReply
    // ================================================================================================================

    public enum ReplyCutMutation
    {
        Append,
        WriteAll,
        Move,
        CreateDirectory,
        StreamCommit,
        TransactionCommit,
    }

    public enum ReplyCut
    {
        IsolateClient,
        BlackHoleReplies,
    }

    public static TheoryData<ReplyCutMutation, ReplyCut> ReplyCutCases
    {
        get
        {
            var data = new TheoryData<ReplyCutMutation, ReplyCut>();
            foreach (ReplyCutMutation m in Enum.GetValues<ReplyCutMutation>())
            {
                foreach (ReplyCut cut in Enum.GetValues<ReplyCut>())
                    data.Add(m, cut);
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-013: the client loses connectivity right after the mutation is applied, before the reply. The client reports an
    /// unknown outcome carrying its identity (never success, never a definite failure); a new client/session resolves it
    /// as committed with exactly one effect; once reconnected, a retry with the same identity returns the same outcome.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReplyCutCases))]
    [Catalog("OFF-013", "S", "INV-01", "INV-08")]
    public Task OFF_013_ClientOfflineAfterCommitBeforeReply(ReplyCutMutation mutation, ReplyCut cut) =>
        ClusterTest.RunAsync(Sim(3), async cluster =>
        {
            (DfsClient c0, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            (DfsClient c1, DfsVolume r) = await ConnectAsync(cluster, "c1", Ct);
            await using var own0 = c0;
            await using var own1 = c1;
            await v.Directories.CreateDirectoryAsync("/o13", Ct);
            await v.Files.WriteAllBytesAsync("/o13/f", V1, Ct);
            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            var withK = new DfsMutationOptions { OperationId = k };
            DfsStream? stream = null;
            DfsTransaction? tx = null;
            DfsOperationKind kind;
            Func<Task<CommitToken>> invoke;
            Func<Task> assertOnce;
            switch (mutation)
            {
                case ReplyCutMutation.Append:
                    kind = DfsOperationKind.Append;
                    invoke = () => v.Files.AppendAllBytesAsync("/o13/f", V2, withK, Ct);
                    assertOnce = () => ExpectContentAsync(Concat(V1, V2), r.Files.ReadAllBytesAsync("/o13/f", Ct), "appended exactly once");
                    break;
                case ReplyCutMutation.WriteAll:
                    kind = DfsOperationKind.WriteAll;
                    invoke = () => v.Files.WriteAllBytesAsync("/o13/f", Big1, withK, Ct);
                    assertOnce = () => ExpectContentAsync(Big1, r.Files.ReadAllBytesAsync("/o13/f", Ct), "replaced");
                    break;
                case ReplyCutMutation.Move:
                    kind = DfsOperationKind.Move;
                    invoke = () => v.Files.MoveAsync("/o13/f", "/o13/g", false, withK, Ct);
                    assertOnce = async () =>
                    {
                        Assert.False(await r.Files.ExistsAsync("/o13/f", Ct));
                        await ExpectContentAsync(V1, r.Files.ReadAllBytesAsync("/o13/g", Ct), "moved");
                    };
                    break;
                case ReplyCutMutation.CreateDirectory:
                    kind = DfsOperationKind.CreateDirectory;
                    invoke = async () => (await v.Directories.CreateDirectoryAsync("/o13/d", withK, Ct)).ObservedAt;
                    assertOnce = async () => Assert.True(await r.Directories.ExistsAsync("/o13/d", Ct));
                    break;
                case ReplyCutMutation.StreamCommit:
                    kind = DfsOperationKind.CommitFile;
                    stream = await v.Files.OpenAsync("/o13/f", new DfsOpenOptions { Mode = FileMode.Truncate, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
                    await stream.WriteAsync(Big2, Ct);
                    invoke = () => stream.CommitAsync(withK, Ct);
                    // The (faulted) writer handle may still be held: read with a writer-compatible share mode.
                    assertOnce = () => ExpectContentAsync(Big2, ReadSharedAsync(r, "/o13/f", Ct), "stream revision");
                    break;
                default:
                    kind = DfsOperationKind.TxCommit;
                    tx = await v.Transactions.BeginAsync(null, Ct);
                    await tx.Files.WriteAllBytesAsync("/o13/f", V2, Ct);
                    await tx.Files.WriteAllBytesAsync("/o13/t2", V2, Ct);
                    invoke = () => tx.CommitAsync(Ct);
                    assertOnce = async () =>
                    {
                        await ExpectContentAsync(V2, r.Files.ReadAllBytesAsync("/o13/f", Ct), "tx file 1");
                        await ExpectContentAsync(V2, r.Files.ReadAllBytesAsync("/o13/t2", Ct), "tx file 2");
                    };
                    break;
            }

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            FaultArm cutArm = cluster.Faults.Arm(new FaultMatch(FaultSites.ApplyAfterBeforeReply, Node: leader.Name, Operation: kind), FaultAction.Custom(_ =>
            {
                if (cut == ReplyCut.IsolateClient)
                {
                    cluster.Network.Isolate(ClientName("c0"));
                }
                else
                {
                    foreach (TestNode node in cluster.Nodes)
                        cluster.Network.BlackHole(node.Name, ClientName("c0"));
                }

                return Task.CompletedTask;
            }));

            var history = new HistoryRecorder();
            long op = history.Invoke("c0", kind.ToString(), "/o13", null, tx is null ? k : null);
            OffAttempt<CommitToken> result = await TryAsync(invoke);
            cluster.Faults.AssertReached(cutArm);
            Assert.False(result.Succeeded, "no reply can reach the disconnected client");
            Assert.Equal(OpOutcome.Unknown, result.Classification);
            DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(result.Error);
            if (tx is null)
                Assert.Equal(k, unknown.OperationId);
            else
                Assert.Equal(tx.Id, unknown.TransactionId);
            history.Complete(op, OpOutcome.Unknown, result.Error);

            DfsOperationOutcome outcome = tx is null
                ? await r.QueryOperationAsync(k, resolve: false, Ct)
                : await r.Transactions.QueryOutcomeAsync(tx.Id, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            history.Resolve(op, OpOutcome.Ok, outcome.Token);
            Assert.True(history.Get(op).WasResolved);
            await assertOnce();

            cluster.Network.Heal();
            switch (mutation)
            {
                case ReplyCutMutation.StreamCommit:
                    Assert.True(stream!.IsFaulted, "an indeterminate publication faults the stream");
                    await TryAsync(() => stream.DisposeAsync().AsTask());
                    break;
                case ReplyCutMutation.TransactionCommit:
                {
                    OffAttempt<CommitToken> again = await TryAsync(() => tx!.CommitAsync(Ct));
                    if (again.Succeeded)
                        Assert.Equal(outcome.Token, again.Value);
                    await TryAsync(() => tx!.DisposeAsync().AsTask());
                    break;
                }

                case ReplyCutMutation.CreateDirectory:
                {
                    // CreateDirectory returns metadata: its ObservedAt is "read at or after" the creating commit.
                    InodeId created = (await r.Directories.GetInfoAsync("/o13/d", Ct)).InodeId;
                    DfsDirectoryInfo again = await v.Directories.CreateDirectoryAsync("/o13/d", withK, Ct);
                    Assert.Equal(created, again.InodeId);
                    Assert.True(again.ObservedAt.IsAtOrAfter(outcome.Token));
                    break;
                }

                default:
                {
                    CommitToken retried = await invoke();
                    Assert.Equal(outcome.Token, retried);
                    break;
                }
            }

            await assertOnce();
        }, Ct);

    // ================================================================================================================
    // OFF-014 MinorityReadNotLabelledLive
    // ================================================================================================================

    public enum MinorityShape
    {
        ThreeLeaderAlone,
        ThreeFollowerAlone,
        ThreeAllSplit,
        ThreeLeaderCannotSend,
        ThreeLeaderCannotReceive,
        FiveLeaderWithOneFollower,
        FiveTwoFollowers,
    }

    public static TheoryData<MinorityShape> MinorityShapes => new(Enum.GetValues<MinorityShape>());

    /// <summary>
    /// OFF-014: a client that can only reach a minority (or a leader cut off in one direction) uses every live-read API
    /// (whole-file read, stream read and length on a live handle, existence, metadata, enumeration). Each call either
    /// fails honestly or is rerouted to the authoritative state; it never returns the stale pre-partition state. Checked
    /// immediately and after election/lease windows, then after healing; the history is linearizable.
    /// </summary>
    [Theory]
    [MemberData(nameof(MinorityShapes))]
    [Catalog("OFF-014", "S", "INV-03", "INV-08")]
    public Task OFF_014_MinorityReadNotLabelledLive(MinorityShape shape) =>
        ClusterTest.RunAsync(Sim(shape.ToString().StartsWith("Three", StringComparison.Ordinal) ? 3 : 5), async cluster =>
        {
            (DfsClient admin, DfsVolume av) = await ConnectAsync(cluster, "setup", Ct);
            await using var ownA = admin;
            await av.Directories.CreateDirectoryAsync("/p", Ct);
            var history = new HistoryRecorder();
            await history.RecordAsync("setup", "write", "/p/f", V1, () => av.Files.WriteAllBytesAsync("/p/f", V1, Ct));
            await av.Files.WriteAllBytesAsync("/p/g", V1, Ct);

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();
            string[] minority;
            string[] majority;
            switch (shape)
            {
                case MinorityShape.ThreeLeaderAlone:
                case MinorityShape.ThreeAllSplit:
                case MinorityShape.ThreeLeaderCannotSend:
                case MinorityShape.ThreeLeaderCannotReceive:
                    minority = [leader.Name];
                    majority = followers.Select(n => n.Name).ToArray();
                    break;
                case MinorityShape.ThreeFollowerAlone:
                    minority = [followers[0].Name];
                    majority = [leader.Name, followers[1].Name];
                    break;
                case MinorityShape.FiveLeaderWithOneFollower:
                    minority = [leader.Name, followers[0].Name];
                    majority = followers.Skip(1).Select(n => n.Name).ToArray();
                    break;
                default:
                    minority = [followers[0].Name, followers[1].Name];
                    majority = [leader.Name, .. followers.Skip(2).Select(n => n.Name)];
                    break;
            }

            (DfsClient cmin, DfsVolume vmin) = await ConnectAsync(cluster, "cmin", Ct, seeds: minority, operationTimeout: ShortOperation);
            (DfsClient cmaj, DfsVolume vmaj) = await ConnectAsync(cluster, "cmaj", Ct, seeds: majority);
            await using var ownMin = cmin;
            await using var ownMaj = cmaj;
            DfsStream liveHandle = await vmin.Files.OpenAsync("/p/f", new DfsOpenOptions { Access = FileAccess.Read, Share = FileShare.ReadWrite }, Ct);

            bool asymmetric = shape is MinorityShape.ThreeLeaderCannotSend or MinorityShape.ThreeLeaderCannotReceive;
            switch (shape)
            {
                case MinorityShape.ThreeAllSplit:
                    cluster.Network.Partition([leader.Name, ClientName("cmin")], [followers[0].Name, ClientName("cmaj"), ClientName("setup")], [followers[1].Name]);
                    break;
                case MinorityShape.ThreeLeaderCannotSend:
                    foreach (TestNode f in followers)
                        cluster.Network.Cut(leader.Name, f.Name);
                    break;
                case MinorityShape.ThreeLeaderCannotReceive:
                    foreach (TestNode f in followers)
                        cluster.Network.Cut(f.Name, leader.Name);
                    break;
                default:
                    cluster.Network.Partition([.. minority, ClientName("cmin")], [.. majority, ClientName("cmaj"), ClientName("setup")]);
                    break;
            }

            bool majorityExists = shape != MinorityShape.ThreeAllSplit;
            ulong revisionV2 = 0;
            if (majorityExists)
            {
                long writeOp = history.Invoke("cmaj", "write", "/p/f", V2);
                CommitToken token = default;
                await cluster.WaitUntilAsync(async () =>
                {
                    OffAttempt<CommitToken> w = await TryAsync(() => vmaj.Files.WriteAllBytesAsync("/p/f", V2, Ct));
                    token = w.Value;
                    return w.Succeeded;
                }, Bounded, "the majority side elects a leader and accepts the write", Ct);
                history.Complete(writeOp, OpOutcome.Ok, token);
                revisionV2 = token.Sequence;
                await vmaj.Files.DeleteAsync("/p/g", Ct);
                await vmaj.Files.WriteAllBytesAsync("/p/h", V2, Ct);
            }

            foreach (TimeSpan wait in new[] { TimeSpan.Zero, cluster.Options.Timings.LeaseDuration + TimeSpan.FromSeconds(1) })
            {
                await cluster.DelayAsync(wait, Ct);
                string when = $"{shape} after {wait}";
                await cluster.Inspect.AssertNoSplitBrainAsync();

                OffAttempt<byte[]> all = await TryAsync(() => history.RecordAsync("cmin", "read", "/p/f", null, () => vmin.Files.ReadAllBytesAsync("/p/f", Ct)));
                CheckFresh(all, x => AssertEx.BytesEqual(V2, x, $"{when}: ReadAllBytes"), when + " ReadAllBytes");

                OffAttempt<byte[]> viaStream = await TryAsync(async () =>
                {
                    await using DfsStream s = await vmin.Files.OpenReadAsync("/p/f", Ct);
                    return await ReadToEndAsync(s);
                });
                CheckFresh(viaStream, x => AssertEx.BytesEqual(V2, x, $"{when}: OpenRead"), when + " OpenRead");

                OffAttempt<long> length = await TryAsync(() => liveHandle.GetLengthAsync(Ct).AsTask());
                CheckFresh(length, x => Assert.Equal(V2.Length, x), when + " live handle length");

                OffAttempt<bool> deleted = await TryAsync(() => vmin.Files.ExistsAsync("/p/g", Ct));
                CheckFresh(deleted, Assert.False, when + " Exists(deleted)");

                OffAttempt<bool> created = await TryAsync(() => vmin.Files.ExistsAsync("/p/h", Ct));
                CheckFresh(created, Assert.True, when + " Exists(created)");

                OffAttempt<DfsFileInfo> info = await TryAsync(() => vmin.Files.GetInfoAsync("/p/f", Ct));
                CheckFresh(info, x => Assert.Equal(revisionV2, x!.Revision), when + " GetInfo");

                OffAttempt<List<DfsEntryInfo>> listing = await TryAsync(async () => await vmin.Directories.EnumerateAsync("/p", null, Ct).ToListAsync(Ct));
                CheckFresh(listing, x => Assert.Equal(new[] { "f", "h" }, x!.Select(e => e.Name).Order(StringComparer.Ordinal)), when + " Enumerate");
            }

            void CheckFresh<T>(OffAttempt<T> attempt, Action<T> assertFresh, string what)
            {
                if (attempt.Succeeded)
                {
                    Assert.True(majorityExists || asymmetric, $"{what}: a live read was answered without any majority authority");
                    assertFresh(attempt.Value!);
                }
                else
                {
                    Assert.True(IsAvailabilityFailure(attempt.Error!), $"{what}: must fail honestly (not as absence) or be rerouted: {attempt.Error}");
                }
            }

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(Bounded, Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            byte[] final = await history.RecordAsync("cmin", "read", "/p/f", null, () => vmin.Files.ReadAllBytesAsync("/p/f", Ct));
            AssertEx.BytesEqual(majorityExists ? V2 : V1, final, "after healing");
            await TryAsync(() => liveHandle.DisposeAsync().AsTask());
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid();
        }, Ct);

    // ================================================================================================================
    // OFF-015 PinnedReadDuringPartition
    // ================================================================================================================

    public enum PinnedShape
    {
        ThreeClientWithFollower,
        ThreeClientWithLeader,
        FiveClientWithTwoFollowers,
    }

    public static TheoryData<PinnedShape> PinnedShapes => new(Enum.GetValues<PinnedShape>());

    /// <summary>
    /// OFF-015: a client partitioned with a minority holds a version-pinned stream and a snapshot view opened before the
    /// partition. Within the pin contract such reads return exactly the pinned historical bytes, labelled as pinned;
    /// they never switch to live or newer data, never return partial data, and after the pin lapsed they fail
    /// explicitly. Live reads from the minority fail meanwhile.
    /// </summary>
    [Theory]
    [MemberData(nameof(PinnedShapes))]
    [Catalog("OFF-015", "S", "INV-03", "INV-08")]
    public Task OFF_015_PinnedReadDuringPartition(PinnedShape shape) =>
        ClusterTest.RunAsync(Sim(shape == PinnedShape.FiveClientWithTwoFollowers ? 5 : 3), async cluster =>
        {
            NodeTimings t = cluster.Options.Timings;
            (DfsClient setup, DfsVolume sv) = await ConnectAsync(cluster, "setup", Ct);
            await using var ownS = setup;
            await sv.Directories.CreateDirectoryAsync("/h", Ct);
            await sv.Files.WriteAllBytesAsync("/h/f", Big1, Ct);
            // Every node (so also the minority side) holds every object: the pinned view's data are available there.
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            DfsSnapshotInfo snapshot = await sv.Snapshots.CreateAsync("s1", null, Ct);
            ulong revision1 = (await sv.Files.GetInfoAsync("/h/f", Ct)).Revision;

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();
            string[] minority = shape switch
            {
                PinnedShape.ThreeClientWithFollower => [followers[0].Name],
                PinnedShape.ThreeClientWithLeader => [leader.Name],
                _ => [followers[0].Name, followers[1].Name],
            };
            string[] majority = cluster.Nodes.Select(n => n.Name).Except(minority).ToArray();

            (DfsClient cmin, DfsVolume vmin) = await ConnectAsync(cluster, "cmin", Ct, seeds: minority, operationTimeout: ShortOperation);
            (DfsClient cmaj, DfsVolume vmaj) = await ConnectAsync(cluster, "cmaj", Ct, seeds: majority);
            await using var ownMin = cmin;
            await using var ownMaj = cmaj;
            DfsStream pinned = await vmin.Files.OpenAsync("/h/f", new DfsOpenOptions { Access = FileAccess.Read, Share = FileShare.ReadWrite, ReadView = DfsReadView.VersionPinned }, Ct);
            Assert.Equal(DfsReadView.VersionPinned, pinned.ReadView);
            Assert.Equal(revision1, pinned.OpenRevision);
            DfsSnapshotView view = await vmin.Snapshots.OpenAsync("s1", Ct);
            Assert.Equal(snapshot.Id, view.Info.Id);
            Assert.True(view.Files.IsReadOnly);

            cluster.Network.Partition([.. minority, ClientName("cmin")], [.. majority, ClientName("cmaj"), ClientName("setup")]);
            await cluster.WaitUntilAsync(async () => (await TryAsync(() => vmaj.Files.WriteAllBytesAsync("/h/f", Big2, Ct))).Succeeded,
                Bounded, "the majority publishes a newer revision", Ct);

            void AssertHistoricalOrExplicitFailure(OffAttempt<byte[]> attempt, string what, bool allowAvailability = true)
            {
                if (attempt.Succeeded)
                {
                    AssertEx.BytesEqual(Big1, attempt.Value, $"{what}: a pinned/historical read returns exactly the pinned revision");
                }
                else
                {
                    // An expired pin/handle is an explicit, typed failure (LOCK-05: never data from reclaimed extents).
                    bool expired = attempt.Error is DfsPinExpiredException or DfsLeaseLostException
                        || attempt.Error is DfsException { ErrorCode: DfsErrorCode.PinExpired or DfsErrorCode.LeaseLost or DfsErrorCode.InvalidHandle };
                    Assert.True(expired || (allowAvailability && IsAvailabilityFailure(attempt.Error!)),
                        $"{what}: must fail explicitly, got {attempt.Error}");
                }
            }

            async Task<byte[]> ReadPinnedFromStartAsync()
            {
                pinned.Position = 0;
                return await ReadToEndAsync(pinned);
            }

            // Within the pin contract.
            AssertHistoricalOrExplicitFailure(await TryAsync(ReadPinnedFromStartAsync), "pinned stream within its pin");
            AssertHistoricalOrExplicitFailure(await TryAsync(() => view.Files.ReadAllBytesAsync("/h/f", Ct)), "snapshot view within its pin");
            Assert.Equal(revision1, pinned.OpenRevision);
            OffAttempt<byte[]> live = await TryAsync(() => vmin.Files.ReadAllBytesAsync("/h/f", Ct));
            Assert.False(live.Succeeded, "a live read must not be served by the minority");
            Assert.True(IsAvailabilityFailure(live.Error!), $"{live.Error}");

            // Beyond the pin lifetime without renewal: never newer data, never silently live.
            await cluster.DelayAsync(t.PinDuration + t.LeaseDuration + TimeSpan.FromSeconds(1), Ct);
            AssertHistoricalOrExplicitFailure(await TryAsync(ReadPinnedFromStartAsync), "pinned stream after its pin lapsed");

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(Bounded, Ct);
            AssertHistoricalOrExplicitFailure(await TryAsync(ReadPinnedFromStartAsync), "pinned stream after healing", allowAvailability: false);
            OffAttempt<byte[]> viewAfter = await TryAsync(() => view.Files.ReadAllBytesAsync("/h/f", Ct));
            AssertHistoricalOrExplicitFailure(viewAfter, "snapshot view after healing", allowAvailability: false);
            await TryAsync(() => view.DisposeAsync().AsTask());
            await using (DfsSnapshotView reopened = await vmin.Snapshots.OpenAsync("s1", Ct))
                await ExpectContentAsync(Big1, reopened.Files.ReadAllBytesAsync("/h/f", Ct), "snapshot stays historical");
            await ExpectContentAsync(Big2, vmin.Files.ReadAllBytesAsync("/h/f", Ct), "live view after healing");
            await TryAsync(() => pinned.DisposeAsync().AsTask());
        }, Ct);

    // ================================================================================================================
    // OFF-016 OfflineNodeNotImplicitlyRemoved — every voter-disconnect subset of the 3- and 5-voter models
    // ================================================================================================================

    public static TheoryData<int, int> VoterDisconnectSubsets
    {
        get
        {
            var data = new TheoryData<int, int>();
            foreach (int n in new[] { 3, 5 })
            {
                foreach (int mask in AllSubsets(n))
                    data.Add(n, mask);
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-016: disconnecting any subset of voting members for longer than election, lease and session windows never
    /// changes the committed configuration (voters, learners, node status); progress happens exactly when a majority of
    /// the configured voters is reachable — never because the reachable set alone forms a "majority of itself".
    /// </summary>
    [Theory]
    [MemberData(nameof(VoterDisconnectSubsets))]
    [Catalog("OFF-016", "S", "INV-11", "INV-18", "INV-08")]
    public Task OFF_016_OfflineNodeNotImplicitlyRemoved(int nodeCount, int disconnectedMask) =>
        ClusterTest.RunAsync(Sim(nodeCount), async cluster =>
        {
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            await using var own = client;
            await v.Directories.CreateDirectoryAsync("/o16", Ct);
            await v.Files.WriteAllBytesAsync("/o16/f", V1, Ct);
            GroupInspection volume0 = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            GroupInspection control0 = await cluster.Inspect.GetControlGroupAsync();
            string[] voters = volume0.Voters.Order(StringComparer.Ordinal).ToArray();
            string[] learners = volume0.Learners.Order(StringComparer.Ordinal).ToArray();
            string[] controlVoters = control0.Voters.Order(StringComparer.Ordinal).ToArray();
            var disconnected = Members(disconnectedMask, nodeCount).Select(i => cluster[i].Name).ToHashSet(StringComparer.Ordinal);
            bool configuredMajority = HasConfiguredMajority(voters, disconnected);
            int reachableVoters = voters.Count(x => !disconnected.Contains(x));
            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            // Generous deadline where progress is required (pooled connections to cut nodes must be abandoned, a retry
            // schedule is not prescribed); short where only an honest failure is expected.
            (DfsClient prober, DfsVolume pv) = await ConnectAsync(cluster, "c-voters", Ct,
                operationTimeout: configuredMajority ? ProgressOperation : ShortOperation);
            await using var ownProber = prober;

            foreach (string name in disconnected)
                cluster.Network.Isolate(name);
            NodeTimings t = cluster.Options.Timings;
            await cluster.DelayAsync(t.SessionTimeout + t.LeaseDuration + TimeSpan.FromSeconds(3), Ct);

            // The committed membership configuration is unchanged on every node (reachable or not).
            foreach (TestNode node in cluster.Nodes)
            {
                GroupInspection? g = await node.Inspect.GetGroupAsync(Volume);
                Assert.NotNull(g);
                Assert.Equal(voters, g.Voters.Order(StringComparer.Ordinal));
                Assert.Equal(learners, g.Learners.Order(StringComparer.Ordinal));
                GroupInspection? c = await node.Inspect.GetGroupAsync("control");
                Assert.NotNull(c);
                Assert.Equal(controlVoters, c.Voters.Order(StringComparer.Ordinal));
            }

            OffAttempt<CommitToken> write = await TryAsync(() => pv.Files.WriteAllBytesAsync("/o16/f", V2, new DfsMutationOptions { OperationId = k }, Ct));
            string context = $"{nodeCount} voters, disconnected {{{string.Join(",", disconnected.Order(StringComparer.Ordinal))}}}: {reachableVoters} of {voters.Length} configured voters reachable";
            if (configuredMajority)
            {
                Assert.True(write.Succeeded, $"{context}: a configured majority must make progress: {write}");
            }
            else
            {
                Assert.False(write.Succeeded, $"{context}: quorum must be computed over the configured voters, not the {reachableVoters} reachable ones");
                Assert.True(IsNoSuccessFailure(write.Error!), $"{context}: {write.Error}");
            }

            OffAttempt<IReadOnlyList<DfsNodeInfo>> nodes = await TryAsync(() => cluster.Admin.ListNodesAsync(Ct));
            if (nodes.Succeeded)
            {
                foreach (DfsNodeInfo info in nodes.Value!.Where(n => disconnected.Contains(n.Name)))
                    Assert.NotEqual(DfsNodeStatus.Removed, info.Status);
            }

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(Bounded, Ct);
            GroupInspection healed = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            Assert.Equal(voters, healed.Voters.Order(StringComparer.Ordinal));
            DfsOperationOutcome resolved = await v.QueryOperationAsync(k, resolve: true, Ct);
            if (write.Succeeded)
                Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
            else if (write.Classification == OpOutcome.Failed)
                Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
            await ExpectContentAsync(resolved.Status == DfsOutcomeStatus.Committed ? V2 : V1, v.Files.ReadAllBytesAsync("/o16/f", Ct), context);
            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);

    // ================================================================================================================
    // OFF-017 RepeatedOfflineOnlineCycles
    // ================================================================================================================

    public enum CycledRole
    {
        VolumeLeader,
        Follower,
        EveryNodeRoundRobin,
        Client,
    }

    public static TheoryData<CycledRole, int> CycleCases
    {
        get
        {
            var data = new TheoryData<CycledRole, int>();
            foreach (CycledRole role in Enum.GetValues<CycledRole>())
            {
                data.Add(role, 3);
                data.Add(role, 5);
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-017: each role is cycled repeatedly through disconnect, pause, crash and graceful stop with restart/reconnect,
    /// while appends and replacements continue. After stable recovery: retried/unknown operations resolve with single
    /// effects (linearizable histories), no lease or staging is leaked, replicas converge, repair restores every reachable
    /// object's placement, and no split brain occurred.
    /// </summary>
    [Theory]
    [MemberData(nameof(CycleCases))]
    [Catalog("OFF-017", "S", "INV-01", "INV-05", "INV-18", "INV-11")]
    public Task OFF_017_RepeatedOfflineOnlineCycles(CycledRole role, int nodeCount) =>
        ClusterTest.RunAsync(Sim(nodeCount) with { ScenarioTimeout = TimeSpan.FromMinutes(10) }, async cluster =>
        {
            const string log = "/cy/log", reg = "/cy/reg";
            var logHistory = new HistoryRecorder();
            var regHistory = new HistoryRecorder();
            var pending = new List<(HistoryRecorder Recorder, long Op, DfsOperationId Id)>();
            int clientGeneration = 0;
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "w0", Ct, operationTimeout: ShortOperation);
            await v.Directories.CreateDirectoryAsync("/cy", Ct);
            int step = 0;

            async Task WorkAsync(DfsVolume vol, string process)
            {
                for (int j = 0; j < 2; j++)
                {
                    step++;
                    OffAttempt<DfsOperationId> id = await TryAsync(() => vol.NewOperationIdAsync(Ct));
                    DfsOperationId? opId = id.Succeeded ? id.Value : null;
                    byte[] entry = TestData.Bytes(0x0C7C_0000UL + (ulong)step, 24);
                    long appendOp = logHistory.Invoke(process, "append", log, entry, opId);
                    OffAttempt<CommitToken> appended = await TryAsync(() => vol.Files.AppendAllBytesAsync(log, entry, new DfsMutationOptions { OperationId = opId }, Ct));
                    logHistory.Complete(appendOp, appended.Classification, appended.Succeeded ? appended.Value : appended.Error);
                    if (appended.Classification == OpOutcome.Unknown && opId is not null)
                        pending.Add((logHistory, appendOp, opId.Value));

                    OffAttempt<DfsOperationId> id2 = await TryAsync(() => vol.NewOperationIdAsync(Ct));
                    DfsOperationId? opId2 = id2.Succeeded ? id2.Value : null;
                    byte[] value = TestData.Bytes(0x0C7D_0000UL + (ulong)step, 40);
                    long writeOp = regHistory.Invoke(process, "write", reg, value, opId2);
                    OffAttempt<CommitToken> written = await TryAsync(() => vol.Files.WriteAllBytesAsync(reg, value, new DfsMutationOptions { OperationId = opId2 }, Ct));
                    regHistory.Complete(writeOp, written.Classification, written.Succeeded ? written.Value : written.Error);
                    if (written.Classification == OpOutcome.Unknown && opId2 is not null)
                        pending.Add((regHistory, writeOp, opId2.Value));

                    await TryAsync(() => regHistory.RecordAsync(process, "read", reg, null, () => vol.Files.ReadAllBytesAsync(reg, Ct)));
                }
            }

            OfflineMode[] transitions = [OfflineMode.Disconnect, OfflineMode.Pause, OfflineMode.Crash, OfflineMode.Stop];
            for (int cycle = 0; cycle < 2; cycle++)
            {
                foreach (OfflineMode transition in transitions)
                {
                    string process = $"w{clientGeneration}";
                    if (role == CycledRole.Client)
                    {
                        string netName = ClientName(process);
                        switch (transition)
                        {
                            case OfflineMode.Disconnect:
                                cluster.Network.Isolate(netName);
                                await WorkAsync(v, process);
                                cluster.Network.Heal();
                                break;
                            case OfflineMode.Pause:
                            {
                                // Everything the client sends is held for a lease duration, then delivered late; disposing
                                // the hold also stops capturing, so the remaining work is not silently held forever.
                                MessageHold hold = cluster.Network.Hold(new MessageFilter(From: netName));
                                Task during = WorkAsync(v, process);
                                await cluster.DelayAsync(cluster.Options.Timings.LeaseDuration, Ct);
                                hold.ReleaseAll();
                                hold.Dispose();
                                await during;
                                break;
                            }

                            case OfflineMode.Crash:
                                cluster.KillClient(client);
                                clientGeneration++;
                                (client, v) = await ConnectAsync(cluster, $"w{clientGeneration}", Ct, operationTimeout: ShortOperation);
                                break;
                            default:
                                await client.DisposeAsync();
                                clientGeneration++;
                                (client, v) = await ConnectAsync(cluster, $"w{clientGeneration}", Ct, operationTimeout: ShortOperation);
                                break;
                        }

                        await WorkAsync(v, $"w{clientGeneration}");
                        continue;
                    }

                    TestNode target = role switch
                    {
                        CycledRole.VolumeLeader => await cluster.GetLeaderAsync(Volume, Ct),
                        CycledRole.Follower => await GetFollowerAsync(cluster, Ct),
                        _ => cluster[(cycle * transitions.Length + Array.IndexOf(transitions, transition)) % nodeCount],
                    };
                    await TakeOfflineAsync(cluster, target, transition);
                    await WorkAsync(v, process);
                    await cluster.Inspect.AssertNoSplitBrainAsync();
                    await BringOnlineAsync(cluster, target, transition);
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    await WorkAsync(v, process);
                }
            }

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(Bounded, Ct);

            // Retries/unknowns resolve to single effects.
            foreach ((HistoryRecorder recorder, long op, DfsOperationId id) in pending)
            {
                DfsOperationOutcome outcome = await v.QueryOperationAsync(id, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted or DfsOutcomeStatus.Expired);
                if (outcome.Status != DfsOutcomeStatus.Expired)
                    recorder.Resolve(op, outcome.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);
            }

            await logHistory.RecordAsync("final", "read", log, null, () => v.Files.ReadAllBytesAsync(log, Ct));
            await regHistory.RecordAsync("final", "read", reg, null, () => v.Files.ReadAllBytesAsync(reg, Ct));
            LinearizabilityChecker.Check(logHistory.Operations, new AppendLogModel()).AssertValid();
            LinearizabilityChecker.Check(regHistory.Operations, new RegisterModel()).AssertValid();
            await client.DisposeAsync();

            // Convergence of replicas, leases, staging, repair and references.
            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
            Assert.Equal(0, health.UnderReplicatedObjects);
            Assert.Equal(0, health.ZoneDeficitObjects);
            Assert.Equal(0, health.MissingObjects);
            Assert.Equal(0, health.PlacementViolations);
            await cluster.WaitUntilAsync(async () =>
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
                return state.Handles.All(h => h.Expired) && state.Staging.All(s => !s.Open);
            }, Bounded, "no lease or staging session of a dead/closed client remains active", Ct);
            await cluster.WaitUntilAsync(async () => await GetStagedBytesAsync(cluster) == 0, Bounded, "staged bytes are reclaimed", Ct);
            VolumeStateInspection final = await cluster.Inspect.GetVolumeStateAsync(Volume);
            DfsStoragePolicy policy = (await cluster.Admin.GetVolumeStatusAsync(Volume, Ct)).Info.DefaultPolicy;
            foreach (ObjectId obj in final.ReachableObjects())
            {
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
                Assert.True(copies >= policy.TargetCopies && zones >= policy.TargetDistinctZones, $"{obj}: {copies} copies / {zones} zones after repair");
            }
        }, Ct);

    // ================================================================================================================
    // OFF-018 DifferentRestartOrders
    // ================================================================================================================

    public static TheoryData<int, string, bool> RestartOrders
    {
        get
        {
            var data = new TheoryData<int, string, bool>();
            foreach (string order in Permutations("012"))
            {
                data.Add(3, order, false);
                data.Add(3, order, true);
            }

            // Five nodes, explicit volume voters n0..n2: L = old volume leader, V = other volume voters, D = data-only nodes.
            foreach (string order in Permutations("LVD"))
            {
                data.Add(5, order, false);
                data.Add(5, order, true);
            }

            return data;
        }
    }

    private static IEnumerable<string> Permutations(string items) =>
        items.Length <= 1
            ? [items]
            : items.SelectMany((c, i) => Permutations(items.Remove(i, 1)).Select(rest => c + rest));

    /// <summary>
    /// OFF-018: after a whole-cluster stop (crash or graceful), data, metadata and control roles restart in every bounded
    /// order. While fewer than a configured majority of volume voters run, no node assumes leadership and no write
    /// succeeds; once a majority runs a leader is elected within a bounded time (no bootstrap deadlock); finally all
    /// acknowledged data and outcomes are intact and replicas converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(RestartOrders))]
    [Catalog("OFF-018", "S", "INV-01", "INV-18", "INV-08")]
    public Task OFF_018_DifferentRestartOrders(int nodeCount, string order, bool graceful) =>
        ClusterTest.RunAsync(nodeCount == 3 ? Sim(3) : ExplicitVoters(5), async cluster =>
        {
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            await using var own = client;
            IReadOnlyList<(string Path, byte[] Bytes)> files = await WriteDataSetAsync(v, "/o18", Ct);
            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            CommitToken acknowledged = await v.Files.AppendAllBytesAsync("/o18/small", V1, new DfsMutationOptions { OperationId = k }, Ct);
            var expected = files.Select(f => f.Path == "/o18/small" ? (f.Path, Concat(f.Bytes, V1)) : f).ToList();
            TestNode oldLeader = await cluster.GetLeaderAsync(Volume, Ct);
            string[] voters = (await GetVolumeVotersAsync(cluster)).ToArray();

            foreach (TestNode node in cluster.Nodes)
            {
                if (graceful)
                    await node.StopAsync();
                else
                    await node.CrashAsync();
            }

            List<TestNode[]> groups = nodeCount == 3
                ? order.Select(ch => new[] { cluster[ch - '0'] }).ToList()
                : order.Select(ch => ch switch
                {
                    'L' => new[] { oldLeader },
                    'V' => cluster.Nodes.Where(n => voters.Contains(n.Name) && n.Name != oldLeader.Name).ToArray(),
                    _ => cluster.Nodes.Where(n => !voters.Contains(n.Name)).ToArray(),
                }).ToList();

            foreach (TestNode[] group in groups)
            {
                foreach (TestNode node in group)
                    await node.RestartAsync();
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                int runningVoters = cluster.Nodes.Count(n => n.State == TestNodeState.Running && voters.Contains(n.Name));
                string context = $"order {order} ({(graceful ? "graceful" : "crash")}): {runningVoters}/{voters.Length} voters running";
                if (runningVoters * 2 <= voters.Length)
                {
                    await AssertNoRunningLeaderAsync(cluster, context);
                    OffAttempt<CommitToken> premature = await TryAsync(() => v.Files.WriteAllBytesAsync("/o18/premature", V2, Ct));
                    Assert.False(premature.Succeeded, $"{context}: no write may succeed before a configured majority runs");
                    await AssertNoRunningLeaderAsync(cluster, context);
                }
                else
                {
                    await WaitForNewLeaderAsync(cluster, null, 0, Ct);
                }
            }

            await cluster.WaitForStableAsync(Bounded, Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await AssertDataSetAsync(v, expected, $"after restart order {order}", Ct);
            DfsOperationOutcome outcome = await v.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.Equal(acknowledged, outcome.Token);
            CommitToken next = await v.Files.WriteAllBytesAsync("/o18/after", V2, Ct);
            Assert.True(next.CompareTo(acknowledged) > 0);
            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
        }, Ct);

    // ================================================================================================================
    // OFF-019 BackupOfflineDoesNotBlockLive
    // ================================================================================================================

    public enum BackupOutage
    {
        Stopped,
        Isolated,
        Paused,
    }

    public enum BudgetExceeded
    {
        PinAge,
        PinnedBytes,
    }

    public static TheoryData<BackupOutage, BudgetExceeded> BackupOutageCases
    {
        get
        {
            var data = new TheoryData<BackupOutage, BudgetExceeded>();
            foreach (BackupOutage outage in Enum.GetValues<BackupOutage>())
            {
                data.Add(outage, BudgetExceeded.PinAge);
                data.Add(outage, BudgetExceeded.PinnedBytes);
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-019: the late-sync target stays absent through many commits and snapshots. Live commits never wait for it
    /// (bounded latency, correct contents), it never appears in voter/learner/node sets, and once its transfer-pin budget
    /// (age or bytes) is exceeded the source abandons the lineage (pins released, reseed required); on return it is
    /// reseeded under a new lineage and publishes the latest snapshot.
    /// </summary>
    [Theory]
    [MemberData(nameof(BackupOutageCases))]
    [Catalog("OFF-019", "S", "INV-01", "INV-11", "INV-08")]
    public Task OFF_019_BackupOfflineDoesNotBlockLive(BackupOutage outage, BudgetExceeded budget) =>
        ClusterTest.RunAsync(Sim(3, backups: 1), async cluster =>
        {
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct);
            await using var own = client;
            TestBackupNode backup = cluster.BackupNodes[0];
            DfsBackupBudgets budgets = budget == BudgetExceeded.PinAge
                ? new DfsBackupBudgets(MaxPinAge: TimeSpan.FromSeconds(5), MaxPinnedBytes: 1L << 30)
                : new DfsBackupBudgets(MaxPinAge: TimeSpan.FromMinutes(10), MaxPinnedBytes: 256 * 1024);
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: true), budgets);
            await v.Directories.CreateDirectoryAsync("/o19", Ct);
            await v.Files.WriteAllBytesAsync("/o19/seed", V1, Ct);
            await v.Snapshots.CreateAsync("s-initial", null, Ct);
            await v.Backups.RequestSyncAsync(backup.TargetName, "s-initial", Ct);
            await v.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "s-initial", Bounded, Ct);
            Guid lineage0 = (await v.Backups.GetStatusAsync(backup.TargetName, Ct)).LineageId;

            switch (outage)
            {
                case BackupOutage.Stopped:
                    await backup.StopAsync();
                    break;
                case BackupOutage.Isolated:
                    cluster.Network.Isolate(backup.Name);
                    break;
                default:
                    await backup.PauseAsync();
                    break;
            }

            var written = new List<(string Path, byte[] Bytes)>();
            TimeSpan slowest = TimeSpan.Zero;
            string lastSnapshot = "s-initial";
            for (int i = 0; i < 40; i++)
            {
                string path = $"/o19/f{i % 10}";
                byte[] data = i % 5 == 0 ? TestData.Bytes(0x0B19_0000UL + (ulong)i, 128 * 1024) : TestData.Bytes(0x0B19_0000UL + (ulong)i, 300);
                long start = cluster.Time.GetTimestamp();
                await v.Files.WriteAllBytesAsync(path, data, Ct);
                TimeSpan elapsed = cluster.Time.GetElapsedTime(start);
                slowest = elapsed > slowest ? elapsed : slowest;
                written.RemoveAll(f => f.Path == path);
                written.Add((path, data));
                if (i % 8 == 7)
                {
                    lastSnapshot = $"s{i}";
                    await v.Snapshots.CreateAsync(lastSnapshot, null, Ct);
                }
            }

            Assert.True(slowest < TimeSpan.FromSeconds(2), $"live commits must not wait for the absent backup (slowest {slowest})");
            await AssertDataSetAsync(v, written, "live contents with the backup absent", Ct);

            // The backup never takes part in live consensus or membership.
            GroupInspection volumeGroup = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            GroupInspection controlGroup = await cluster.Inspect.GetControlGroupAsync();
            Assert.DoesNotContain(backup.Name, volumeGroup.Voters.Concat(volumeGroup.Learners));
            Assert.DoesNotContain(backup.Name, controlGroup.Voters.Concat(controlGroup.Learners));
            Assert.DoesNotContain((await cluster.Admin.GetClusterStatusAsync(Ct)).Nodes, n => n.Name == backup.Name);

            // Bounded history: once the budget is exceeded the lineage is abandoned and pins are released.
            if (budget == BudgetExceeded.PinAge)
                await cluster.DelayAsync(budgets.MaxPinAge + TimeSpan.FromSeconds(2), Ct);
            await cluster.WaitUntilAsync(async () => (await v.Backups.GetStatusAsync(backup.TargetName, Ct)).ReseedRequired,
                Bounded, "the source abandons the incremental lineage and marks the target for reseeding", Ct);
            DfsBackupStatus abandoned = await v.Backups.GetStatusAsync(backup.TargetName, Ct);
            Assert.True(abandoned.PinnedSourceBytes <= budgets.MaxPinnedBytes, $"pinned {abandoned.PinnedSourceBytes} bytes beyond the budget");
            await cluster.WaitUntilAsync(async () =>
                (await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.All(p => !string.Equals(p.Kind, "Transfer", StringComparison.OrdinalIgnoreCase)),
                Bounded, "transfer pins of the abandoned lineage are released", Ct);

            // Live progress continues after abandonment.
            await v.Files.WriteAllBytesAsync("/o19/after-abandon", V2, Ct);

            switch (outage)
            {
                case BackupOutage.Stopped:
                    await backup.RestartAsync();
                    break;
                case BackupOutage.Isolated:
                    cluster.Network.Heal();
                    break;
                default:
                    await backup.ResumeAsync();
                    break;
            }

            await v.Backups.RequestSyncAsync(backup.TargetName, lastSnapshot, Ct);
            DfsBackupCheckpoint reseeded = await v.Backups.AwaitBackupSnapshotAsync(backup.TargetName, lastSnapshot, Bounded, Ct);
            Assert.Equal(lastSnapshot, reseeded.SnapshotName);
            DfsBackupStatus after = await v.Backups.GetStatusAsync(backup.TargetName, Ct);
            Assert.False(after.ReseedRequired);
            Assert.NotEqual(lineage0, after.LineageId);
        }, Ct);

    // ================================================================================================================
    // OFF-020 NetworkHealsButDiskIsMissing
    // ================================================================================================================

    public static TheoryData<bool, int> EmptyDiskCases => new()
    {
        { false, 3 },
        { true, 3 },
        { false, 5 },
        { true, 5 },
    };

    /// <summary>
    /// OFF-020: a node returns with its device replaced by an empty disk. It is treated as missing state: it holds no
    /// verified copies, the deficit is exposed, it cannot complete a quorum in place of a lost healthy voter, and its
    /// endpoint answering is never counted as a healthy replica; after admission/repair the placement is restored.
    /// </summary>
    [Theory]
    [MemberData(nameof(EmptyDiskCases))]
    [Catalog("OFF-020", "S", "INV-11", "INV-18", "INV-01")]
    public Task OFF_020_NetworkHealsButDiskIsMissing(bool replacedNodeWasLeader, int nodeCount) =>
        ClusterTest.RunAsync(Sim(nodeCount), async cluster =>
        {
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            await using var own = client;
            IReadOnlyList<(string Path, byte[] Bytes)> files = await WriteDataSetAsync(v, "/o20", Ct);
            await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
            IReadOnlyList<ObjectId> objects = await GetFileObjectsAsync(cluster, "/o20/multi");
            IReadOnlyList<string> voters = await GetVolumeVotersAsync(cluster);

            TestNode target = replacedNodeWasLeader ? await cluster.GetLeaderAsync(Volume, Ct) : await GetFollowerAsync(cluster, Ct);
            var heldBefore = new Dictionary<ObjectId, bool>();
            foreach (ObjectId obj in objects)
                heldBefore[obj] = (await cluster.Inspect.GetObjectHoldersAsync(obj)).Contains(target.Name);

            await target.StopAsync();
            await target.ReplaceDiskWithEmptyAsync();
            OffAttempt<bool> rejoined = await TryAsync(() => target.RestartAsync());
            await cluster.WaitForStableAsync(Bounded, Ct);

            // Missing state: no verified copies on the empty disk; the endpoint answering does not count.
            foreach (ObjectId obj in objects)
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(obj);
                // Only objects the replacement really stores may be counted (repair may be copying concurrently, so only
                // this direction of the check is race-free).
                if (rejoined.Succeeded && holders.Contains(target.Name))
                    Assert.True(await target.Inspect.HasObjectAsync(obj), $"{obj}: the empty replacement is counted as a holder");
                if (heldBefore[obj] && !holders.Contains(target.Name))
                {
                    DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
                    Assert.True(health.UnderReplicatedObjects > 0 || (await cluster.Inspect.CountDurableDomainsAsync(obj)).Copies >= 3,
                        "a copy lost with the replaced disk must be reported as a deficit until repaired");
                }
            }

            await AssertDataSetAsync(v, files, "data intact with a replaced disk", Ct);

            // The empty member must not stand in for a lost healthy voter.
            TestNode[] healthyOthers = cluster.Nodes.Where(n => n.Name != target.Name && voters.Contains(n.Name)).ToArray();
            int toCrash = healthyOthers.Length - (voters.Count / 2);   // leaves healthy voters one short of a majority
            TestNode[] crashed = healthyOthers.Take(Math.Max(0, toCrash)).ToArray();
            foreach (TestNode node in crashed)
                await node.CrashAsync();
            var down = crashed.Select(n => n.Name).Append(target.Name).ToHashSet(StringComparer.Ordinal);
            Assert.False(HasConfiguredMajority(voters, down), "test precondition: healthy voters alone are one short of a majority");
            OffAttempt<CommitToken> write = await TryAsync(() => v.Files.WriteAllBytesAsync("/o20/needs-quorum", V2, Ct));
            if (write.Succeeded)
            {
                // Only acceptable if the replacement had been fully admitted and caught up before; it must then hold every
                // committed metadata record: each file still resolves to its exact committed content. Its data objects may be
                // held, at this moment, only by the voters just crashed (the copies lost with the replaced disk may still be
                // under repair — nothing above waited for it), so a read may fail as typed unavailability, never as absence or
                // other bytes; durability of every object is asserted below once the crashed voters are back.
                foreach ((string path, byte[] bytes) in files)
                {
                    OffAttempt<byte[]> read = await TryAsync(() => v.Files.ReadAllBytesAsync(path, Ct));
                    if (read.Succeeded)
                        AssertEx.BytesEqual(bytes, read.Value!, "no committed state lost");
                    else
                        Assert.True(read.Error is DfsUnavailableException and not DfsQuorumUnavailableException,
                            $"{path}: with a quorum serving, a read may only fail because its data holders are down, got {read}");
                }
            }

            foreach (TestNode node in crashed)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(Bounded, Ct);
            await AssertDataSetAsync(v, files, "after the healthy voters return", Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();

            if (rejoined.Succeeded || nodeCount > 3)
            {
                await cluster.WaitForRepairAsync(Volume, Bounded, Ct);
                foreach (ObjectId obj in objects)
                    Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(obj));
            }
            else
            {
                DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
                Assert.True(health.UnderReplicatedObjects > 0, "with the replacement not admitted, the deficit stays visible");
            }
        }, Ct);

    /// <summary>
    /// OFF-020 (vote safety): V2 is committed only on X and Y (Z partitioned). X's disk is then replaced, Y is down and Z
    /// (stale) returns. The empty X must not let Z form a quorum that loses V2: any successful read returns V2, and the
    /// final state after Y returns contains V2 (checked by a linearizability oracle).
    /// </summary>
    [Fact]
    [Catalog("OFF-020", "S", "INV-01", "INV-18")]
    public Task OFF_020_NetworkHealsButDiskIsMissing_CommittedStateOnlyOnReplacedDisk() =>
        ClusterTest.RunAsync(Sim(3), async cluster =>
        {
            const string path = "/o20/f";
            (DfsClient client, DfsVolume v) = await ConnectAsync(cluster, "c0", Ct, operationTimeout: ShortOperation);
            await using var own = client;
            var history = new HistoryRecorder();
            await v.Directories.CreateDirectoryAsync("/o20", Ct);
            await history.RecordAsync("c0", "write", path, V1, () => v.Files.WriteAllBytesAsync(path, V1, Ct));

            TestNode x = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode y = await GetFollowerAsync(cluster, Ct, 0);
            TestNode z = await GetFollowerAsync(cluster, Ct, 1);
            cluster.Network.Isolate(z.Name);
            await history.RecordAsync("c0", "write", path, V2, () => v.Files.WriteAllBytesAsync(path, V2, Ct));

            await y.CrashAsync();
            await x.CrashAsync();
            await x.ReplaceDiskWithEmptyAsync();
            cluster.Network.Heal();
            await TryAsync(() => x.RestartAsync());

            for (int i = 0; i < 4; i++)
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                await TryAsync(() => history.RecordAsync("c0", "read", path, null, () => v.Files.ReadAllBytesAsync(path, Ct)));
                await cluster.Inspect.AssertNoSplitBrainAsync();
            }

            await y.RestartAsync();
            await cluster.WaitForStableAsync(Bounded, Ct);
            byte[] final = await history.RecordAsync("c0", "read", path, null, () => v.Files.ReadAllBytesAsync(path, Ct));
            AssertEx.BytesEqual(V2, final, "the committed V2 survives");
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid();
        }, Ct);

    // ================================================================================================================
    // OFF-021 PauseExceedsElectionAndLeaseWindows
    // ================================================================================================================

    /// <summary>
    /// OFF-021 (paused metadata leader): the leader is paused longer than election and lease windows (and the session
    /// timeout) with a client request queued to it. A successor is elected; on resume the old leader's queued work cannot
    /// regain authority (never two leaders per term, it adopts the newer term) and the history stays linearizable.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(12)]
    [Catalog("OFF-021", "S", "INV-05", "INV-03", "INV-18")]
    public Task OFF_021_PauseExceedsElectionAndLeaseWindows(int pauseSeconds) =>
        ClusterTest.RunAsync(Sim(3), async cluster =>
        {
            const string path = "/o21/f";
            (DfsClient c, DfsVolume v) = await ConnectAsync(cluster, "c", Ct);
            await using var ownC = c;
            await v.Directories.CreateDirectoryAsync("/o21", Ct);
            var history = new HistoryRecorder();
            await history.RecordAsync("c", "write", path, V1, () => v.Files.WriteAllBytesAsync(path, V1, Ct));

            TestNode oldLeader = await cluster.GetLeaderAsync(Volume, Ct);
            ulong term0 = (await cluster.Inspect.GetVolumeGroupAsync(Volume)).Term;
            (DfsClient cl, DfsVolume vl) = await ConnectAsync(cluster, "cl", Ct, seeds: [oldLeader.Name], operationTimeout: TimeSpan.FromSeconds(pauseSeconds + 4));
            await using var ownL = cl;
            DfsOperationId kQueued = await vl.NewOperationIdAsync(Ct);

            await oldLeader.PauseAsync();
            long queuedOp = history.Invoke("cl", "write", path, Big1, kQueued);
            Task<CommitToken> queued = vl.Files.WriteAllBytesAsync(path, Big1, new DfsMutationOptions { OperationId = kQueued }, Ct);

            TestNode successor = await WaitForNewLeaderAsync(cluster, oldLeader.Name, term0, Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await history.RecordAsync("c", "write", path, V2, () => v.Files.WriteAllBytesAsync(path, V2, Ct));
            await cluster.DelayAsync(TimeSpan.FromSeconds(pauseSeconds), Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();

            await oldLeader.ResumeAsync();
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.WaitUntilAsync(async () => (await oldLeader.Inspect.GetGroupAsync(Volume))!.Term > term0,
                Bounded, "the resumed former leader adopts the successor's newer term", Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();

            OffAttempt<CommitToken> queuedResult = await TryAsync(() => queued);
            if (queuedResult.Succeeded)
            {
                history.Complete(queuedOp, OpOutcome.Ok, queuedResult.Value);
            }
            else
            {
                history.Complete(queuedOp, queuedResult.Classification, queuedResult.Error);
                DfsOperationOutcome resolved = await v.QueryOperationAsync(kQueued, resolve: true, Ct);
                if (queuedResult.Classification == OpOutcome.Unknown)
                    history.Resolve(queuedOp, resolved.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);
                else
                    Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
            }

            await history.RecordAsync("cl", "read", path, null, () => vl.Files.ReadAllBytesAsync(path, Ct));
            await history.RecordAsync("c", "read", path, null, () => v.Files.ReadAllBytesAsync(path, Ct));
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid();
            Assert.NotEqual(oldLeader.Name, successor.Name);
        }, Ct);

    public static TheoryData<bool, bool, bool> PausedWriterCases
    {
        get
        {
            var data = new TheoryData<bool, bool, bool>();
            foreach (bool replacementCommitsFirst in new[] { false, true })
            {
                foreach (bool leaderChange in new[] { false, true })
                {
                    foreach (bool reverse in new[] { false, true })
                        data.Add(replacementCommitsFirst, leaderChange, reverse);
                }
            }

            return data;
        }
    }

    /// <summary>
    /// OFF-021 / §9.3 (paused writer process): writer A (generation g, staged data) is paused with its memory and queued
    /// messages; only server time advances until A expires; writer B obtains a newer generation (committing before or
    /// after A resumes), optionally across a metadata leader change. A's delayed renewal, uploads and commit — delivered in
    /// order or reversed — are all rejected, A's stream faults, B stays the only valid writer, and A's orphan staging is
    /// eventually reclaimed.
    /// </summary>
    [Theory]
    [MemberData(nameof(PausedWriterCases))]
    [Catalog("OFF-021", "S", "INV-05", "INV-18")]
    public Task OFF_021_PauseExceedsElectionAndLeaseWindows_PausedWriterProcess(bool replacementCommitsBeforeResume, bool leaderChange, bool reverseRelease) =>
        ClusterTest.RunAsync(Sim(3), async cluster =>
        {
            const string path = "/o21/w";
            var clockA = new TestClock(cluster.Time);
            (DfsClient a, DfsVolume va) = await ConnectAsync(cluster, "A", Ct, operationTimeout: TimeSpan.FromSeconds(30), clock: clockA);
            (DfsClient b, DfsVolume vb) = await ConnectAsync(cluster, "B", Ct);
            (DfsClient r, DfsVolume vr) = await ConnectAsync(cluster, "R", Ct);
            await using var ownA = a;
            await using var ownB = b;
            await using var ownR = r;
            await va.Directories.CreateDirectoryAsync("/o21", Ct);
            await va.Files.WriteAllBytesAsync(path, V1, Ct);
            var writerOpen = new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read };

            DfsStream sa = await va.Files.OpenAsync(path, writerOpen, Ct);
            ulong generationA = sa.WriterGeneration;
            await sa.WriteAsync(TestData.Bytes(0x0921_0001, 200 * 1024), Ct);   // staged data (partly auto-uploaded)
            DfsOperationId ka = await va.NewOperationIdAsync(Ct);

            // Pause A's process: freeze its clock and hold every message it sends.
            MessageHold held = cluster.Network.Hold(new MessageFilter(From: ClientName("A")));
            clockA.Freeze();
            Task<CommitToken> aCommit = sa.CommitAsync(new DfsMutationOptions { OperationId = ka }, Ct);
            await cluster.DelayAsync(cluster.Options.Timings.LeaseDuration + TimeSpan.FromSeconds(1), Ct);

            if (leaderChange)
            {
                TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
                await leader.CrashAsync();
                await cluster.WaitForStableAsync(Bounded, Ct);
                await leader.RestartAsync();
                await cluster.WaitForStableAsync(Bounded, Ct);
            }

            DfsStream? sb = null;
            await cluster.WaitUntilAsync(async () =>
            {
                OffAttempt<DfsStream> open = await TryAsync(() => vb.Files.OpenAsync(path, writerOpen, Ct));
                sb = open.Value;
                return open.Succeeded;
            }, Bounded, "A's lease safely expires and B obtains the writer reservation", Ct);
            Assert.True(sb!.WriterGeneration > generationA, "B's fencing generation must exceed A's");
            await sb.WriteAsync(V2, Ct);
            await sb.SetLengthAsync(V2.Length, Ct);
            CommitToken? bToken = replacementCommitsBeforeResume ? await sb.CommitAsync(Ct) : null;

            // Resume A: deliver its delayed renewal, uploads and commit (in order or reversed).
            clockA.Unfreeze();
            held.ReleaseAll(reverseRelease);
            held.Dispose();
            OffAttempt<CommitToken> aResult = await TryAsync(() => aCommit);
            Assert.False(aResult.Succeeded, "the obsolete writer generation must not publish");
            Assert.True(aResult.Error is DfsLeaseLostException || aResult.Classification == OpOutcome.Unknown,
                $"expected lease loss (or an unknown outcome resolving to abort), got {aResult.Error}");
            Assert.True(sa.IsFaulted, "the stale writer's stream must be faulted");
            await Assert.ThrowsAnyAsync<Exception>(() => sa.WriteAsync(V1, Ct).AsTask());
            Assert.NotEqual(DfsOutcomeStatus.Committed, (await vr.QueryOperationAsync(ka, resolve: true, Ct)).Status);

            // B remains the only valid writer.
            bToken ??= await sb.CommitAsync(Ct);
            await sb.DisposeAsync();
            await ExpectContentAsync(V2, vr.Files.ReadAllBytesAsync(path, Ct), "B's commit is authoritative");
            Assert.Equal(bToken.Value.Sequence, (await vr.Files.GetInfoAsync(path, Ct)).Revision);

            await TryAsync(() => sa.DisposeAsync().AsTask());
            await cluster.WaitUntilAsync(async () => await GetStagedBytesAsync(cluster) == 0, Bounded,
                "A's orphan staging is reclaimed once safely abandoned", Ct);
        }, Ct);

    // ================================================================================================================
    // OFF-022 OfflinePastCredentialExpiry
    // ================================================================================================================

    public enum CredentialReturn
    {
        ClientSessionExpiredSameCredentials,
        ClientCredentialsRotatedWhileOffline,
        ClientPermissionsRevokedWhileOffline,
        NodeCrashedPastSessionExpiry,
        NodeReturnsWithWrongClusterSecret,
    }

    public static TheoryData<CredentialReturn> CredentialReturns => new(Enum.GetValues<CredentialReturn>());

    /// <summary>
    /// OFF-022: a client or node returns after its session/credential state expired or changed. Clients must
    /// re-establish (and pass) authentication and a new session before acting — stale handles and old credentials are
    /// rejected; nodes must re-register a new incarnation and catch up before their state counts — a node with wrong
    /// cluster credentials is never counted toward quorum; cluster identity is unchanged throughout.
    /// </summary>
    [Theory]
    [MemberData(nameof(CredentialReturns))]
    [Catalog("OFF-022", "S", "INV-08", "INV-18", "INV-05")]
    public Task OFF_022_OfflinePastCredentialExpiry(CredentialReturn scenario) =>
        ClusterTest.RunAsync(Sim(3), async cluster =>
        {
            const string path = "/o22/f";
            NodeTimings t = cluster.Options.Timings;
            (DfsClient admin, DfsVolume av) = await ConnectAsync(cluster, "admin-c", Ct, operationTimeout: ShortOperation);
            await using var ownAdmin = admin;
            await av.Directories.CreateDirectoryAsync("/o22", Ct);
            await av.Files.WriteAllBytesAsync(path, V1, Ct);

            if (scenario is CredentialReturn.NodeCrashedPastSessionExpiry or CredentialReturn.NodeReturnsWithWrongClusterSecret)
            {
                TestNode follower = await GetFollowerAsync(cluster, Ct, 0);
                TestNode other = await GetFollowerAsync(cluster, Ct, 1);
                ulong incarnation0 = follower.Incarnation;
                if (scenario == CredentialReturn.NodeCrashedPastSessionExpiry)
                {
                    await follower.CrashAsync();
                    await av.Files.WriteAllBytesAsync(path, V2, Ct);
                    await cluster.DelayAsync(t.SessionTimeout + TimeSpan.FromSeconds(5), Ct);
                    await follower.RestartAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    Assert.True(follower.Incarnation > incarnation0, "a restarted node must register a new incarnation");
                    // The re-registration is a control-group commit: wait (bounded) until the cluster map reflects it.
                    await cluster.WaitUntilAsync(async () =>
                        (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == follower.Name).Incarnation == follower.Incarnation,
                        Bounded, "the restarted node's new incarnation is registered with the control group", Ct);
                    DfsNodeInfo registered = (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == follower.Name);
                    Assert.Equal(follower.Incarnation, registered.Incarnation);
                    Assert.Equal(DfsNodeStatus.Active, registered.Status);
                    await cluster.WaitForReplicationAsync(Volume, Ct);
                    await cluster.Inspect.AssertReplicasConvergedAsync(Volume);

                    // Only now is it trusted as part of a quorum: with the other follower down it serves the newest state.
                    await other.StopAsync();
                    await ExpectContentAsync(V2, av.Files.ReadAllBytesAsync(path, Ct), "the caught-up node serves current state");
                    await av.Files.WriteAllBytesAsync(path, Big1, Ct);
                    await other.RestartAsync();
                }
                else
                {
                    await follower.StopAsync();
                    byte[] wrongSecret = TestData.Bytes(0xBAD5_EC2E, 32);
                    ReadOnlyMemory<byte> legitimateSecret = default;
                    await TryAsync(() => follower.RestartAsync(o =>
                    {
                        legitimateSecret = o.ClusterSecret;
                        return o with { ClusterSecret = wrongSecret };
                    }));
                    await other.CrashAsync();   // only the leader and the impostor remain
                    OffAttempt<CommitToken> write = await TryAsync(() => av.Files.WriteAllBytesAsync(path, V2, Ct));
                    Assert.False(write.Succeeded, "a node with wrong cluster credentials must never complete a quorum");
                    Assert.True(IsNoSuccessFailure(write.Error!), $"{write.Error}");
                    OffAttempt<byte[]> read = await TryAsync(() => av.Files.ReadAllBytesAsync(path, Ct));
                    Assert.False(read.Succeeded, "no live read without a legitimate majority");
                    await other.RestartAsync();
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    if (follower.State == TestNodeState.Running)
                        await follower.StopAsync();

                    // The node returns with its legitimate credentials (a reconfigured restart keeps its options).
                    await follower.RestartAsync(o => o with { ClusterSecret = legitimateSecret });
                    await cluster.WaitForStableAsync(Bounded, Ct);
                    await cluster.WaitForReplicationAsync(Volume, Ct);
                    await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
                }

                Assert.Equal(cluster.ClusterId, admin.Cluster.ClusterId);
                await cluster.Inspect.AssertNoSplitBrainAsync();
                return;
            }

            TestPrincipal alice = await cluster.CreatePrincipalAsync("alice",
                new Dictionary<string, DfsPermissions>(StringComparer.Ordinal) { [Volume] = DfsPermissions.Read | DfsPermissions.Write });
            (DfsClient ca, DfsVolume va) = await ConnectAsync(cluster, "alice", Ct, operationTimeout: ShortOperation, credentials: alice.Credentials);
            await using var ownAlice = ca;
            await va.Files.WriteAllBytesAsync(path, V2, Ct);
            DfsStream staleHandle = await va.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read }, Ct);
            await staleHandle.WriteAsync(Big1, Ct);
            var sessionsFilter = new MessageFilter(From: ClientName("alice"), Type: MessageType.OpenSession, Direction: LinkDirection.Request);
            long sessionsBefore = cluster.Network.Log.CountMessages(sessionsFilter);

            cluster.Network.Isolate(ClientName("alice"));
            const string newSecret = "rotated-secret-0ff022";
            switch (scenario)
            {
                case CredentialReturn.ClientCredentialsRotatedWhileOffline:
                    await cluster.Admin.DeletePrincipalAsync("alice", Ct);
                    await cluster.Admin.CreatePrincipalAsync("alice", newSecret, clusterAdmin: false, Ct);
                    await cluster.Admin.GrantAsync(Volume, "alice", DfsPermissions.Read | DfsPermissions.Write, Ct);
                    break;
                case CredentialReturn.ClientPermissionsRevokedWhileOffline:
                    await cluster.Admin.RevokeAsync(Volume, "alice", DfsPermissions.Write, Ct);
                    break;
            }

            await cluster.DelayAsync(t.SessionTimeout + TimeSpan.FromSeconds(5), Ct);
            await cluster.WaitUntilAsync(async () =>
                (await cluster.Inspect.GetVolumeStateAsync(Volume)).Sessions.Where(s => s.Principal == "alice").All(s => s.Expired),
                Bounded, "the offline client's sessions expire server-side", Ct);
            ulong[] expiredSessions = (await cluster.Inspect.GetVolumeStateAsync(Volume)).Sessions.Where(s => s.Principal == "alice").Select(s => s.Id).ToArray();
            cluster.Network.Heal();

            // The stale handle never publishes.
            OffAttempt<CommitToken> stale = await TryAsync(() => staleHandle.CommitAsync(Ct));
            Assert.False(stale.Succeeded, "a handle of an expired session must not publish");
            await ExpectContentAsync(V2, av.Files.ReadAllBytesAsync(path, Ct), "stale handle had no effect");
            await TryAsync(() => staleHandle.DisposeAsync().AsTask());

            switch (scenario)
            {
                case CredentialReturn.ClientSessionExpiredSameCredentials:
                {
                    await va.Files.WriteAllBytesAsync(path, V1, Ct);
                    Assert.True(cluster.Network.Log.CountMessages(sessionsFilter) > sessionsBefore, "a new session must be established before acting");
                    VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
                    Assert.Contains(state.Sessions, s => s.Principal == "alice" && !s.Expired && !expiredSessions.Contains(s.Id));
                    await ExpectContentAsync(V1, av.Files.ReadAllBytesAsync(path, Ct), "write in the new session");
                    break;
                }

                case CredentialReturn.ClientCredentialsRotatedWhileOffline:
                {
                    OffAttempt<CommitToken> old = await TryAsync(() => va.Files.WriteAllBytesAsync(path, V1, Ct));
                    Assert.False(old.Succeeded, "old credentials must not be accepted after rotation");
                    Assert.IsAssignableFrom<UnauthorizedAccessException>(old.Error);
                    await ExpectContentAsync(V2, av.Files.ReadAllBytesAsync(path, Ct), "rejected client had no effect");
                    (DfsClient fresh, DfsVolume fv) = await ConnectAsync(cluster, "alice2", Ct, credentials: new DfsCredentials("alice", newSecret));
                    await using var ownFresh = fresh;
                    await fv.Files.WriteAllBytesAsync(path, V1, Ct);
                    await ExpectContentAsync(V1, av.Files.ReadAllBytesAsync(path, Ct), "rotated credentials work");
                    break;
                }

                default:
                {
                    OffAttempt<CommitToken> denied = await TryAsync(() => va.Files.WriteAllBytesAsync(path, V1, Ct));
                    Assert.False(denied.Succeeded, "revoked write permission must be enforced on return");
                    Assert.IsAssignableFrom<UnauthorizedAccessException>(denied.Error);
                    await ExpectContentAsync(V2, va.Files.ReadAllBytesAsync(path, Ct), "read permission still applies");
                    break;
                }
            }

            Assert.Equal(cluster.ClusterId, ca.Cluster.ClusterId);
        }, Ct);
}
