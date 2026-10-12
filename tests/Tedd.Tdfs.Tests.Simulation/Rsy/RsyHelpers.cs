using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Rsy;

/// <summary>How a replica is taken offline for a resynchronization scenario.</summary>
public enum OfflineMode
{
    /// <summary>Graceful stop; returns with <see cref="TestNode.RestartAsync"/> from its durable image.</summary>
    Stop,

    /// <summary>Abrupt crash losing unflushed writes; returns with <see cref="TestNode.RestartAsync"/>.</summary>
    Crash,

    /// <summary>Process keeps running but every link is cut; returns when the network heals.</summary>
    Isolate,
}

/// <summary>Which catch-up path the returning replica is forced onto.</summary>
public enum CatchupPath
{
    /// <summary>The missing range is still in the sources' logs: incremental log replay (RCV-03, first sentence).</summary>
    Log,

    /// <summary>The missing range was compacted on every source: checkpoint plus suffix (RCV-03, second sentence).</summary>
    Checkpoint,
}

/// <summary>The three roles of the A/B/C scenarios of test plan §9.4.</summary>
/// <param name="Leader">"A": the volume leader when the scenario started.</param>
/// <param name="Other">"B": the remaining follower that stays online (first one for clusters larger than three).</param>
/// <param name="Returning">"C": the follower that goes offline and later catches up.</param>
internal sealed record RsyRoles(TestNode Leader, TestNode Other, TestNode Returning);

/// <summary>State recorded when the returning replica went offline.</summary>
internal sealed record RsyOutage(RsyRoles Roles, OfflineMode Mode, ulong ReturningLastIndex, ulong ReturningAppliedIndex,
    ulong ReturningSnapshotIndex, RaftGroupStats StatsBefore);

/// <summary>Shared scenario helpers for the RSY suite.</summary>
internal static class RsyHelpers
{
    /// <summary>The volume every RSY scenario uses.</summary>
    public const string Volume = "vol";

    /// <summary><see cref="NodeTimings.Fast"/> logical update unit (64 KiB) — extent boundaries in the scenarios.</summary>
    public const int Chunk = 64 << 10;

    /// <summary>Default inline threshold of test volumes.</summary>
    public const int Inline = 4096;

    /// <summary>Bound for a returning replica to catch up once its prerequisites are restored (cluster time).</summary>
    public static readonly TimeSpan CatchupTimeout = TimeSpan.FromSeconds(60);

    /// <summary>Bound for background repair/anti-entropy to converge once healthy sources exist (cluster time).</summary>
    public static readonly TimeSpan RepairTimeout = TimeSpan.FromSeconds(60);

    /// <summary>The test cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Fast timings used by every RSY scenario (lease 2 s, session 10 s, Raft snapshot threshold 200 entries).</summary>
    public static NodeTimings Timings => NodeTimings.Fast;

    /// <summary>
    /// Deterministic-simulation options: 3 nodes one per zone (target 3 / ack 2 in 2 zones), 5 nodes one per zone with five
    /// voters, or <paramref name="nodeCount"/> nodes with min(n, 5) voters.
    /// </summary>
    public static TestClusterOptions Sim(int nodeCount = 3)
    {
        TestClusterOptions baseOptions = nodeCount switch
        {
            3 => TestClusterOptions.ThreeZones,
            5 => TestClusterOptions.FiveNodes,
            _ => new TestClusterOptions
            {
                NodeCount = nodeCount,
                ControlVoters = Math.Min(nodeCount, 5),
                Volumes = [new TestVolumeSpec(Volume) { Metadata = new DfsMetadataPolicy { Voters = Math.Min(nodeCount, 5) } }],
            },
        };
        return baseOptions with
        {
            Mode = ClusterMode.Deterministic,
            ScenarioTimeout = TimeSpan.FromMinutes(15),
        };
    }

    /// <summary>Runs a scenario with the test cancellation token.</summary>
    public static Task RunAsync(TestClusterOptions options, Func<TestCluster, Task> scenario) =>
        ClusterTest.RunAsync(options, scenario, Ct);

    /// <summary>Connects a named client (optionally restricted to some seed nodes) and returns its volume.</summary>
    public static async Task<(DfsClient Client, DfsVolume Volume)> ConnectAsync(TestCluster cluster, string name,
        IReadOnlyList<string>? seeds = null, TestClock? clock = null, TimeSpan? operationTimeout = null)
    {
        DfsClient client = await cluster.ConnectAsync(new TestClientOptions
        {
            Name = name,
            SeedNodes = seeds,
            Clock = clock,
            OperationTimeout = operationTimeout,
        }, Ct);
        return (client, client.GetVolume(Volume));
    }

    /// <summary>Picks A (volume leader), C (a follower, preferably not the control leader) and B (another follower).</summary>
    public static async Task<RsyRoles> PickRolesAsync(TestCluster cluster)
    {
        TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
        TestNode control = await cluster.GetControlLeaderAsync(Ct);
        var followers = cluster.Nodes.Where(n => n.Name != leader.Name && n.State == TestNodeState.Running).ToList();
        Assert.True(followers.Count >= 2, "RSY scenarios need at least two running followers.");
        TestNode returning = followers.FirstOrDefault(n => n.Name != control.Name) ?? followers[0];
        TestNode other = followers.First(n => n.Name != returning.Name);
        return new RsyRoles(leader, other, returning);
    }

    /// <summary>Running nodes other than <paramref name="except"/>.</summary>
    public static IReadOnlyList<TestNode> RunningExcept(TestCluster cluster, TestNode except) =>
        cluster.Nodes.Where(n => n.Name != except.Name && n.State == TestNodeState.Running).ToList();

    /// <summary>The node's inspection of a Raft group (volume group by default).</summary>
    public static async Task<GroupInspection> GroupAsync(TestNode node, string group = Volume)
    {
        GroupInspection? g = await node.Inspect.GetGroupAsync(group);
        Assert.True(g is not null, $"{node.Name} does not host group '{group}'.");
        return g!;
    }

    /// <summary>The node's catch-up statistics for a Raft group (volume group by default).</summary>
    public static async Task<RaftGroupStats> StatsAsync(TestNode node, string group = Volume)
    {
        RaftGroupStats? s = await node.Inspect.GetRaftGroupStatsAsync(group);
        Assert.True(s is not null, $"{node.Name} has no statistics for group '{group}'.");
        return s!;
    }

    /// <summary>The node's applied volume state.</summary>
    public static async Task<VolumeStateInspection> StateAsync(TestNode node)
    {
        VolumeStateInspection? s = await node.Inspect.GetVolumeStateAsync(Volume);
        Assert.True(s is not null, $"{node.Name} has no applied state for volume '{Volume}'.");
        return s!;
    }

    /// <summary>The leader's applied volume state.</summary>
    public static Task<VolumeStateInspection> LeaderStateAsync(TestCluster cluster) => cluster.Inspect.GetVolumeStateAsync(Volume);

    /// <summary>Takes a node offline.</summary>
    public static Task TakeOfflineAsync(TestCluster cluster, TestNode node, OfflineMode mode)
    {
        switch (mode)
        {
            case OfflineMode.Stop:
                return node.StopAsync();
            case OfflineMode.Crash:
                return node.CrashAsync(Tedd.Tdfs.Storage.CrashMode.LoseUnflushed);
            case OfflineMode.Isolate:
                cluster.Network.Isolate(node.Name);
                return Task.CompletedTask;
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }
    }

    /// <summary>Returns an offline node (restart from its durable image, or heal the network).</summary>
    public static Task ReturnAsync(TestCluster cluster, TestNode node, OfflineMode mode)
    {
        if (mode == OfflineMode.Isolate)
        {
            cluster.Network.Heal();
            return Task.CompletedTask;
        }

        return node.RestartAsync();
    }

    /// <summary>Records the returning node's progress and takes it offline.</summary>
    public static async Task<RsyOutage> BeginOutageAsync(TestCluster cluster, OfflineMode mode, RsyRoles? roles = null)
    {
        await cluster.WaitForReplicationAsync(Volume, Ct);
        roles ??= await PickRolesAsync(cluster);
        GroupInspection g = await GroupAsync(roles.Returning);
        RaftGroupStats stats = await StatsAsync(roles.Returning);
        await TakeOfflineAsync(cluster, roles.Returning, mode);
        return new RsyOutage(roles, mode, g.LastIndex, g.AppliedIndex, g.SnapshotIndex, stats);
    }

    /// <summary>
    /// Forces (Checkpoint) or rules out (Log) compaction of the returning node's missing range on every online source,
    /// returns the node and waits until it applied the leader's commit index; then asserts which catch-up path it took.
    /// </summary>
    public static async Task EndOutageAsync(TestCluster cluster, RsyOutage outage, CatchupPath path, DfsVolume volume, string fillDirectory, ulong seed)
    {
        TestNode returning = outage.Roles.Returning;
        IReadOnlyList<TestNode> sources = RunningExcept(cluster, returning);
        if (path == CatchupPath.Checkpoint)
            await ForceCompactionAsync(cluster, volume, sources, outage.ReturningLastIndex, fillDirectory, seed);
        else
            await AssertMissingRangeRetainedAsync(sources, outage.ReturningLastIndex);

        await ReturnAsync(cluster, returning, outage.Mode);
        await WaitCaughtUpAsync(cluster, returning);
        await AssertCatchupPathAsync(cluster, returning, path, outage.Mode == OfflineMode.Isolate ? outage.StatsBefore : null);
    }

    /// <summary>
    /// Commits more than <see cref="NodeTimings.RaftSnapshotThreshold"/> (200 with Fast timings) small inline writes while
    /// the returning node is down, and keeps writing until every source has compacted its log strictly beyond
    /// <paramref name="pastIndex"/> (so index <c>pastIndex + 1</c> is only available inside a checkpoint). Fails when
    /// compaction does not happen within ten times the threshold. Returns the number of files written.
    /// </summary>
    public static async Task<int> ForceCompactionAsync(TestCluster cluster, DfsVolume volume, IReadOnlyList<TestNode> sources,
        ulong pastIndex, string directory, ulong seed)
    {
        int threshold = cluster.Options.Timings.RaftSnapshotThreshold;
        int maxWrites = threshold * 10;
        const int batchSize = 25;
        await volume.Directories.CreateDirectoryAsync(directory, Ct);
        int written = 0;
        while (true)
        {
            bool compacted = true;
            foreach (TestNode source in sources)
            {
                GroupInspection g = await GroupAsync(source);
                if (g.SnapshotIndex <= pastIndex)
                {
                    compacted = false;
                    break;
                }
            }

            if (compacted && written > threshold)
                return written;
            Assert.True(written < maxWrites,
                $"Sources [{string.Join(", ", sources.Select(s => s.Name))}] did not compact their logs beyond index {pastIndex} after {written} " +
                $"committed writes (RaftSnapshotThreshold = {threshold}); log compaction must be bounded by the threshold.");

            var batch = new Task<CommitToken>[batchSize];
            for (int i = 0; i < batchSize; i++)
            {
                int n = written + i;
                batch[i] = volume.Files.WriteAllBytesAsync($"{directory}/f{n:D5}", TestData.Bytes(seed + (ulong)n, 48), Ct);
            }

            CommitToken[] tokens = await Task.WhenAll(batch);
            Assert.All(tokens, t => Assert.False(t.IsNone));
            written += batchSize;
        }
    }

    /// <summary>Precondition of log-path tests: no source compacted the returning node's missing range.</summary>
    public static async Task AssertMissingRangeRetainedAsync(IEnumerable<TestNode> sources, ulong returningLastIndex)
    {
        foreach (TestNode source in sources)
        {
            GroupInspection g = await GroupAsync(source);
            Assert.True(g.SnapshotIndex <= returningLastIndex,
                $"Precondition: {source.Name} compacted its log to {g.SnapshotIndex}, beyond the returning node's last index {returningLastIndex}; " +
                "the incremental (log) catch-up path cannot be exercised by this scenario.");
        }
    }

    /// <summary>Waits (bounded) until <paramref name="node"/> is running and applied the volume leader's current commit index.</summary>
    public static async Task WaitCaughtUpAsync(TestCluster cluster, TestNode node, TimeSpan? timeout = null)
    {
        TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
        ulong target = (await GroupAsync(leader)).CommitIndex;
        await cluster.WaitUntilAsync(async () =>
        {
            if (node.State != TestNodeState.Running)
                return false;
            GroupInspection? g = await node.Inspect.GetGroupAsync(Volume);
            return g is not null && g.AppliedIndex >= target;
        }, timeout ?? CatchupTimeout, $"{node.Name} must apply volume '{Volume}' up to the leader's commit index {target} once it is back", Ct);
    }

    /// <summary>
    /// Asserts the catch-up path a node took and that every log index reached its state machine exactly once (individually
    /// or through exactly one installed checkpoint — no skipped, duplicated or invented entries).
    /// </summary>
    /// <param name="cluster">Cluster.</param>
    /// <param name="node">The returning node.</param>
    /// <param name="path">Expected path.</param>
    /// <param name="before">Statistics taken before the outage when the node kept running (isolation); null after a restart.</param>
    public static async Task AssertCatchupPathAsync(TestCluster cluster, TestNode node, CatchupPath path, RaftGroupStats? before)
    {
        RaftGroupStats after = await StatsAsync(node);
        Assert.Equal((long)(after.AppliedIndex - after.StartIndex), after.EntriesApplied + after.EntriesCoveredByInstall);
        long installs = after.SnapshotsInstalled - (before?.SnapshotsInstalled ?? 0);
        bool installHit = cluster.Faults.Coverage.Hits.Any(h =>
            h.Site == FaultSites.RaftSnapshotInstall && h.Node == node.Name && h.Volume == Volume);
        if (path == CatchupPath.Checkpoint)
        {
            Assert.True(installs >= 1, $"{node.Name} must install a peer checkpoint because its missing range was compacted (installed {installs}).");
            Assert.True(installHit, $"The checkpoint-install path (site {FaultSites.RaftSnapshotInstall}) was not reached on {node.Name}.");
            Assert.True(after.EntriesCoveredByInstall - (before?.EntriesCoveredByInstall ?? 0) > 0,
                "An installed checkpoint must cover the compacted range instead of replaying invented entries.");
        }
        else
        {
            Assert.Equal(0, installs);
            Assert.False(installHit, $"{node.Name} must catch up by incremental log replay while its missing range is still in the log, not by checkpoint install.");
            if (before is not null)
            {
                Assert.Equal((long)(after.AppliedIndex - before.AppliedIndex), after.EntriesApplied - before.EntriesApplied);
                Assert.Equal(before.EntriesCoveredByInstall, after.EntriesCoveredByInstall);
            }
            else
            {
                Assert.Equal(0, after.EntriesCoveredByInstall);
            }
        }
    }

    /// <summary>Compares two file inspections field by field (null means "no such path").</summary>
    public static void AssertSameFile(FileInspection? expected, FileInspection? actual, string because)
    {
        if (expected is null)
        {
            Assert.True(actual is null, $"{because}: expected no file, found inode {actual?.Inode} at {actual?.Path}.");
            return;
        }

        Assert.True(actual is not null, $"{because}: file {expected.Path} (inode {expected.Inode}) is missing.");
        Assert.True(expected.Inode == actual!.Inode, $"{because}: {expected.Path} inode {actual.Inode} != authoritative {expected.Inode}.");
        Assert.True(expected.Revision == actual.Revision, $"{because}: {expected.Path} revision {actual.Revision} != authoritative {expected.Revision}.");
        Assert.True(expected.Length == actual.Length, $"{because}: {expected.Path} length {actual.Length} != authoritative {expected.Length}.");
        Assert.True(expected.IsInline == actual.IsInline, $"{because}: {expected.Path} inline flag differs.");
        Assert.True(expected.WriterGeneration == actual.WriterGeneration,
            $"{because}: {expected.Path} writer generation {actual.WriterGeneration} != authoritative {expected.WriterGeneration}.");
        Assert.True(expected.Policy == actual.Policy, $"{because}: {expected.Path} policy differs.");
        Assert.Equal(expected.Extents, actual.Extents);
    }

    /// <summary>True when two inspections describe the same inode version (null-safe).</summary>
    public static bool SameFile(FileInspection? x, FileInspection? y) =>
        x is null || y is null
            ? x is null && y is null
            : x.Inode == y.Inode && x.Revision == y.Revision && x.Length == y.Length && x.Extents.SequenceEqual(y.Extents);

    /// <summary>
    /// Waits for replication and asserts the node's applied state equals the leader's: replica digests converged and every
    /// listed path resolves to the same inode version (or to nothing) on both.
    /// </summary>
    public static async Task AssertMatchesLeaderAsync(TestCluster cluster, TestNode node, IEnumerable<string> paths)
    {
        await cluster.WaitForReplicationAsync(Volume, Ct);
        await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
        VolumeStateInspection leader = await LeaderStateAsync(cluster);
        VolumeStateInspection mine = await StateAsync(node);
        Assert.Equal(leader.InodeCount, mine.InodeCount);
        foreach (string path in paths)
            AssertSameFile(leader.GetFile(path), mine.GetFile(path), $"{node.Name} after catch-up");
    }

    /// <summary>Makes <paramref name="node"/> the volume leader so the following client calls are served by it.</summary>
    public static async Task ServeFromAsync(TestCluster cluster, TestNode node)
    {
        TestNode leader = await cluster.TransferLeadershipAsync(Volume, node.Name, TimeSpan.FromSeconds(20), Ct);
        Assert.Equal(node.Name, leader.Name);
        Assert.Equal(node.Name, (await cluster.GetLeaderAsync(Volume, Ct)).Name);
    }

    /// <summary>Distinct objects referenced by a file's authoritative extents (empty for inline files).</summary>
    public static async Task<IReadOnlyList<ObjectId>> ObjectsOfAsync(TestCluster cluster, string path)
    {
        FileInspection? f = await cluster.Inspect.GetFileAsync(Volume, path);
        Assert.True(f is not null, $"{path} must exist.");
        return f!.Extents.Select(e => e.Object).Distinct().ToList();
    }

    /// <summary>Creates a directory (recursive, idempotent).</summary>
    public static Task MkdirAsync(DfsVolume volume, string path) => volume.Directories.CreateDirectoryAsync(path, Ct);

    /// <summary>Overwrites a range through a writable stream and publishes it as one revision.</summary>
    public static async Task<CommitToken> OverwriteAsync(DfsFileClient files, string path, long offset, byte[] data)
    {
        await using DfsStream s = await files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
        s.Seek(offset, SeekOrigin.Begin);
        await s.WriteAsync(data, Ct);
        CommitToken token = await s.CommitAsync(Ct);
        Assert.False(token.IsNone);
        return token;
    }

    /// <summary>Changes a file's length through a writable stream and publishes it as one revision.</summary>
    public static async Task<CommitToken> ResizeAsync(DfsFileClient files, string path, long length)
    {
        await using DfsStream s = await files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
        await s.SetLengthAsync(length, Ct);
        CommitToken token = await s.CommitAsync(Ct);
        Assert.False(token.IsNone);
        return token;
    }

    /// <summary>Reads a whole file (one pinned revision) and compares it byte for byte.</summary>
    public static async Task AssertBytesAsync(DfsFileClient files, string path, byte[] expected, string? because = null)
    {
        byte[] actual = await files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(expected, actual, because ?? path);
    }

    /// <summary>Reads a range through a live read-only stream.</summary>
    public static async Task<byte[]> ReadRangeAsync(DfsFileClient files, string path, long offset, int count)
    {
        await using DfsStream s = await files.OpenReadAsync(path, Ct);
        s.Seek(offset, SeekOrigin.Begin);
        var buffer = new byte[count];
        int total = 0;
        while (total < count)
        {
            int n = await s.ReadAsync(buffer.AsMemory(total), Ct);
            if (n == 0)
                break;
            total += n;
        }

        return buffer.AsSpan(0, total).ToArray();
    }

    /// <summary>Concatenates byte arrays.</summary>
    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int offset = 0;
        foreach (byte[] p in parts)
        {
            p.CopyTo(result, offset);
            offset += p.Length;
        }

        return result;
    }

    /// <summary>
    /// True for failures a client may legitimately observe while authority/data is unavailable (never for wrong data):
    /// typed unavailability, timeouts, not-leader/quorum/node-unavailable codes and outcome-unknown on reads.
    /// </summary>
    public static bool IsAvailabilityFailure(Exception ex) => ex switch
    {
        DfsUnavailableException => true,
        TimeoutException => true,
        DfsException { ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.QuorumUnavailable or DfsErrorCode.NodeUnavailable
            or DfsErrorCode.Unavailable or DfsErrorCode.Timeout or DfsErrorCode.OutcomeUnknown } => true,
        _ => false,
    };

    /// <summary>3 copies in 3 zones, all required before acknowledgement.</summary>
    public static DfsStoragePolicy Strict3 => new()
    {
        TargetCopies = 3,
        TargetDistinctZones = 3,
        DurableCopiesBeforeAck = 3,
        DurableZonesBeforeAck = 3,
    };

    /// <summary>A single copy.</summary>
    public static DfsStoragePolicy Single1 => new()
    {
        TargetCopies = 1,
        TargetDistinctZones = 1,
        DurableCopiesBeforeAck = 1,
        DurableZonesBeforeAck = 1,
    };

    /// <summary>The scenario volume with the named policies "strict3" and "single1".</summary>
    public static TestVolumeSpec VolumeWithPolicies(DfsMetadataPolicy? metadata = null) => new(Volume)
    {
        Metadata = metadata ?? new DfsMetadataPolicy(),
        NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal) { ["strict3"] = Strict3, ["single1"] = Single1 },
    };

    /// <summary>A copy of a frame with one byte flipped.</summary>
    public static byte[] Flip(ReadOnlyMemory<byte> frame, int offset)
    {
        byte[] copy = frame.ToArray();
        copy[offset] ^= 0x5A;
        return copy;
    }

    /// <summary>True when <paramref name="node"/> stores every object.</summary>
    public static async Task<bool> HoldsAllAsync(TestNode node, IEnumerable<ObjectId> objects)
    {
        foreach (ObjectId o in objects)
        {
            if (!await node.Inspect.HasObjectAsync(o))
                return false;
        }

        return true;
    }

    /// <summary>True when the harness sees a verified copy of every object on <paramref name="nodeName"/>.</summary>
    public static async Task<bool> VerifiedOnAsync(TestCluster cluster, string nodeName, IEnumerable<ObjectId> objects)
    {
        foreach (ObjectId o in objects)
        {
            if (!(await cluster.Inspect.GetObjectHoldersAsync(o)).Contains(nodeName))
                return false;
        }

        return true;
    }

    /// <summary>Silently deletes the node's copies of the objects.</summary>
    public static async Task DeleteCopiesAsync(TestNode node, IEnumerable<ObjectId> objects)
    {
        foreach (ObjectId o in objects)
            await node.Disk.DeleteObjectAsync(o);
    }

    /// <summary>
    /// A committed transaction/operation queried later reports Committed with its original token, or Expired once its
    /// outcome record was legitimately retired — never Aborted, Pending or NotFound.
    /// </summary>
    public static void AssertCommittedOrRetired(DfsOperationOutcome outcome, CommitToken expected)
    {
        Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Expired,
            $"a committed decision must never be reported as {outcome.Status}");
        if (outcome.Status == DfsOutcomeStatus.Committed)
            Assert.Equal(expected, outcome.Token);
    }

    /// <summary>Writes a line to the test output.</summary>
    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>Hits recorded at a fault site on a node (all actions, armed or not).</summary>
    public static int HitCount(TestCluster cluster, string site, string node, string? detail = null) =>
        cluster.Faults.Coverage.Hits.Count(h => h.Site == site && h.Node == node && (detail is null || h.Detail == detail));

    /// <summary>Replica holders of every object, keyed by object.</summary>
    public static async Task<Dictionary<ObjectId, string[]>> HoldersAsync(TestCluster cluster, IEnumerable<ObjectId> objects)
    {
        var result = new Dictionary<ObjectId, string[]>();
        foreach (ObjectId o in objects)
            result[o] = (await cluster.Inspect.GetObjectHoldersAsync(o)).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        return result;
    }
}

/// <summary>How the returning replica is kept stale while a reader probes it (RSY-003).</summary>
public enum StaleWindow
{
    /// <summary>The replica and the reader are partitioned together on the minority side.</summary>
    MinorityPartition,

    /// <summary>The replica is back but every replication message to it is held.</summary>
    HoldAppends,

    /// <summary>The replica receives committed entries but applying them is blocked.</summary>
    BlockApply,
}

/// <summary>What is deleted while the replica is offline (RSY-005).</summary>
public enum DeletionKind
{
    /// <summary>A single file.</summary>
    File,

    /// <summary>A directory tree, recursively.</summary>
    RecursiveDirectory,
}

/// <summary>How old operation ids are retired during the outage (RSY-010).</summary>
public enum RetirementKind
{
    /// <summary>The owning client process dies; its session expires and retained outcomes age out.</summary>
    SessionExpired,

    /// <summary>The live session performs more than MaxOutcomesPerSession further operations.</summary>
    OutcomeWindowExceeded,
}

/// <summary>Where checkpoint catch-up is interrupted (RSY-012).</summary>
public enum ResyncStage
{
    /// <summary>While receiving checkpoint chunks.</summary>
    Receive,

    /// <summary>Before verifying the complete checkpoint.</summary>
    Verify,

    /// <summary>Before durably installing the verified checkpoint.</summary>
    Install,

    /// <summary>While replaying the post-checkpoint log suffix.</summary>
    SuffixReplay,

    /// <summary>Before activating the installed state.</summary>
    Activate,
}

/// <summary>How catch-up is interrupted (RSY-012).</summary>
public enum Interruption
{
    /// <summary>The returning node crashes (unflushed writes lost).</summary>
    Crash,

    /// <summary>The returning node is disconnected from every peer.</summary>
    Disconnect,
}

/// <summary>How the catch-up source fails (RSY-013).</summary>
public enum SourceFailure
{
    /// <summary>The leader crashes after the first checkpoint chunk reached the returning node.</summary>
    CrashDuringReceive,

    /// <summary>The leader is partitioned away after the first chunk; its stale chunks are delivered later.</summary>
    PartitionDuringReceive,

    /// <summary>The leader crashes while the returning node replays the post-checkpoint suffix.</summary>
    CrashDuringSuffix,
}

/// <summary>Which transferred bytes are corrupted (RSY-015).</summary>
public enum TransferCorruption
{
    /// <summary>One bit-flipped checkpoint chunk is delivered to the returning node.</summary>
    CheckpointChunkBitFlip,

    /// <summary>A truncated checkpoint chunk frame is delivered to the returning node.</summary>
    CheckpointChunkTruncated,

    /// <summary>A bit-flipped object payload is delivered to a storage node.</summary>
    ObjectPayloadBitFlip,

    /// <summary>The first repair source's stored copy is corrupt.</summary>
    RepairSourceCorruptAtRest,
}

/// <summary>How copies are lost before repair (RSY-016).</summary>
public enum CopyLoss
{
    /// <summary>Two of three copies are silently deleted; every node stays up.</summary>
    SilentDeletionOnTwo,

    /// <summary>One holder goes offline and its stored copies are lost; another holder silently loses its copies.</summary>
    OfflineAndSilentDeletion,
}

/// <summary>How every required copy becomes unusable (RSY-017).</summary>
public enum NoSourceKind
{
    /// <summary>Every copy deleted.</summary>
    AllMissing,

    /// <summary>Every copy corrupted.</summary>
    AllCorrupt,

    /// <summary>Some copies deleted, the rest corrupted.</summary>
    MixedMissingCorrupt,

    /// <summary>Only the middle extent of a multi-extent file loses every copy.</summary>
    MiddleExtentOnly,

    /// <summary>A single-copy policy whose only holder is offline.</summary>
    SoleHolderOffline,
}

/// <summary>Direction of a zone-policy change during repair (RSY-019).</summary>
public enum PolicyDirection
{
    /// <summary>More copies/zones.</summary>
    Increase,

    /// <summary>Fewer copies/zones.</summary>
    Decrease,
}

/// <summary>How a stale identity reappears (RSY-020).</summary>
public enum StaleIdentity
{
    /// <summary>A force-removed member restarts with its old disk.</summary>
    Removed,

    /// <summary>A member replaced by a new node in another zone restarts with its old disk.</summary>
    Replaced,
}

/// <summary>Disk-clone schedule (RSY-021).</summary>
public enum CloneSchedule
{
    /// <summary>Original and clone run at the same time.</summary>
    Concurrent,

    /// <summary>The clone starts while the original is down; the original returns later.</summary>
    CloneWhileOriginalDown,
}

/// <summary>Which catch-up phase runs out of space (RSY-022).</summary>
public enum DiskFullPhase
{
    /// <summary>Installing the metadata checkpoint.</summary>
    CheckpointInstall,

    /// <summary>Re-replicating data objects.</summary>
    DataRepair,
}

/// <summary>What limits repair throughput (RSY-023).</summary>
public enum RepairLimit
{
    /// <summary>Only the configured repair budget.</summary>
    BudgetOnly,

    /// <summary>Network bandwidth to the destination is starved, then restored.</summary>
    BandwidthStarvedThenRestored,

    /// <summary>Destination capacity is insufficient, then restored.</summary>
    CapacityStarvedThenRestored,
}

/// <summary>Kind of silent divergence (RSY-025).</summary>
public enum SilentDivergence
{
    /// <summary>The background replication to one target never happened.</summary>
    MissedReplication,

    /// <summary>A stored copy is silently corrupted.</summary>
    SilentCorruption,

    /// <summary>A stored copy silently disappears.</summary>
    SilentDeletion,
}

/// <summary>When the delayed older checkpoint arrives (RSY-026).</summary>
public enum OldCheckpointDelivery
{
    /// <summary>After the replica installed a newer checkpoint and replayed more log.</summary>
    AfterNewerInstall,

    /// <summary>After a leader change (the old checkpoint carries an older term).</summary>
    AfterLeaderChange,

    /// <summary>After the replica restarted from its newer durable state.</summary>
    AfterRestart,
}
