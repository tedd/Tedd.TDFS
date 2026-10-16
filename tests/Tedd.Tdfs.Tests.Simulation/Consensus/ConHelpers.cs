using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Con;

/// <summary>Shared helpers for the CON (and NET) simulation suites.</summary>
internal static class ConHelpers
{
    /// <summary>The default test volume.</summary>
    public const string Vol = "vol";

    /// <summary>The control group name used by inspectors.</summary>
    public const string Control = "control";

    /// <summary>
    /// Liveness bound for elections and recovery under <see cref="NodeTimings.Fast"/> (election timeout 150-300 ms,
    /// lease 2 s): generous so that only a genuine liveness defect can exceed it.
    /// </summary>
    public static readonly TimeSpan ElectionBound = TimeSpan.FromSeconds(10);

    /// <summary>Operation timeout for clients that are expected to fail fast (minority sides, blocked paths).</summary>
    public static readonly TimeSpan ShortOperationTimeout = TimeSpan.FromSeconds(3);

    /// <summary>Lease duration of <see cref="NodeTimings.Fast"/>.</summary>
    public static readonly TimeSpan Lease = NodeTimings.Fast.LeaseDuration;

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>The options in deterministic simulation mode.</summary>
    public static TestClusterOptions Det(TestClusterOptions options) => options with { Mode = ClusterMode.Deterministic };

    /// <summary>Network name of a test client.</summary>
    public static string ClientNet(string clientName) => "client:" + clientName;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    public static DfsMutationOptions WithId(DfsOperationId id) => new() { OperationId = id };

    /// <summary>
    /// Drops every reply of <c>QueryOperation</c> (until disarmed). The SDK resolves a mutation that got no reply through
    /// <c>QueryOperation(resolve: true)</c> (ARCHITECTURE §7/§9.2) and then legitimately reports the decided outcome;
    /// scenarios whose premise is that no decision ever reaches the caller (every reply lost) must therefore lose the
    /// replies of that resolution channel too.
    /// </summary>
    public static FaultArm ArmDropOutcomeQueryReplies(TestCluster cluster) =>
        cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.QueryOperation, Count: 100_000), FaultAction.DropReply());

    /// <summary>
    /// Network variant of <see cref="ArmDropOutcomeQueryReplies"/>: the replies of every <c>QueryOperation</c> the client
    /// <paramref name="clientNet"/> sends are lost (until the rule is cancelled).
    /// </summary>
    public static MessageRule DropOutcomeQueryReplies(TestCluster cluster, string clientNet) =>
        cluster.Network.DropReplies(new MessageFilter(From: clientNet, Type: MessageType.QueryOperation), 100_000);

    public static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    public static async Task<(T? Value, Exception? Error)> CaptureValueAsync<T>(Func<Task<T>> action)
    {
        try
        {
            return (await action(), null);
        }
        catch (Exception ex)
        {
            return (default, ex);
        }
    }

    /// <summary>True for an explicit "commit may have happened" report (FSD §13).</summary>
    public static bool IsUnknownOutcome(Exception ex) =>
        ex is DfsCommitOutcomeUnknownException || ex is DfsException { ErrorCode: DfsErrorCode.OutcomeUnknown };

    /// <summary>True for an explicit, typed availability failure (quorum, leader, node, timeout).</summary>
    public static bool IsUnavailability(Exception ex) =>
        ex is DfsUnavailableException
        || ex is TimeoutException
        || ex is DfsException
        {
            ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.QuorumUnavailable or DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable
            or DfsErrorCode.Timeout or DfsErrorCode.PlacementUnavailable
        };

    /// <summary>
    /// Asserts that an operation which could not reach its quorum did not report success and failed explicitly: either a
    /// typed availability error or an explicit unknown outcome (never a definite success, never a silent absence).
    /// </summary>
    public static void AssertNoCommitClaimed(Exception? error, string because)
    {
        Assert.True(error is not null, $"{because}: the operation reported success without the required quorum");
        Assert.True(IsUnavailability(error!) || IsUnknownOutcome(error!),
            $"{because}: expected an explicit unavailability or unknown-outcome error, got {error!.GetType().FullName}: {error.Message}");
    }

    /// <summary>
    /// Asserts the FSD §13 surface for a request that was delivered (so a commit may have happened): an explicit
    /// unknown-outcome error carrying the operation identity, never success and never a definite failure.
    /// </summary>
    public static void AssertUnknownOutcome(Exception? error, DfsOperationId id, string because)
    {
        Assert.True(error is not null, $"{because}: success was reported although no commit decision reached the client");
        Assert.True(IsUnknownOutcome(error!),
            $"{because}: the request was delivered, so the commit may have happened and must be reported as unknown; got {error!.GetType().FullName}: {error.Message}");
        if (error is DfsCommitOutcomeUnknownException unknown)
            Assert.Equal(id, unknown.OperationId);
    }

    /// <summary>
    /// Resolves <paramref name="id"/> authoritatively and checks that the resolution is definite and consistent with what
    /// the caller was told (<paramref name="error"/>) and with the namespace: a reported success must be committed, a
    /// definite (non-unknown) failure must be an abort (FSD §13: an operation that may have committed is reported as
    /// unknown), a commit must be visible exactly once and an abort must have no effect.
    /// </summary>
    public static async Task<DfsOperationOutcome> AssertResolvedConsistentlyAsync(DfsVolume resolver, DfsOperationId id, Exception? error,
        Func<Task> assertApplied, Func<Task> assertNotApplied, string because)
    {
        DfsOperationOutcome outcome = await resolver.QueryOperationAsync(id, resolve: true, Ct);
        Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"{because}: resolution must be definite, got {outcome.Status}");
        if (error is null)
            Assert.True(outcome.Status == DfsOutcomeStatus.Committed, $"{because}: success was reported but the operation resolved as {outcome.Status}");
        else if (!IsUnknownOutcome(error))
            Assert.True(outcome.Status == DfsOutcomeStatus.Aborted,
                $"{because}: a definite failure ({error.GetType().Name}) was reported for an operation that committed; it had to be reported as unknown");
        if (outcome.Status == DfsOutcomeStatus.Committed)
            await assertApplied();
        else
            await assertNotApplied();
        return outcome;
    }

    /// <summary>
    /// The highest log index any of <paramref name="nodes"/> holds right now. Entries proposed after this moment have
    /// higher indexes, so "no new metadata was committed" is <c>CommitIndex &lt;= this bound</c> (robust against
    /// in-flight messages that only spread knowledge of entries committed earlier).
    /// </summary>
    public static async Task<ulong> MaxLastIndexAsync(IEnumerable<TestNode> nodes, string group = Vol)
    {
        ulong max = 0;
        foreach (TestNode node in nodes)
        {
            if (node.State != TestNodeState.Running)
                continue;
            max = Math.Max(max, (await GroupOnAsync(node, group)).LastIndex);
        }

        return max;
    }

    /// <summary>The nodes of the cluster other than <paramref name="excluded"/>.</summary>
    public static TestNode[] Others(TestCluster cluster, params TestNode[] excluded) =>
        cluster.Nodes.Where(n => excluded.All(e => !string.Equals(e.Name, n.Name, StringComparison.Ordinal))).ToArray();

    public static string[] Names(IEnumerable<TestNode> nodes) => nodes.Select(n => n.Name).ToArray();

    /// <summary>A running node's own view of a group (fails the test when the node does not host the group).</summary>
    public static async Task<GroupInspection> GroupOnAsync(TestNode node, string group = Vol)
    {
        GroupInspection? inspection = await node.Inspect.GetGroupAsync(group);
        Assert.True(inspection is not null, $"{node.Name} does not host group '{group}'");
        return inspection!;
    }

    /// <summary>The running node among <paramref name="nodes"/> that believes it leads <paramref name="group"/>, if any.</summary>
    public static async Task<TestNode?> LeaderAmongAsync(IEnumerable<TestNode> nodes, string group = Vol)
    {
        foreach (TestNode node in nodes)
        {
            if (node.State != TestNodeState.Running)
                continue;
            GroupInspection? inspection = await node.Inspect.GetGroupAsync(group);
            if (inspection is { Role: RaftRole.Leader })
                return node;
        }

        return null;
    }

    /// <summary>Waits (bounded) until one of <paramref name="nodes"/> leads <paramref name="group"/> and returns it.</summary>
    public static async Task<TestNode> WaitForLeaderAmongAsync(TestCluster cluster, IReadOnlyList<TestNode> nodes, string because, string group = Vol)
    {
        await cluster.WaitUntilAsync(async () => await LeaderAmongAsync(nodes, group) is not null, ElectionBound, because, Ct);
        return (await LeaderAmongAsync(nodes, group))!;
    }

    /// <summary>Restarts every crashed or stopped node and backup node.</summary>
    public static async Task RestartAllDownAsync(TestCluster cluster)
    {
        foreach (TestNode node in cluster.Nodes)
        {
            if (node.State is TestNodeState.Crashed or TestNodeState.Stopped)
                await node.RestartAsync();
            else if (node.State == TestNodeState.Paused)
                await node.ResumeAsync();
        }

        foreach (TestBackupNode backup in cluster.BackupNodes)
        {
            if (backup.State is TestNodeState.Crashed or TestNodeState.Stopped)
                await backup.RestartAsync();
            else if (backup.State == TestNodeState.Paused)
                await backup.ResumeAsync();
        }
    }

    /// <summary>Heals the network, restarts down nodes and waits for a stable, converged cluster.</summary>
    public static async Task RecoverAndConvergeAsync(TestCluster cluster, string volume = Vol)
    {
        cluster.Network.Heal();
        await RestartAllDownAsync(cluster);
        await cluster.WaitForStableAsync(ElectionBound * 3, Ct);
        await cluster.WaitForReplicationAsync(volume, Ct);
        await cluster.Inspect.AssertNoSplitBrainAsync();
        await cluster.Inspect.AssertReplicasConvergedAsync(volume);
    }

    /// <summary>Reads a whole file through a client and compares it byte for byte.</summary>
    public static async Task AssertContentAsync(DfsVolume volume, string path, byte[] expected, string because)
    {
        byte[] actual = await volume.Files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(expected, actual, because);
    }

    /// <summary>Awaits <paramref name="actual"/> and compares it byte for byte with <paramref name="expected"/>.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int offset = 0;
        foreach (byte[] part in parts)
        {
            part.CopyTo(result, offset);
            offset += part.Length;
        }

        return result;
    }

    /// <summary>Asserts that a file is absent (never a false "present").</summary>
    public static async Task AssertAbsentAsync(DfsVolume volume, string path, string because) =>
        Assert.False(await volume.Files.ExistsAsync(path, Ct), $"{because}: {path} must not exist");

    /// <summary>The leader's applied view of a file on a specific replica (null when absent).</summary>
    public static async Task<FileInspection?> FileOnReplicaAsync(TestCluster cluster, string node, string path, string volume = Vol) =>
        (await cluster.Inspect.GetVolumeStateAsync(volume, node)).GetFile(path);
}
