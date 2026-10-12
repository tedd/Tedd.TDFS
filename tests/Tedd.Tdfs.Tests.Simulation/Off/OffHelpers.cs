using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Off;

/// <summary>How a node is taken offline.</summary>
public enum OfflineMode
{
    /// <summary>Abrupt crash; the disk keeps only durable state.</summary>
    Crash,

    /// <summary>Graceful stop; disk preserved.</summary>
    Stop,

    /// <summary>Process frozen with memory and queued messages retained.</summary>
    Pause,

    /// <summary>Every network link of the node is cut; the process keeps running.</summary>
    Disconnect,
}

/// <summary>Result of an attempted call: a value, or the exception it failed with.</summary>
internal readonly record struct OffAttempt<T>(T? Value, Exception? Error)
{
    public bool Succeeded => Error is null;
    public OpOutcome Classification => Error is null ? OpOutcome.Ok : HistoryRecorder.Classify(Error);
    public override string ToString() => Error is null ? $"Ok({Value})" : $"{Error.GetType().Name}: {Error.Message}";
}

/// <summary>Shared helpers for the OFF simulation tests.</summary>
internal static class OffHelpers
{
    public const string Volume = "vol";

    /// <summary>Bounded liveness wait (virtual time).</summary>
    public static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);

    /// <summary>Per-operation timeout for clients that probe unavailable configurations.</summary>
    public static readonly TimeSpan ShortOperation = TimeSpan.FromSeconds(4);

    /// <summary>
    /// Per-operation deadline where progress is required: long enough to fail over from paused or disconnected leaders
    /// and storage targets (several election timeouts and RPC retries), still well inside <see cref="Bounded"/>.
    /// </summary>
    public static readonly TimeSpan ProgressOperation = TimeSpan.FromSeconds(20);

    public static readonly byte[] V1 = TestData.Bytes(0x0FF_0001, 900);
    public static readonly byte[] V2 = TestData.Bytes(0x0FF_0002, 1100);
    public static readonly byte[] Big1 = TestData.Bytes(0x0FF_0003, 4 * (64 << 10));
    public static readonly byte[] Big2 = TestData.Bytes(0x0FF_0004, 4 * (64 << 10));

    /// <summary>Deterministic cluster of <paramref name="nodes"/> one-per-zone nodes (3 or 5 voters).</summary>
    public static TestClusterOptions Sim(int nodes = 3, int backups = 0, DfsStoragePolicy? policy = null)
    {
        int voters = nodes >= 5 ? 5 : Math.Min(3, nodes);
        return TestClusterOptions.ThreeZones with
        {
            NodeCount = nodes,
            ControlVoters = voters,
            Volumes = [new TestVolumeSpec(Volume) { Metadata = new DfsMetadataPolicy { Voters = voters }, DefaultPolicy = policy }],
            Mode = ClusterMode.Deterministic,
            BackupNodeCount = backups,
        };
    }

    /// <summary>Deterministic cluster with explicit node specs and volume voters.</summary>
    public static TestClusterOptions Custom(IReadOnlyList<TestNodeSpec> nodes, DfsMetadataPolicy metadata, int controlVoters = 3, DfsStoragePolicy? policy = null) =>
        TestClusterOptions.ThreeZones with
        {
            Nodes = nodes,
            Topology = TestTopology.Custom,
            ControlVoters = controlVoters,
            Volumes = [new TestVolumeSpec(Volume) { Metadata = metadata, DefaultPolicy = policy }],
            Mode = ClusterMode.Deterministic,
        };

    /// <summary>Explicit volume voters n0..n{voters-1} on a cluster of <paramref name="nodes"/> one-per-zone nodes.</summary>
    public static TestClusterOptions ExplicitVoters(int nodes, int voters = 3) => TestClusterOptions.ThreeZones with
    {
        NodeCount = nodes,
        ControlVoters = voters,
        Volumes =
        [
            new TestVolumeSpec(Volume)
            {
                Metadata = new DfsMetadataPolicy
                {
                    Voters = voters,
                    Placement = DfsVoterPlacement.Explicit,
                    ExplicitVoterNodes = Enumerable.Range(0, voters).Select(i => "n" + i).ToArray(),
                },
            },
        ],
        Mode = ClusterMode.Deterministic,
    };

    public static async Task<(DfsClient Client, DfsVolume Volume)> ConnectAsync(TestCluster cluster, string name, CancellationToken ct,
        IReadOnlyList<string>? seeds = null, TimeSpan? operationTimeout = null, DfsCredentials? credentials = null, TestClock? clock = null)
    {
        DfsClient client = await cluster.ConnectAsync(new TestClientOptions
        {
            Name = name,
            SeedNodes = seeds,
            OperationTimeout = operationTimeout,
            Credentials = credentials,
            Clock = clock,
        }, ct);
        return (client, client.GetVolume(Volume));
    }

    public static string ClientName(string name) => "client:" + name;

    public static async Task<OffAttempt<T>> TryAsync<T>(Func<Task<T>> action)
    {
        try
        {
            return new OffAttempt<T>(await action(), null);
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.IAssertionException)
        {
            return new OffAttempt<T>(default, ex);
        }
    }

    public static async Task<OffAttempt<bool>> TryAsync(Func<Task> action)
    {
        try
        {
            await action();
            return new OffAttempt<bool>(true, null);
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.IAssertionException)
        {
            return new OffAttempt<bool>(false, ex);
        }
    }

    public static async Task ExpectContentAsync(byte[] expected, Task<byte[]> read, string because)
    {
        byte[] actual = await read;
        AssertEx.BytesEqual(expected, actual, because);
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

    /// <summary>
    /// True for failures that honestly report missing authority or availability (no leader, no quorum, placement or node
    /// unavailable, timeout, expired pin/lease) — as opposed to fabricated absence or success.
    /// </summary>
    public static bool IsAvailabilityFailure(Exception ex) =>
        ex is DfsUnavailableException
        || ex is TimeoutException
        || ex is DfsPinExpiredException
        || ex is DfsLeaseLostException
        || ex is DfsException
        {
            ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NotLeader or DfsErrorCode.QuorumUnavailable or DfsErrorCode.Unavailable
                or DfsErrorCode.NodeUnavailable or DfsErrorCode.PlacementUnavailable
        };

    /// <summary>True for a legal non-success answer of a mutation without the required authority/placement.</summary>
    public static bool IsNoSuccessFailure(Exception ex) => IsAvailabilityFailure(ex) || HistoryRecorder.Classify(ex) == OpOutcome.Unknown;

    /// <summary>True when the configured voters minus the offline ones still form a strict majority of the configured set.</summary>
    public static bool HasConfiguredMajority(IReadOnlyCollection<string> configuredVoters, IReadOnlySet<string> offline)
    {
        int online = configuredVoters.Count(v => !offline.Contains(v));
        return online * 2 > configuredVoters.Count;
    }

    /// <summary>True when the online nodes offer enough distinct hosts and zones for the policy's acknowledgement.</summary>
    public static bool CanMeetAckPolicy(DfsStoragePolicy policy, IEnumerable<DfsTopologyLabels> onlineLabels)
    {
        (int copies, int zones) = Placement.CountDomains(onlineLabels);
        return copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck;
    }

    public static async Task TakeOfflineAsync(TestCluster cluster, TestNode node, OfflineMode mode)
    {
        switch (mode)
        {
            case OfflineMode.Crash:
                await node.CrashAsync();
                break;
            case OfflineMode.Stop:
                await node.StopAsync();
                break;
            case OfflineMode.Pause:
                await node.PauseAsync();
                break;
            case OfflineMode.Disconnect:
                cluster.Network.Isolate(node.Name);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }
    }

    /// <summary>Brings a node back (restart from its durable image, resume, or heal all network cuts).</summary>
    public static async Task BringOnlineAsync(TestCluster cluster, TestNode node, OfflineMode mode)
    {
        switch (mode)
        {
            case OfflineMode.Crash:
            case OfflineMode.Stop:
                await node.RestartAsync();
                break;
            case OfflineMode.Pause:
                await node.ResumeAsync();
                break;
            case OfflineMode.Disconnect:
                cluster.Network.Heal();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
        }
    }

    public static async Task<IReadOnlyList<string>> GetVolumeVotersAsync(TestCluster cluster)
    {
        GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Volume);
        return group.Voters;
    }

    /// <summary>A volume voter that is not the current leader.</summary>
    public static async Task<TestNode> GetFollowerAsync(TestCluster cluster, CancellationToken ct, int skip = 0)
    {
        TestNode leader = await cluster.GetLeaderAsync(Volume, ct);
        IReadOnlyList<string> voters = await GetVolumeVotersAsync(cluster);
        return cluster.Nodes.Where(n => n.Name != leader.Name && voters.Contains(n.Name)).Skip(skip).First();
    }

    /// <summary>Distinct objects referenced by a file's current extents.</summary>
    public static async Task<IReadOnlyList<ObjectId>> GetFileObjectsAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Volume, path);
        Assert.NotNull(file);
        return file.Extents.Select(e => e.Object).Distinct().ToArray();
    }

    /// <summary>Total staged (unpublished) bytes on all running nodes.</summary>
    public static async Task<long> GetStagedBytesAsync(TestCluster cluster)
    {
        long total = 0;
        foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
            total += await node.Inspect.GetStagedBytesAsync();
        return total;
    }

    /// <summary>
    /// Fails if any running node reports itself leader of the volume group (used while fewer than a configured majority
    /// of voters are running: no authority may be assumed).
    /// </summary>
    public static async Task AssertNoRunningLeaderAsync(TestCluster cluster, string because)
    {
        foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
        {
            GroupInspection? group = await node.Inspect.GetGroupAsync(Volume);
            if (group is not null)
                Assert.True(group.Role != Tedd.Tdfs.Consensus.RaftRole.Leader, $"{node.Name} claims leadership of '{Volume}' (term {group.Term}): {because}");
        }
    }

    /// <summary>Waits until some running node other than <paramref name="excluded"/> leads the volume group at a term above <paramref name="minTermExclusive"/>.</summary>
    public static async Task<TestNode> WaitForNewLeaderAsync(TestCluster cluster, string? excluded, ulong minTermExclusive, CancellationToken ct)
    {
        TestNode? found = null;
        await cluster.WaitUntilAsync(async () =>
        {
            foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running && n.Name != excluded))
            {
                GroupInspection? group = await node.Inspect.GetGroupAsync(Volume);
                if (group is { Role: Tedd.Tdfs.Consensus.RaftRole.Leader } && group.Term > minTermExclusive)
                {
                    found = node;
                    return true;
                }
            }

            return false;
        }, Bounded, $"a successor leader (term > {minTermExclusive}) is elected among the remaining majority", ct);
        return found!;
    }

    /// <summary>Writes several committed files of different sizes and returns (path, bytes) for later verification.</summary>
    public static async Task<IReadOnlyList<(string Path, byte[] Bytes)>> WriteDataSetAsync(DfsVolume volume, string root, CancellationToken ct)
    {
        var files = new List<(string, byte[])>
        {
            (root + "/empty", []),
            (root + "/small", TestData.Bytes(0x5E7_0001, 100)),
            (root + "/inline-edge", TestData.Bytes(0x5E7_0002, 4096)),
            (root + "/multi", TestData.Bytes(0x5E7_0003, 3 * (64 << 10) + 17)),
        };
        await volume.Directories.CreateDirectoryAsync(root, ct);
        foreach ((string path, byte[] bytes) in files)
            await volume.Files.WriteAllBytesAsync(path, bytes, ct);
        return files;
    }

    /// <summary>
    /// Whole-file live read that coexists with an open writer: a version-pinned handle (one revision selected
    /// linearizably at open) with <c>FileShare.ReadWrite | FileShare.Delete</c>, so a sharing conflict with a writer that
    /// may still hold the file is never mistaken for a publication or availability result (test plan §5).
    /// </summary>
    public static async Task<byte[]> ReadSharedAsync(DfsVolume volume, string path, CancellationToken ct)
    {
        await using DfsStream s = await volume.Files.OpenAsync(path, new DfsOpenOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite | FileShare.Delete,
            ReadView = DfsReadView.VersionPinned,
        }, ct);
        using var ms = new MemoryStream();
        await s.CopyToAsync(ms, ct);
        return ms.ToArray();
    }

    public static Task AssertDataSetAsync(DfsVolume volume, IReadOnlyList<(string Path, byte[] Bytes)> files, string because, CancellationToken ct) =>
        AssertFilesAsync(volume.Files, files, because, ct);

    /// <summary>Asserts exact bytes of every file through any file facade (live volume, snapshot view, backup view).</summary>
    public static async Task AssertFilesAsync(DfsFileClient facade, IReadOnlyList<(string Path, byte[] Bytes)> files, string because, CancellationToken ct)
    {
        foreach ((string path, byte[] bytes) in files)
            await ExpectContentAsync(bytes, facade.ReadAllBytesAsync(path, ct), $"{path}: {because}");
    }

    /// <summary>The volume's effective default data policy as reported by the cluster (not the harness's derived default).</summary>
    public static async Task<DfsStoragePolicy> GetVolumePolicyAsync(DfsVolume volume, CancellationToken ct) =>
        (await volume.GetInfoAsync(ct)).DefaultPolicy;

    /// <summary>All 2^n subsets of the first <paramref name="n"/> node indexes, as bit masks.</summary>
    public static IEnumerable<int> AllSubsets(int n) => Enumerable.Range(0, 1 << n);

    public static IReadOnlyList<int> Members(int mask, int n) => Enumerable.Range(0, n).Where(i => (mask & (1 << i)) != 0).ToArray();
}
