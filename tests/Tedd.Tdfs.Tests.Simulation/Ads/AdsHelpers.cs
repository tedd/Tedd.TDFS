using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Ads;

/// <summary>
/// Shared helpers of the ADS simulation rows (async-only: these scenarios run inside the deterministic simulation, so no
/// synchronous SDK calls, real-time delays or thread-pool work).
/// </summary>
internal static class AdsHelpers
{
    public const string Vol = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Upper bound for a backup transfer of a few MiB in a healthy simulated cluster (virtual time).</summary>
    public static readonly TimeSpan SyncTimeout = TimeSpan.FromSeconds(60);

    /// <summary>Deterministic simulation: virtual time, simulated disks, in-memory network; optional backup nodes.</summary>
    public static TestClusterOptions Det(int nodes = 3, int backups = 0) => new()
    {
        NodeCount = nodes,
        ControlVoters = Math.Min(3, nodes),
        BackupNodeCount = backups,
        Mode = ClusterMode.Deterministic,
        ScenarioTimeout = TimeSpan.FromMinutes(15),
        Volumes = [new TestVolumeSpec(Vol) { Metadata = new DfsMetadataPolicy { Voters = Math.Min(3, nodes) } }],
    };

    public static int Chunk(TestCluster cluster) => cluster.Options.Timings.ChunkSize;

    public static TimeSpan Lease(TestCluster cluster) => cluster.Options.Timings.LeaseDuration;

    /// <summary>Durable copies the default policy requires before acknowledging (the floor GC must never cross).</summary>
    public static int AckCopies(TestCluster cluster) => cluster.Options.GetEffectiveDefaultPolicy().DurableCopiesBeforeAck;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>Network identity of a test client ("client:{name}").</summary>
    public static string Net(string clientName) => new TestClientOptions { Name = clientName }.NetworkName;

    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, TestClock? clock = null) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, Clock = clock }, Ct);

    public static void BytesEqual(byte[] expected, byte[] actual, string because) => AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Deterministic bytes without any zero byte.</summary>
    public static byte[] NonZero(ulong seed, int length)
    {
        byte[] data = TestData.Bytes(seed, length);
        for (int i = 0; i < data.Length; i++)
        {
            if (data[i] == 0)
                data[i] = (byte)(1 + (i % 251));
        }

        return data;
    }

    /// <summary>A reader granting every share so writers, deleters and renamers can coexist (test plan §5).</summary>
    public static DfsOpenOptions Reader(DfsReadView view = DfsReadView.Live) => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite | FileShare.Delete,
        ReadView = view,
    };

    public static DfsOpenOptions Writer(FileMode mode = FileMode.Open, FileAccess access = FileAccess.ReadWrite) =>
        new() { Mode = mode, Access = access, Share = FileShare.Read };

    public static async Task<byte[]> ReadToEndAsync(Stream stream)
    {
        if (stream.CanSeek)
            stream.Seek(0, SeekOrigin.Begin);
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer, Ct);
        return buffer.ToArray();
    }

    /// <summary>Committed contents through a pinned reader that grants every share (never a setup-level conflict).</summary>
    public static async Task<byte[]> ReadCommittedAsync(DfsFileClient files, string path)
    {
        await using DfsStream reader = await files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
        return await ReadToEndAsync(reader);
    }

    /// <summary>Stages <paramref name="data"/> as the complete new contents of a seekable writable stream and commits it.</summary>
    public static async Task<CommitToken> ReplaceAndCommitAsync(DfsStream stream, byte[] data)
    {
        stream.Seek(0, SeekOrigin.Begin);
        await stream.WriteAsync(data, Ct);
        await stream.SetLengthAsync(data.Length, Ct);
        return await stream.CommitAsync(Ct);
    }

    public static async Task<Exception?> CatchAsync(Func<Task> action)
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

    public static string Describe(Exception? ex) => ex is null
        ? "no exception (the operation succeeded)"
        : $"{ex.GetType().FullName}{(ex is DfsException d ? $" [{d.ErrorCode}]" : string.Empty)}: {ex.Message}";

    public static void AssertSharingViolation(Exception? ex, string because)
    {
        if (ex is not DfsSharingViolationException conflict)
        {
            Assert.Fail($"{because}: expected DfsSharingViolationException but got {Describe(ex)}");
            return;
        }

        Assert.True(conflict.ErrorCode is DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict, $"{because}: got {conflict.ErrorCode}");
        Assert.True(conflict.IsRetryable, $"{because}: sharing conflicts are retryable");
    }

    /// <summary>Lost writer authority surfaces as <see cref="DfsLeaseLostException"/> (FSD §13).</summary>
    public static void AssertLeaseLost(Exception? ex, string because)
    {
        if (ex is not DfsLeaseLostException lost)
        {
            Assert.Fail($"{because}: expected DfsLeaseLostException but got {Describe(ex)}");
            return;
        }

        Assert.Equal(DfsErrorCode.LeaseLost, lost.ErrorCode);
    }

    public static void AssertTransactionConflict(Exception? ex, DfsTransactionId id, string because)
    {
        if (ex is not DfsTransactionConflictException conflict || ex is DfsTransactionAbortedException)
        {
            Assert.Fail($"{because}: expected DfsTransactionConflictException (TransactionConflict) but got {Describe(ex)}");
            return;
        }

        Assert.Equal(DfsErrorCode.TransactionConflict, conflict.ErrorCode);
        Assert.True(conflict.IsRetryable, $"{because}: a transaction conflict is retryable");
        Assert.Equal(id, conflict.TransactionId);
    }

    /// <summary>Awaits <paramref name="task"/> but fails the test if it is still pending after <paramref name="bound"/> of harness time.</summary>
    public static async Task<T> WithinAsync<T>(TestCluster cluster, Task<T> task, TimeSpan bound, string because)
    {
        if (await Task.WhenAny(task, cluster.DelayAsync(bound, Ct)) != task)
            Assert.Fail($"{because}: still pending after {bound}");
        return await task;
    }

    public static IReadOnlyList<TestNode> Running(TestCluster cluster) =>
        cluster.Nodes.Where(n => n.State == TestNodeState.Running).ToArray();

    /// <summary>Waits until a running node other than <paramref name="oldLeader"/> leads the volume group.</summary>
    public static async Task<TestNode> WaitForNewLeaderAsync(TestCluster cluster, string oldLeader, TimeSpan timeout)
    {
        TestNode? found = null;
        await cluster.WaitUntilAsync(async () =>
        {
            foreach (TestNode node in Running(cluster))
            {
                if (node.Name == oldLeader)
                    continue;
                GroupInspection? group = await node.Inspect.GetGroupAsync(Vol);
                if (group is not null && group.Role == RaftRole.Leader)
                {
                    found = node;
                    return true;
                }
            }

            return false;
        }, timeout, $"a leader of '{Vol}' other than {oldLeader} must be elected", Ct);
        return found!;
    }

    public static async Task<HandleInspection[]> ActiveHandlesAsync(TestCluster cluster, InodeId inode) =>
        (await cluster.Inspect.GetVolumeStateAsync(Vol)).Handles.Where(h => h.Inode == inode && !h.Expired).ToArray();

    public static async Task<FileInspection> InspectFileAsync(TestCluster cluster, string filePath, string volume = Vol)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, filePath);
        Assert.True(file is not null, $"{filePath} must exist in the leader's applied state of '{volume}'");
        return file!;
    }

    public static async Task<StreamInspection> InspectStreamAsync(TestCluster cluster, string filePath, string streamName)
    {
        FileInspection file = await InspectFileAsync(cluster, filePath);
        StreamInspection? stream = file.Streams.SingleOrDefault(s => string.Equals(s.Name, streamName, StringComparison.Ordinal));
        Assert.True(stream is not null, $"{filePath} must have the named stream '{streamName}' in the applied state");
        return stream!;
    }

    public static HashSet<ObjectId> Objects(IEnumerable<ExtentInspection> extents) => extents.Select(e => e.Object).ToHashSet();

    /// <summary>Every object referenced by any stream (default and named) of the file.</summary>
    public static HashSet<ObjectId> AllObjects(FileInspection file)
    {
        HashSet<ObjectId> set = Objects(file.Extents);
        foreach (StreamInspection stream in file.Streams)
            set.UnionWith(Objects(stream.Extents));
        return set;
    }

    public static async Task<HashSet<ObjectId>> StoredAsync(TestCluster cluster, string volume = Vol) =>
        [.. await cluster.Inspect.GetAllStoredObjectsAsync(volume)];

    /// <summary>Objects stored on any running node that were uploaded under one of <paramref name="staging"/>.</summary>
    public static async Task<HashSet<ObjectId>> StoredObjectsOfStagingAsync(TestCluster cluster, IReadOnlySet<StagingId> staging)
    {
        var result = new HashSet<ObjectId>();
        foreach (TestNode node in Running(cluster))
        {
            foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Vol))
            {
                if (staging.Contains(record.Staging))
                    result.Add(record.Id);
            }
        }

        return result;
    }

    /// <summary>Every id has at least <paramref name="minHolders"/> nodes durably holding a verified copy (INV-09).</summary>
    public static async Task AssertHeldAsync(TestCluster cluster, IEnumerable<ObjectId> ids, int minHolders, string because)
    {
        foreach (ObjectId id in ids)
        {
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
            Assert.True(holders.Count >= minHolders,
                $"{because}: object {id} has {holders.Count} verified holder(s) [{string.Join(",", holders)}], at least {minHolders} required (INV-09).");
        }
    }

    /// <summary>Lets several GC/inventory passes run (virtual time).</summary>
    public static Task QuiesceGcAsync(TestCluster cluster, int passes = 12) =>
        cluster.DelayAsync((cluster.Options.Timings.GcInterval * passes) + (cluster.Options.Timings.InventoryInterval * 4), Ct);

    /// <summary>Maintenance budget for "eventually reclaimed" waits.</summary>
    public static TimeSpan ReclaimBudget(TestCluster cluster) =>
        (cluster.Options.Timings.GcInterval * 40) + (cluster.Options.Timings.InventoryInterval * 10) + cluster.Options.Timings.PinDuration
        + cluster.Options.Timings.SessionTimeout;

    public static void JumpWallClocks(TestCluster cluster, TimeSpan delta)
    {
        foreach (TestNode node in Running(cluster))
            node.Clock.JumpWall(delta);
    }

    public static Task WaitReclaimedAsync(TestCluster cluster, IReadOnlyCollection<ObjectId> ids, string because) =>
        cluster.WaitUntilAsync(async () => !(await StoredAsync(cluster)).Overlaps(ids), ReclaimBudget(cluster), because, Ct);

    public static async Task<Dictionary<string, CommitToken>> WriteModelAsync(DfsFileClient files, string filePath, StreamModel model)
    {
        var tokens = new Dictionary<string, CommitToken>(StringComparer.Ordinal)
        {
            [string.Empty] = await files.WriteAllBytesAsync(filePath, model.Default, Ct),
        };
        foreach (KeyValuePair<string, byte[]> stream in model.NamedStreams)
            tokens[stream.Key] = await files.WriteAllBytesAsync(DfsStreamPath.Combine(filePath, stream.Key), stream.Value, Ct);
        return tokens;
    }

    /// <summary>Default-stream bytes, the exact named-stream set (ordinal, lengths) and every named stream's bytes.</summary>
    public static async Task AssertStreamsAsync(DfsFileClient files, string filePath, StreamModel expected, string because)
    {
        BytesEqual(expected.Default, await files.ReadAllBytesAsync(filePath, Ct), $"{because}: default stream of {filePath}");
        IReadOnlyList<DfsStreamInfo> listed = await files.ListStreamsAsync(filePath, Ct);
        string[] expectedNames = expected.NamedStreams.Keys.ToArray();
        string[] actualNames = listed.Select(s => s.Name).ToArray();
        Assert.True(expectedNames.SequenceEqual(actualNames, StringComparer.Ordinal),
            $"{because}: named streams of {filePath} are [{string.Join(", ", actualNames)}], expected [{string.Join(", ", expectedNames)}]");
        foreach (DfsStreamInfo info in listed)
        {
            byte[] bytes = expected.NamedStreams[info.Name];
            Assert.True(info.Length == bytes.Length, $"{because}: {filePath}:{info.Name} length {info.Length}, expected {bytes.Length}");
            BytesEqual(bytes, await files.ReadAllBytesAsync(DfsStreamPath.Combine(filePath, info.Name), Ct), $"{because}: {filePath}:{info.Name}");
        }
    }

    /// <summary>A directory's named streams: exact set (ordinal) and bytes.</summary>
    public static async Task AssertDirectoryStreamsAsync(DfsFileClient files, string directory, SortedDictionary<string, byte[]> expected, string because)
    {
        IReadOnlyList<DfsStreamInfo> listed = await files.ListStreamsAsync(directory, Ct);
        Assert.True(expected.Keys.SequenceEqual(listed.Select(s => s.Name), StringComparer.Ordinal),
            $"{because}: streams of directory {directory} are [{string.Join(", ", listed.Select(s => s.Name))}], expected [{string.Join(", ", expected.Keys)}]");
        foreach ((string name, byte[] bytes) in expected)
            BytesEqual(bytes, await files.ReadAllBytesAsync(DfsStreamPath.Combine(directory, name), Ct), $"{because}: {directory}:{name}");
    }

    /// <summary>Names directly under <paramref name="directory"/> (ordinal-sorted).</summary>
    public static async Task<string[]> NamesAsync(DfsDirectoryClient directories, string directory)
    {
        var names = new List<string>();
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync(directory, null, Ct))
            names.Add(entry.Name);
        return [.. names.Order(StringComparer.Ordinal)];
    }
}

/// <summary>Independent expected view of one file: default-stream bytes plus named streams (ordinal by name).</summary>
internal sealed class StreamModel
{
    public StreamModel(byte[] defaultStream) => Default = defaultStream.ToArray();

    public byte[] Default { get; set; }

    public SortedDictionary<string, byte[]> NamedStreams { get; } = new(StringComparer.Ordinal);

    public StreamModel With(string name, byte[] data)
    {
        NamedStreams[name] = data.ToArray();
        return this;
    }

    public StreamModel Clone()
    {
        var copy = new StreamModel(Default);
        foreach (KeyValuePair<string, byte[]> stream in NamedStreams)
            copy.NamedStreams[stream.Key] = stream.Value.ToArray();
        return copy;
    }
}
