using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.BAK;

/// <summary>
/// Independent expected view of one snapshot's namespace: every file path with its exact bytes, plus every
/// directory. Built by the test from the operations it performed, never from the system under test.
/// </summary>
internal sealed class TreeModel
{
    public SortedDictionary<string, byte[]> Files { get; } = new(StringComparer.Ordinal);

    public SortedSet<string> Directories { get; } = new(StringComparer.Ordinal);

    public TreeModel Clone()
    {
        var copy = new TreeModel();
        foreach (KeyValuePair<string, byte[]> file in Files)
            copy.Files[file.Key] = file.Value.ToArray();
        foreach (string dir in Directories)
            copy.Directories.Add(dir);
        return copy;
    }

    public void Put(string path, byte[] data)
    {
        Files[path] = data.ToArray();
        AddParents(path);
    }

    public void AddDirectory(string path)
    {
        Directories.Add(path);
        AddParents(path);
    }

    public void Remove(string path) => Assert.True(Files.Remove(path), $"model has no file {path}");

    public long TotalFileBytes => Files.Values.Sum(f => (long)f.Length);

    private void AddParents(string path)
    {
        string parent = DfsPath.GetParent(path);
        while (!string.Equals(parent, DfsPath.Root, StringComparison.Ordinal))
        {
            Directories.Add(parent);
            parent = DfsPath.GetParent(parent);
        }
    }
}

/// <summary>Shared scenario plumbing for the BAK simulation suite.</summary>
internal static class BAKHelpers
{
    public const string Volume = "vol";

    /// <summary>Upper bound for a backup transfer of a few MiB to complete in a healthy simulated cluster.</summary>
    public static readonly TimeSpan SyncTimeout = TimeSpan.FromSeconds(60);

    /// <summary>Upper bound for an armed fault point to be reached once its triggering work was requested.</summary>
    public static readonly TimeSpan FaultTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Short backup barrier timeout used where the barrier must NOT be satisfied.</summary>
    public static readonly TimeSpan ShortAwait = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Bound for a small live commit in a healthy live cluster (NodeTimings.Fast: 5 ms ticks, elections within
    /// 150-300 ms). A live commit that waits on a backup would exceed it by orders of magnitude.
    /// </summary>
    public static readonly TimeSpan LiveCommitBound = TimeSpan.FromSeconds(5);

    /// <summary>No automatic schedule: transfers happen only when a test requests them.</summary>
    public static readonly DfsBackupSchedule ManualSchedule = new(Interval: null, OnEverySnapshot: false);

    /// <summary>Budgets that never trigger abandonment within a test.</summary>
    public static readonly DfsBackupBudgets GenerousBudgets = new(MaxPinAge: TimeSpan.FromHours(1), MaxPinnedBytes: 1L << 30);

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Virtual-time budget of one BAK scenario. Several rows deliberately keep a target offline or disconnected for
    /// minutes of virtual time (BAK-014: 2 min, BAK-029: 5 min, BAK-028: throttled transfer), which the default
    /// 120 s <see cref="TestClusterOptions.ScenarioTimeout"/> would cut short and report as a failure.
    /// </summary>
    public static readonly TimeSpan ScenarioBudget = TimeSpan.FromMinutes(10);

    /// <summary>Deterministic simulation with backup nodes; volume voters clipped to the node count.</summary>
    public static TestClusterOptions Options(int nodeCount = 3, int backupCount = 1) => new()
    {
        NodeCount = nodeCount,
        BackupNodeCount = backupCount,
        Mode = ClusterMode.Deterministic,
        ScenarioTimeout = ScenarioBudget,
        Volumes = [new TestVolumeSpec(Volume) { Metadata = new DfsMetadataPolicy { Voters = Math.Min(3, nodeCount) } }],
    };

    public static Task<DfsClient> ConnectLiveAsync(TestCluster cluster, string name = "c0") =>
        cluster.ConnectAsync(new TestClientOptions { Name = name }, Ct);

    public static Task<DfsClient> ConnectBackupAsync(TestCluster cluster, DfsCredentials credentials, int backupIndex = 0, string name = "bk") =>
        cluster.ConnectBackupAsync(backupIndex, new TestClientOptions { Name = name, Credentials = credentials }, Ct);

    public static Task<DfsClient> ConnectBackupReaderAsync(TestCluster cluster, int backupIndex = 0, string name = "bk") =>
        ConnectBackupAsync(cluster, cluster.BackupNodes[backupIndex].ReaderCredentials, backupIndex, name);

    public static Task RegisterAsync(TestBackupNode backup, DfsBackupBudgets? budgets = null) =>
        backup.RegisterAsync(Volume, ManualSchedule, budgets ?? GenerousBudgets);

    /// <summary>
    /// A small tree covering the layout classes: multi-extent (several 64 KiB logical chunks), one chunk + 1, inline
    /// (below the 4096-byte threshold), threshold + 1, empty file, nested and empty directories, multibyte name.
    /// </summary>
    public static TreeModel InitialTree(ulong seed)
    {
        var model = new TreeModel();
        model.Put("/docs/a.bin", TestData.Bytes(seed, (200 * 1024) + 17));
        model.Put("/docs/b.bin", TestData.Bytes(seed + 1, (64 * 1024) + 1));
        model.Put("/small.txt", Encoding.UTF8.GetBytes(TestData.Utf8Text(seed + 2, 40)));
        model.Put("/deep/x/y/z.bin", TestData.Bytes(seed + 3, 4097));
        model.Put("/empty.bin", []);
        model.Put("/名前/ü.bin", TestData.Bytes(seed + 4, 100 * 1024));
        model.AddDirectory("/emptydir");
        return model;
    }

    public static async Task WriteTreeAsync(DfsVolume volume, TreeModel model)
    {
        foreach (string dir in model.Directories)
            await volume.Directories.CreateDirectoryAsync(dir, Ct);
        foreach (KeyValuePair<string, byte[]> file in model.Files)
        {
            CommitToken token = await volume.Files.WriteAllBytesAsync(file.Key, file.Value, Ct);
            Assert.False(token.IsNone, $"WriteAllBytes({file.Key}) must return the commit token of its durable publication");
        }
    }

    public static async Task<CommitToken> PutAsync(DfsVolume volume, TreeModel model, string path, byte[] data, DfsMutationOptions? options = null)
    {
        string parent = DfsPath.GetParent(path);
        if (!string.Equals(parent, DfsPath.Root, StringComparison.Ordinal))
            await volume.Directories.CreateDirectoryAsync(parent, Ct);
        CommitToken token = options is null
            ? await volume.Files.WriteAllBytesAsync(path, data, Ct)
            : await volume.Files.WriteAllBytesAsync(path, data, options, Ct);
        Assert.False(token.IsNone, $"WriteAllBytes({path}) must return a commit token");
        model.Put(path, data);
        return token;
    }

    public static async Task DeleteFileAsync(DfsVolume volume, TreeModel model, string path)
    {
        await volume.Files.DeleteAsync(path, Ct);
        model.Remove(path);
    }

    public static async Task<DfsSnapshotInfo> SnapshotAsync(DfsVolume volume, string name)
    {
        DfsSnapshotInfo info = await volume.Snapshots.CreateAsync(name, null, Ct);
        Assert.Equal(name, info.Name);
        Assert.False(info.Boundary.IsNone, $"snapshot {name} must capture a real commit boundary");
        return info;
    }

    public static void AssertExactCheckpoint(DfsBackupCheckpoint checkpoint, string target, DfsSnapshotInfo snapshot)
    {
        Assert.Equal(target, checkpoint.Target);
        Assert.Equal(snapshot.Id, checkpoint.SnapshotId);
        Assert.Equal(snapshot.Name, checkpoint.SnapshotName);
        Assert.Equal(snapshot.Boundary, checkpoint.CoveredCommit);
        Assert.True(checkpoint.IsExactSnapshot, $"AwaitBackupSnapshot({snapshot.Name}) must report the exact snapshot contract");
    }

    /// <summary>Requests a transfer of <paramref name="snapshot"/> and waits for the exact-snapshot barrier.</summary>
    public static async Task<DfsBackupCheckpoint> SyncExactAsync(DfsVolume volume, string target, DfsSnapshotInfo snapshot)
    {
        await volume.Backups.RequestSyncAsync(target, snapshot.Name, Ct);
        DfsBackupCheckpoint checkpoint = await volume.Backups.AwaitBackupSnapshotAsync(target, snapshot.Name, SyncTimeout, Ct);
        AssertExactCheckpoint(checkpoint, target, snapshot);
        return checkpoint;
    }

    public static async Task<(DfsSnapshotInfo Snapshot, DfsBackupCheckpoint Checkpoint)> SnapshotAndSyncAsync(DfsVolume volume, string target, string name)
    {
        DfsSnapshotInfo snapshot = await SnapshotAsync(volume, name);
        DfsBackupCheckpoint checkpoint = await SyncExactAsync(volume, target, snapshot);
        return (snapshot, checkpoint);
    }

    /// <summary>Recursively compares a (snapshot) view with the model: directory set, file set, lengths and exact bytes.</summary>
    public static async Task AssertViewMatchesAsync(DfsSnapshotView view, TreeModel expected, string because)
    {
        var files = new SortedDictionary<string, DfsEntryInfo>(StringComparer.Ordinal);
        var directories = new SortedSet<string>(StringComparer.Ordinal);
        await CollectAsync(view.Directories, DfsPath.Root, files, directories);

        if (!expected.Directories.SetEquals(directories))
        {
            Assert.Fail($"{because}: directory set differs. Expected [{string.Join(", ", expected.Directories)}], " +
                        $"actual [{string.Join(", ", directories)}]");
        }

        if (!expected.Files.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(files.Keys))
        {
            Assert.Fail($"{because}: file set differs. Expected [{string.Join(", ", expected.Files.Keys)}], " +
                        $"actual [{string.Join(", ", files.Keys)}]");
        }

        foreach (KeyValuePair<string, byte[]> file in expected.Files)
        {
            DfsEntryInfo entry = files[file.Key];
            Assert.Equal(DfsEntryKind.File, entry.Kind);
            Assert.True(entry.Length == file.Value.Length,
                $"{because}: {file.Key} enumerated length {entry.Length}, expected {file.Value.Length}");
            DfsFileInfo info = await view.Files.GetInfoAsync(file.Key, Ct);
            Assert.True(info.Length == file.Value.Length, $"{because}: {file.Key} GetInfo length {info.Length}, expected {file.Value.Length}");
            byte[] actual = await view.Files.ReadAllBytesAsync(file.Key, Ct);
            AssertEx.BytesEqual(file.Value, actual, $"{because}: {file.Key}");
        }
    }

    private static async Task CollectAsync(DfsDirectoryClient directoryClient, string path, SortedDictionary<string, DfsEntryInfo> files,
        SortedSet<string> directories)
    {
        await foreach (DfsEntryInfo entry in directoryClient.EnumerateAsync(path, null, Ct))
        {
            Assert.True(string.Equals(DfsPath.GetParent(entry.Path), path, StringComparison.Ordinal),
                $"entry {entry.Path} enumerated under {path}");
            if (entry.Kind == DfsEntryKind.Directory)
            {
                Assert.True(directories.Add(entry.Path), $"directory {entry.Path} enumerated twice");
                await CollectAsync(directoryClient, entry.Path, files, directories);
            }
            else
            {
                Assert.True(files.TryAdd(entry.Path, entry), $"file {entry.Path} enumerated twice");
            }
        }
    }

    /// <summary>Opens <paramref name="snapshot"/> through a backup client and compares it with the model.</summary>
    public static async Task AssertBackupSnapshotAsync(DfsClient backupClient, string snapshot, TreeModel expected, string because)
    {
        await using DfsSnapshotView view = await backupClient.GetVolume(Volume).Snapshots.OpenAsync(snapshot, Ct);
        Assert.Equal(snapshot, view.Info.Name);
        await AssertViewMatchesAsync(view, expected, because);
    }

    /// <summary>Asserts the backup node's own published state (pointer, retained list) and that each retained snapshot verifies.</summary>
    public static async Task AssertPublishedStateAsync(TestBackupNode backup, string? expectedPointer, IEnumerable<string> present,
        IEnumerable<string> absent, string because)
    {
        string? pointer = await backup.Inspect.GetPublishedPointerAsync(Volume);
        Assert.True(string.Equals(pointer, expectedPointer, StringComparison.Ordinal),
            $"{because}: published pointer of {backup.Name} is '{pointer ?? "<none>"}', expected '{expectedPointer ?? "<none>"}'");
        IReadOnlyList<string> published = await backup.Inspect.GetPublishedSnapshotsAsync(Volume);
        foreach (string name in present)
        {
            Assert.True(published.Contains(name, StringComparer.Ordinal),
                $"{because}: {name} must be published on {backup.Name}; published = [{string.Join(", ", published)}]");
            IReadOnlyList<ObjectId> bad = await backup.Inspect.VerifyClosureAsync(Volume, name);
            Assert.True(bad.Count == 0, $"{because}: published {name} on {backup.Name} has missing/corrupt closure objects [{string.Join(", ", bad)}]");
        }

        foreach (string name in absent)
        {
            Assert.False(published.Contains(name, StringComparer.Ordinal),
                $"{because}: {name} must not be published on {backup.Name}; published = [{string.Join(", ", published)}]");
        }
    }

    /// <summary>Asserts the snapshot is not selectable through the backup's client surface (list, get, open).</summary>
    public static async Task AssertNotSelectableAsync(DfsClient backupClient, string snapshot, string because)
    {
        DfsSnapshotClient snapshots = backupClient.GetVolume(Volume).Snapshots;
        IReadOnlyList<DfsSnapshotInfo> listed = await snapshots.ListAsync(Ct);
        Assert.False(listed.Any(s => string.Equals(s.Name, snapshot, StringComparison.Ordinal)),
            $"{because}: {snapshot} must not be listed; listed = [{string.Join(", ", listed.Select(s => s.Name))}]");
        await AssertEx.ThrowsDfsAsync<DfsException>(() => snapshots.GetAsync(snapshot, Ct), DfsErrorCode.SnapshotNotFound);
        await AssertEx.ThrowsDfsAsync<DfsException>(async () =>
        {
            await using DfsSnapshotView view = await snapshots.OpenAsync(snapshot, Ct);
        }, DfsErrorCode.SnapshotNotFound);
    }

    /// <summary>Union of the object ids referenced by the current extents of <paramref name="paths"/> on the live volume.</summary>
    public static async Task<HashSet<ObjectId>> FileObjectsAsync(TestCluster cluster, IEnumerable<string> paths)
    {
        var set = new HashSet<ObjectId>();
        foreach (string path in paths)
        {
            FileInspection? file = await cluster.Inspect.GetFileAsync(Volume, path);
            Assert.True(file is not null, $"live volume has no file {path}");
            foreach (ExtentInspection extent in file.Extents)
                set.Add(extent.Object);
        }

        return set;
    }

    public static async Task<HashSet<ObjectId>> BackupObjectsAsync(TestBackupNode backup) =>
        (await backup.Inspect.ListObjectsAsync()).Select(r => r.Id).ToHashSet();

    /// <summary>Verified object records held by running live nodes, by id (first holder wins).</summary>
    public static async Task<Dictionary<ObjectId, ObjectRecordInfo>> LiveObjectRecordsAsync(TestCluster cluster)
    {
        var records = new Dictionary<ObjectId, ObjectRecordInfo>();
        foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
        {
            foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Volume))
                records.TryAdd(record.Id, record);
        }

        return records;
    }

    public static ObjectId Lowest(IEnumerable<ObjectId> ids) => ids.OrderBy(i => i.Value).First();

    public static ObjectId Highest(IEnumerable<ObjectId> ids) => ids.OrderBy(i => i.Value).Last();

    /// <summary>Cuts every link between the backup node and every live node (clients keep their links).</summary>
    public static void CutFromSources(TestCluster cluster, string backupName)
    {
        foreach (TestNode node in cluster.Nodes)
            cluster.Network.CutBoth(node.Name, backupName);
    }

    public static void RestoreToSources(TestCluster cluster, string backupName)
    {
        foreach (TestNode node in cluster.Nodes)
        {
            cluster.Network.Restore(node.Name, backupName);
            cluster.Network.Restore(backupName, node.Name);
        }
    }

    public static void BlackHoleFromSources(TestCluster cluster, string backupName)
    {
        foreach (TestNode node in cluster.Nodes)
        {
            cluster.Network.BlackHole(node.Name, backupName);
            cluster.Network.BlackHole(backupName, node.Name);
        }
    }

    /// <summary>All recorded fault-site hits for <paramref name="site"/> (optionally on one node).</summary>
    public static IReadOnlyList<FaultContext> Hits(TestCluster cluster, string site, string? node = null) =>
        cluster.Faults.Coverage.Hits
            .Where(h => string.Equals(h.Site, site, StringComparison.Ordinal) && (node is null || string.Equals(h.Node, node, StringComparison.Ordinal)))
            .ToList();

    /// <summary>Runs <paramref name="action"/> and returns its duration on the harness clock (virtual in deterministic mode).</summary>
    public static async Task<TimeSpan> TimeAsync(TestCluster cluster, Func<Task> action)
    {
        long start = cluster.Time.GetTimestamp();
        await action();
        return cluster.Time.GetElapsedTime(start);
    }

    /// <summary>Asserts a small live commit completes within <see cref="LiveCommitBound"/> and is readable afterwards.</summary>
    public static async Task<CommitToken> AssertLiveCommitUnblockedAsync(TestCluster cluster, DfsVolume volume, TreeModel? model, string path, byte[] data, string because)
    {
        CommitToken token = CommitToken.None;
        TimeSpan elapsed = await TimeAsync(cluster, async () =>
        {
            token = model is null ? await volume.Files.WriteAllBytesAsync(path, data, Ct) : await PutAsync(volume, model, path, data);
        });
        Assert.False(token.IsNone, $"{because}: live commit of {path} must succeed");
        Assert.True(elapsed <= LiveCommitBound, $"{because}: live commit of {path} took {elapsed}, bound {LiveCommitBound}");
        byte[] readBack = await volume.Files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(data, readBack, $"{because}: {path} read back");
        return token;
    }

    public static async Task AssertFileBytesAsync(DfsFileClient files, string path, byte[] expected, string because)
    {
        byte[] actual = await files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(expected, actual, $"{because}: {path}");
    }

    public static bool IsAccessDenial(Exception ex) =>
        ex is UnauthorizedAccessException or DfsReadOnlyException;

    /// <summary>Runs <paramref name="action"/> expecting it to be refused; returns the exception.</summary>
    public static async Task<Exception> AssertRefusedAsync(Func<Task> action, Func<Exception, bool> acceptable, string because)
    {
        try
        {
            await action();
        }
        catch (Exception ex) when (acceptable(ex))
        {
            return ex;
        }
        catch (Exception ex)
        {
            Assert.Fail($"{because}: unexpected exception {ex.GetType().FullName} ({DfsErrors.ToCode(ex)}): {ex}");
            throw;
        }

        Assert.Fail($"{because}: the operation succeeded but must have been refused");
        throw new InvalidOperationException("unreachable");
    }
}
