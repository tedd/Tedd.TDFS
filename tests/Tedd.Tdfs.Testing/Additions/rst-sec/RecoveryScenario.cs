namespace Tedd.Tdfs.Testing;

/// <summary>A populated source volume whose snapshot has been durably published on a backup target.</summary>
/// <param name="Client">The source client that populated the volume (owned by the scenario).</param>
/// <param name="Volume">The source volume facade.</param>
/// <param name="Info">Source volume info captured after publication.</param>
/// <param name="Expected">Independent manifest of the snapshot's content.</param>
/// <param name="SnapshotCapture">The source's own snapshot view, captured before any disaster.</param>
/// <param name="Snapshot">The published snapshot.</param>
/// <param name="Checkpoint">The backup checkpoint returned by the exact-snapshot barrier.</param>
/// <param name="Backup">The backup node holding the snapshot.</param>
public sealed record BackedUpSource(
    DfsClient Client,
    DfsVolume Volume,
    DfsVolumeInfo Info,
    ExpectedVolumeContent Expected,
    ObservedVolumeContent SnapshotCapture,
    DfsSnapshotInfo Snapshot,
    DfsBackupCheckpoint Checkpoint,
    TestBackupNode Backup);

/// <summary>
/// Shared steps of recovery scenarios (RST/SEC suites), expressed only through the public SDK and harness: populate and
/// publish a source snapshot, destroy the source, build restore requests, and assert recovery identities.
/// </summary>
public static class RecoveryScenario
{
    /// <summary>Default name of the source volume.</summary>
    public const string Volume = "vol";

    /// <summary>Bound for backup barriers in recovery scenarios.</summary>
    public static readonly TimeSpan BackupBarrierTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Manual schedule: the target only syncs on explicit <see cref="DfsBackupClient.RequestSyncAsync"/>.</summary>
    public static DfsBackupSchedule ManualSchedule { get; } = new(Interval: null, OnEverySnapshot: false);

    /// <summary>
    /// Connects a source client, populates <see cref="Volume"/> with <see cref="VolumeContentFixture"/>, registers the
    /// first backup node, creates <paramref name="snapshotName"/>, publishes it with the exact-snapshot barrier and
    /// verifies the source's own snapshot view against the independent manifest.
    /// </summary>
    public static async Task<BackedUpSource> CreateBackedUpSourceAsync(TestCluster cluster, string snapshotName, ulong seed,
        CancellationToken ct, DfsBackupSchedule? schedule = null, DfsSnapshotOptions? snapshotOptions = null, string clientName = "source")
    {
        ArgumentNullException.ThrowIfNull(cluster);
        Assert.True(cluster.BackupNodes.Count > 0, "the scenario needs a backup node");
        DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = clientName }, ct);
        DfsVolume volume = client.GetVolume(Volume);
        ExpectedVolumeContent expected = await VolumeContentFixture.PopulateAsync(volume, seed, ct);
        TestBackupNode backup = cluster.BackupNodes[0];
        await backup.RegisterAsync(Volume, schedule ?? ManualSchedule);
        DfsSnapshotInfo snapshot = await volume.Snapshots.CreateAsync(snapshotName, snapshotOptions, ct);
        DfsBackupCheckpoint checkpoint = await PublishAsync(volume, backup, snapshot, ct);

        ObservedVolumeContent capture;
        await using (DfsSnapshotView view = await volume.Snapshots.OpenAsync(snapshotName, ct))
            capture = await VolumeContentFixture.CaptureAsync(view, ct);
        VolumeContentFixture.AssertMatches(expected, capture, "source snapshot view before any disaster");

        DfsVolumeInfo info = await volume.GetInfoAsync(ct);
        return new BackedUpSource(client, volume, info, expected, capture, snapshot, checkpoint, backup);
    }

    /// <summary>
    /// Requests synchronisation of <paramref name="snapshot"/> to <paramref name="backup"/> and waits for the exact-snapshot
    /// barrier; asserts the checkpoint names exactly that snapshot and its boundary.
    /// </summary>
    public static async Task<DfsBackupCheckpoint> PublishAsync(DfsVolume volume, TestBackupNode backup, DfsSnapshotInfo snapshot, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(volume);
        ArgumentNullException.ThrowIfNull(backup);
        ArgumentNullException.ThrowIfNull(snapshot);
        await volume.Backups.RequestSyncAsync(backup.TargetName, snapshot.Name, ct);
        DfsBackupCheckpoint checkpoint = await volume.Backups.AwaitBackupSnapshotAsync(backup.TargetName, snapshot.Name, BackupBarrierTimeout, ct);
        Assert.Equal(backup.TargetName, checkpoint.Target);
        Assert.True(checkpoint.IsExactSnapshot, $"the exact-snapshot barrier for {snapshot.Name} returned a non-exact checkpoint");
        Assert.Equal(snapshot.Name, checkpoint.SnapshotName);
        Assert.Equal(snapshot.Id, checkpoint.SnapshotId);
        Assert.Equal(snapshot.Boundary, checkpoint.CoveredCommit);
        return checkpoint;
    }

    /// <summary>
    /// Destroys every source service: disposes the source client (dropping client caches), isolates every live node,
    /// crashes it and replaces its disk with an empty one. Backup nodes are untouched. Returns the per-node contact-attempt
    /// counts after destruction (baseline for <see cref="AssertNoSourceContact"/>).
    /// </summary>
    public static async Task<IReadOnlyDictionary<string, long>> DestroySourceAsync(TestCluster cluster, DfsClient? sourceClient)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        if (sourceClient is not null)
            await sourceClient.DisposeAsync();
        foreach (TestNode node in cluster.Nodes.ToArray())
        {
            cluster.Network.Isolate(node.Name);
            await node.CrashAsync();
            await node.ReplaceDiskWithEmptyAsync();
            Assert.Equal(TestNodeState.Crashed, node.State);
        }

        return cluster.Nodes.ToDictionary(n => n.Name, n => cluster.Network.CountAttemptsTo(n.Name), StringComparer.Ordinal);
    }

    /// <summary>Asserts no connection attempt or message was addressed to any destroyed source node since the baseline.</summary>
    public static void AssertNoSourceContact(TestCluster sourceCluster, IReadOnlyDictionary<string, long> baseline, string during)
    {
        ArgumentNullException.ThrowIfNull(sourceCluster);
        ArgumentNullException.ThrowIfNull(baseline);
        foreach ((string node, long before) in baseline)
        {
            long after = sourceCluster.Network.CountAttemptsTo(node);
            Assert.True(after == before,
                $"undeclared source dependency: {after - before} contact attempt(s) addressed to destroyed source node {node} {during}");
        }
    }

    /// <summary>A restore request for <paramref name="snapshot"/> of <see cref="Volume"/> from <paramref name="backup"/>, authenticated as its reader.</summary>
    public static DfsRestoreFromBackupRequest RestoreRequest(TestBackupNode backup, string snapshot, string newVolume, byte[]? recoveryKey = null,
        string sourceVolume = Volume)
    {
        ArgumentNullException.ThrowIfNull(backup);
        return new DfsRestoreFromBackupRequest([backup.Endpoint], backup.ReaderCredentials, sourceVolume, snapshot, newVolume, recoveryKey);
    }

    /// <summary>Asserts two tokens belong to different authorities: not comparable in either direction and ordering throws.</summary>
    public static void AssertNotComparable(CommitToken a, CommitToken b, string because)
    {
        Assert.False(a.IsComparableWith(b), $"{a} must not be comparable with {b}: {because}");
        Assert.False(b.IsComparableWith(a), $"{b} must not be comparable with {a}: {because}");
        Assert.Throws<ArgumentException>(() => a.CompareTo(b));
        Assert.Throws<ArgumentException>(() => b.IsAtOrAfter(a));
    }

    /// <summary>
    /// Asserts a restored volume is a new authority: its name, a new <see cref="VolumeId"/>, a non-None last commit owned by
    /// <paramref name="clusterId"/>/the new volume/its epoch, tokens not comparable with the source's, and the source's
    /// data policies, inline threshold and limits carried over unchanged.
    /// </summary>
    public static void AssertRestoredIdentity(DfsVolumeInfo restored, DfsVolumeInfo source, Guid clusterId, string expectedName)
    {
        ArgumentNullException.ThrowIfNull(restored);
        ArgumentNullException.ThrowIfNull(source);
        Assert.Equal(expectedName, restored.Name);
        Assert.NotEqual(source.Id, restored.Id);
        Assert.False(restored.LastCommit.IsNone, "a restored volume must report its activation commit");
        Assert.Equal(clusterId, restored.LastCommit.ClusterId);
        Assert.Equal(restored.Id, restored.LastCommit.VolumeId);
        Assert.Equal(restored.Epoch, restored.LastCommit.VolumeEpoch);
        AssertNotComparable(restored.LastCommit, source.LastCommit, "the restored volume is a new authority");
        Assert.Equal(source.DefaultPolicy, restored.DefaultPolicy);
        Assert.Equal(source.InlineThreshold, restored.InlineThreshold);
        Assert.Equal(source.Limits, restored.Limits);
        Assert.Equal(source.NamedPolicies.Keys.Order(StringComparer.Ordinal), restored.NamedPolicies.Keys.Order(StringComparer.Ordinal));
        foreach ((string name, DfsStoragePolicy policy) in source.NamedPolicies)
            Assert.Equal(policy, restored.NamedPolicies[name]);
    }

    /// <summary>Reads <paramref name="path"/> completely and asserts its exact bytes.</summary>
    public static async Task AssertFileBytesAsync(DfsFileClient files, string path, byte[] expected, string because, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(files);
        byte[] actual = await files.ReadAllBytesAsync(path, ct);
        AssertEx.BytesEqual(expected, actual, $"{path}: {because}");
    }

    /// <summary>Runs <paramref name="action"/> and returns its exception (null on success).</summary>
    public static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
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

    /// <summary>The canonical error code of an outcome (<see cref="DfsErrorCode.None"/> for success).</summary>
    public static DfsErrorCode CodeOf(Exception? ex) => ex is null ? DfsErrorCode.None : DfsErrors.ToCode(ex);

    /// <summary>Asserts <paramref name="ex"/> failed with one of <paramref name="allowed"/> codes (success is a failure).</summary>
    public static void AssertFailedWith(Exception? ex, string because, params DfsErrorCode[] allowed)
    {
        Assert.True(ex is not null, $"expected a failure with one of [{string.Join(", ", allowed)}] but the operation succeeded: {because}");
        DfsErrorCode code = DfsErrors.ToCode(ex);
        Assert.True(allowed.Contains(code), $"expected one of [{string.Join(", ", allowed)}] but got {code} ({ex.GetType().Name}: {ex.Message}): {because}");
    }

    /// <summary>
    /// Waits for <paramref name="task"/> on the cluster clock (virtual in deterministic mode) and returns its exception
    /// (null on success); fails the test if it does not complete within <paramref name="timeout"/>.
    /// </summary>
    public static async Task<Exception?> AwaitOutcomeAsync(TestCluster cluster, Task task, TimeSpan timeout, string because, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(task);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task delay = cluster.DelayAsync(timeout, cts.Token);
        Task first = await Task.WhenAny(task, delay);
        if (first != task)
            Assert.Fail($"operation did not complete within {timeout}: {because}");
        cts.Cancel();
        try
        {
            await task;
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
