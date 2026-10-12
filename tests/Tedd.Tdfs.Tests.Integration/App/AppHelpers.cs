using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.SnpApp;

namespace Tedd.Tdfs.Tests.Integration.App;

/// <summary>Shared helpers of the APP integration tests (real QUIC transport and real disk storage).</summary>
internal static class AppHelpers
{
    public const string Volume = "vol";

    /// <summary>Three nodes one per zone over QUIC with on-disk storage (real process boundaries for adapters and data).</summary>
    public static TestClusterOptions RealBoundaries(int backups = 0) => TestClusterOptions.ThreeZones with
    {
        Transport = TransportKind.Quic,
        Storage = StorageKind.Disk,
        BackupNodeCount = backups,
    };

    /// <summary>
    /// Arms a probe that records <paramref name="label"/> (source "server", detail = node) into the timeline at the first
    /// <paramref name="count"/> hits of a fault site, without otherwise affecting the operation.
    /// </summary>
    public static FaultArm ArmProbe(TestCluster cluster, AdapterTimeline timeline, string site, DfsOperationKind? operation, string label, int count = 1) =>
        cluster.Faults.Arm(new FaultMatch(site, Operation: operation, Count: count), FaultAction.Custom(context =>
        {
            timeline.Record("server", label, context.Node);
            return Task.CompletedTask;
        }));

    /// <summary>True when <paramref name="exception"/> or any inner/aggregated exception satisfies <paramref name="predicate"/>.</summary>
    public static bool ChainContains(Exception? exception, Func<Exception, bool> predicate)
    {
        if (exception is null)
            return false;
        if (predicate(exception))
            return true;
        if (exception is AggregateException aggregate && aggregate.InnerExceptions.Any(e => ChainContains(e, predicate)))
            return true;
        return ChainContains(exception.InnerException, predicate);
    }

    /// <summary>Runs <paramref name="action"/> and returns its exception, or null.</summary>
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

    /// <summary>Asserts the records describe exactly the given adapters (id, version and restore requirements).</summary>
    public static void AssertAdapterRecords(IEnumerable<IDfsSnapshotAdapter> expected, IReadOnlyList<DfsAdapterRecord> actual, string because)
    {
        IDfsSnapshotAdapter[] adapters = expected.ToArray();
        Assert.True(adapters.Length == actual.Count, $"{because}: {actual.Count} adapter records, expected {adapters.Length}");
        foreach (IDfsSnapshotAdapter adapter in adapters)
        {
            DfsAdapterRecord? record = actual.FirstOrDefault(r => string.Equals(r.Id, adapter.Id, StringComparison.Ordinal));
            Assert.True(record is not null, $"{because}: no record for adapter '{adapter.Id}'");
            Assert.Equal(adapter.Version, record!.Version);
            Assert.Equal(
                adapter.RestoreRequirements.OrderBy(p => p.Key, StringComparer.Ordinal).ToArray(),
                record.RestoreRequirements.OrderBy(p => p.Key, StringComparer.Ordinal).ToArray());
        }
    }

    /// <summary>Fails if a snapshot named <paramref name="name"/> exists with the ApplicationConsistent label.</summary>
    public static async Task AssertNotApplicationConsistentAsync(DfsVolume vol, string name, CancellationToken ct)
    {
        IReadOnlyList<DfsSnapshotInfo> list = await vol.Snapshots.ListAsync(ct);
        DfsSnapshotInfo? existing = list.FirstOrDefault(s => s.Name == name);
        if (existing is null)
            return;
        Assert.True(existing.Label == DfsConsistencyLabel.FilesystemConsistent,
            $"snapshot '{name}' exists after an incomplete adapter run and must not claim application consistency (label {existing.Label})");
        Assert.Empty(existing.Adapters);
    }

    /// <summary>Continuously updates the application until <paramref name="stop"/> is canceled (single writer).</summary>
    public static async Task WriterLoopAsync(LogSampleApp app, ulong seed, CancellationToken stop)
    {
        int n = 0;
        while (!stop.IsCancellationRequested)
        {
            await app.UpdateAsync($"k{n % 9}", TestData.Bytes(seed + (ulong)n, 64 + (n % 5 * 300)), CancellationToken.None);
            n++;
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(5), stop);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>Opens a snapshot view and runs the sample application's recovery on it (read-only).</summary>
    public static async Task<LogRecoveryResult> RecoverFromSnapshotAsync(DfsVolume vol, string snapshot, string root, CancellationToken ct)
    {
        await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(snapshot, ct);
        return await LogSampleApp.RecoverAsync(view.Files, root, false, ct);
    }
}
