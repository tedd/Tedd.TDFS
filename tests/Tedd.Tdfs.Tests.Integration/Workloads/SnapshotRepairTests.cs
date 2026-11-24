using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Integration.Str.StrHelpers;

namespace Tedd.Tdfs.Tests.Integration.Workloads;

public sealed class SnapshotRepairTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task ScrubAndRepairRecoverAReplicaReferencedOnlyByASnapshot(bool namedStream) =>
        ClusterTest.RunAsync(RealBoundaries() with
        {
            Timings = NodeTimings.Fast with { ScrubInterval = TimeSpan.FromHours(1) },
        }, async cluster =>
        {
            const string file = "/history";
            string path = namedStream ? file + ":previous" : file;
            byte[] initial = TestData.Bytes(3301, 3 * 65536 + 23);
            byte[] replacement = TestData.Bytes(3302, 113);
            await using DfsClient client = await ConnectAsync(cluster, "publisher");
            DfsVolume volume = client.GetVolume(Vol);
            await volume.Files.WriteAllBytesAsync(path, initial, Ct);
            FileInspection? layout = await cluster.Inspect.GetFileAsync(Vol, file);
            Assert.NotNull(layout);
            var extents = namedStream ? Assert.Single(layout.Streams).Extents : layout.Extents;
            ObjectId id = extents[0].Object;
            await volume.Snapshots.CreateAsync("retained", ct: Ct);
            await volume.Files.DeleteAsync(file, Ct);
            await volume.Files.WriteAllBytesAsync(file, replacement, Ct);
            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(id));

            TestNode damaged = cluster.Nodes[0];
            long failures = (await damaged.GetCountersAsync()).ChecksumFailures;
            using (cluster.Network.Hold(new MessageFilter(Type: MessageType.ReplicateObject)))
            {
                await damaged.Disk.CorruptObjectAsync(id, offset: 5);
                await cluster.Admin.StartScrubAsync(damaged.Name, Ct);
                await cluster.WaitUntilAsync(async () => (await damaged.GetCountersAsync()).ChecksumFailures > failures,
                    TimeSpan.FromSeconds(30), "scrub detected the corrupt snapshot object before a client read it", Ct);
                Assert.Equal((2, 2), await cluster.Inspect.CountDurableDomainsAsync(id));
                Assert.DoesNotContain(damaged.Name, await cluster.Inspect.GetObjectHoldersAsync(id));
                await using DfsSnapshotView snapshot = await volume.Snapshots.OpenAsync("retained", Ct);
                BytesEqual(initial, await snapshot.Files.ReadAllBytesAsync(path, Ct), "snapshot served from healthy replicas");
                byte[] progress = TestData.Bytes(3303, 19);
                await volume.Files.WriteAllBytesAsync("/foreground", progress, Ct);
                BytesEqual(progress, await volume.Files.ReadAllBytesAsync("/foreground", Ct), "foreground commit while repair is held");
            }
            await cluster.Admin.StartRepairAsync(Vol, Ct);
            await cluster.WaitUntilAsync(async () => (await cluster.Inspect.GetObjectHoldersAsync(id)).Contains(damaged.Name),
                TimeSpan.FromSeconds(60), "the repaired node durably holds a verified copy of the snapshot object", Ct);
            Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(id));
            await damaged.CrashAsync();
            await damaged.RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            Assert.Contains(damaged.Name, await cluster.Inspect.GetObjectHoldersAsync(id));
            await using DfsClient fresh = await ConnectAsync(cluster, "reader");
            DfsVolume reopened = fresh.GetVolume(Vol);
            await using DfsSnapshotView retained = await reopened.Snapshots.OpenAsync("retained", Ct);
            BytesEqual(initial, await retained.Files.ReadAllBytesAsync(path, Ct), "snapshot after restart");
            BytesEqual(replacement, await reopened.Files.ReadAllBytesAsync(file, Ct), "recreated live file after restart");
            Assert.Empty(await reopened.Files.ListStreamsAsync(file, Ct));
        }, Ct);
}
