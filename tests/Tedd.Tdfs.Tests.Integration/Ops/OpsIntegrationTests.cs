using System.Diagnostics;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Ops;

/// <summary>
/// OPS suite, level I: health and status endpoints polled aggressively over real QUIC transport and on-disk storage
/// (FSD §§7, 13, 15; test plan §7.26).
/// </summary>
public sealed class OpsIntegrationTests
{
    private const string Vol = OpsHelpers.Volume;

    private static CancellationToken Ct => OpsHelpers.Ct;

    /// <summary>
    /// Health, readiness and status endpoints are polled back-to-back by two admin clients throughout. (1) Polling alone
    /// issues no durability receipts and writes no objects. (2) A writer process dies holding a writable handle with
    /// buffered bytes: despite the polling its lease expires within a bounded multiple of the lease duration, another
    /// writer can then open the file, and the dead writer's buffer is never published. (3) A paused node and the
    /// under-replication of writes made while it is paused are reported, not hidden by the polling traffic.
    /// </summary>
    [Fact, Catalog("OPS-016", "I", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_016_HealthChecksDoNotMutateAuthority()
    {
        await ClusterTest.RunAsync(OpsHelpers.QuicDisk(), async cluster =>
        {
            TimeSpan lease = cluster.Options.Timings.LeaseDuration;
            await using DfsClient monitor = await cluster.ConnectAsync(new TestClientOptions { Name = "monitor" }, Ct);
            await using DfsClient other = await cluster.ConnectAsync(new TestClientOptions { Name = "other" }, Ct);
            DfsVolume otherVol = other.GetVolume(Vol);
            const string path = "/ops016/file";
            byte[] original = TestData.Bytes(1600, 10 * 1024);
            await otherVol.Directories.CreateDirectoryAsync("/ops016", Ct);
            await otherVol.Files.WriteAllBytesAsync(path, original, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            HealthPoller poller = new([monitor.Admin, cluster.Admin], Vol);
            try
            {
                // (1) Observation creates no durability evidence.
                NodeCounters before = await cluster.Inspect.GetTotalCountersAsync();
                await Task.Delay(TimeSpan.FromSeconds(2), Ct);
                NodeCounters after = await cluster.Inspect.GetTotalCountersAsync();
                Assert.True(poller.Calls > 20, $"the poller must have been busy ({poller.Calls} calls)");
                Assert.Equal(before.ReceiptsIssued, after.ReceiptsIssued);
                Assert.Equal(before.ObjectPuts, after.ObjectPuts);
                Assert.Equal(before.ObjectBytesStored, after.ObjectBytesStored);

                // (2) A dead writer's lease is not kept alive by health traffic.
                DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, Ct);
                DfsStream stream = await writer.GetVolume(Vol).Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
                await stream.WriteAsync(TestData.Bytes(1601, 4096), Ct);
                InodeId inode = stream.InodeId;
                VolumeStateInspection held = await cluster.Inspect.GetVolumeStateAsync(Vol);
                HandleId deadHandle = Assert.Single(held.Handles, h => h.Inode == inode && h.IsWriter && !h.Expired).Id;
                await AssertEx.ThrowsDfsAsync<DfsSharingViolationException>(
                    () => otherVol.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, Ct));

                var sinceDeath = Stopwatch.StartNew();
                cluster.KillClient(writer);
                await cluster.WaitUntilAsync(async () =>
                {
                    try
                    {
                        await using DfsStream replacement = await otherVol.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
                        return true;
                    }
                    catch (DfsSharingViolationException)
                    {
                        return false;
                    }
                }, lease * 3 + TimeSpan.FromSeconds(2), "the dead writer's lease must expire although status endpoints are polled continuously", Ct);
                OpsHelpers.Log($"lease {lease}; replacement writer admitted {sinceDeath.Elapsed} after the writer died; {poller.Calls} health calls so far");
                VolumeStateInspection expired = await cluster.Inspect.GetVolumeStateAsync(Vol);
                Assert.DoesNotContain(expired.Handles, h => h.Id == deadHandle && !h.Expired);
                OpsHelpers.BytesEqual(original, await otherVol.Files.ReadAllBytesAsync(path, Ct), "the dead writer's buffered bytes must never be published");

                // (3) Stale state is visible, not hidden by observation traffic.
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                TestNode paused = cluster.Nodes.First(n => n.Name != leader.Name);
                await paused.PauseAsync();
                await cluster.WaitUntilAsync(async () => !(await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == paused.Name).IsReachable,
                    TimeSpan.FromSeconds(15), "a paused node must be reported unreachable while health endpoints are polled", Ct);
                await otherVol.Files.WriteAllBytesAsync("/ops016/while-paused", TestData.Bytes(1602, 128 * 1024), Ct);
                await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetReplicaHealthAsync(Vol, Ct)).UnderReplicatedObjects >= 1,
                    TimeSpan.FromSeconds(15), "writes made while a replica is paused must be reported under-replicated", Ct);
                await paused.ResumeAsync();
                await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            }
            finally
            {
                await poller.DisposeAsync();
            }

            Assert.Empty(poller.Errors);
        }, Ct);
    }
}
