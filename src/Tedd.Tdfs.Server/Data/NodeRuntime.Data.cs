using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Admin;
using Tedd.Tdfs.Server.Data;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Data-plane plug-in of the node runtime (INTERNALS.md §1.3): the maintenance plane's handlers (<c>ReplicateObject</c>,
/// <c>ReportUsage</c>, the scrub and repair admin operations) and its node-wide jobs (inventory reports and GC sweeps,
/// usage reports, repair passes of the volumes this node leads, scrubbing, compaction, the drain transition).
/// </summary>
internal sealed partial class NodeRuntime
{
    /// <summary>
    /// Object segment size for a node: a quarter of its reserve (1 MiB to 64 MiB). Compaction rewrites one segment at a
    /// time, so its extra space need stays a fraction of the part of the reserve that repair never uses.
    /// </summary>
    internal static long SegmentSizeFor(NodeOptions options) => Math.Clamp(options.ReservedBytes / 4, 1L << 20, 64L << 20);

    partial void RegisterDataHandlers(NodeServices services)
    {
        var plane = new MaintenancePlane(services);
        services.Maintenance = plane;
        plane.Repair.Register(services.Rpc);
        plane.Control.Register(services.Rpc);

        AdminService admin = services.Admin;
        admin.PoolHealthProvider = plane.Control.PoolHealth;
        admin.PoolUsageProvider = plane.Control.PoolUsedBytes;
        admin.PoolStrayHoldersProvider = plane.Control.StrayHolders;
        admin.NodeUsageProvider = plane.Control.UsedBytesOf;
        admin.RemovalCheck = plane.Control.CheckRemoval;
        admin.RegisterVolumeOperation(AdminOp.StartScrub, admin.StartScrubAsync);
        var router = new VolumeRequestRouter(services);
        admin.RegisterVolumeOperation(AdminOp.StartRepair, router.Admin((svc, ctx, m) => svc.StartRepairAsync(ctx, m)));
    }

    partial void StartDataJobs(NodeServices services)
    {
        MaintenancePlane plane = services.Maintenance ?? throw new InvalidOperationException("The maintenance plane is not registered.");
        NodeTimings t = services.Timings;

        // Inventory reports drive garbage collection and repair: the volume leaders classify, this node sweeps (ARCHITECTURE §8).
        services.Jobs.StartPeriodic("inventory", t.InventoryInterval, plane.Inventory.RunAsync, t.InventoryInterval);
        services.Jobs.StartPeriodic("usage", t.InventoryInterval, plane.Usage.RunAsync, t.InventoryInterval);
        services.Jobs.StartPeriodic("repair", t.InventoryInterval, () => RepairPassAsync(services), t.InventoryInterval);
        services.Jobs.StartPeriodic("drain", t.InventoryInterval, plane.Control.RunAsync, t.InventoryInterval * 2);
        services.Jobs.StartPeriodic("scrub", plane.Scrubber.Step, plane.Scrubber.RunAsync);
        TimeSpan compaction = TimeSpan.FromTicks(Math.Max(t.GcInterval.Ticks * 4, TimeSpan.FromMilliseconds(500).Ticks));
        services.Jobs.StartPeriodic("compaction", compaction, () => CompactAsync(services));
    }

    /// <summary>One repair pass of every volume this node leads.</summary>
    private static async Task RepairPassAsync(NodeServices services)
    {
        foreach (VolumeHost host in services.Volumes.Hosts.ToArray())
        {
            if (services.IsStopping)
                return;
            if (host.Service is { IsLeader: true } service && !host.Group.IsClosed)
                await service.RepairPassAsync();
        }
    }

    /// <summary>
    /// Scheduled compaction: segments whose reclaimable bytes reach a quarter of a segment are rewritten (any reclaimable
    /// byte once free space for payload is scarce), oldest first, one segment per step.
    /// </summary>
    private static async Task CompactAsync(NodeServices services)
    {
        if (!services.IsServing || services.IsStopping)
            return;
        ObjectStore store = services.ObjectStore;
        if (store.DeadBytes <= 0)
            return;
        long segment = SegmentSizeFor(services.Options);
        double threshold = store.AvailableBytes(ObjectAdmission.Normal) < 2 * segment ? 0 : 0.25;
        int compacted = await store.CompactSegmentsAsync(threshold);
        if (compacted > 0)
            services.Metrics.SetStored(store.ObjectCount, store.LiveBytes);
    }
}
