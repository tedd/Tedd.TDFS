using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// The node's maintenance plane (ARCHITECTURE §6, §8, §14): inventory reports and GC sweeps, repair copies, scrubbing,
/// usage reports to the control leader, and — while this node leads the control group — the aggregation of those reports
/// (<see cref="MaintenanceControl"/>). The volume-leader side (replica tracking and the repair coordinator) lives in the
/// volume service. Created when the data handlers are registered; every member runs on the node loop.
/// </summary>
internal sealed class MaintenancePlane
{
    public MaintenancePlane(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        Inventory = new InventoryReporter(services);
        Repair = new RepairWorker(services);
        Scrubber = new Scrubber(services);
        Usage = new UsageReporter(services);
        Control = new MaintenanceControl(services);
    }

    /// <summary>Inventory reports and GC sweeps of the local store.</summary>
    public InventoryReporter Inventory { get; }

    /// <summary><c>ReplicateObject</c> execution on this node.</summary>
    public RepairWorker Repair { get; }

    /// <summary>Background and requested scrubbing of the local store.</summary>
    public Scrubber Scrubber { get; }

    /// <summary>Usage reports to the control leader.</summary>
    public UsageReporter Usage { get; }

    /// <summary>Control-leader aggregation of usage reports (drain, removal checks, pool usage and health).</summary>
    public MaintenanceControl Control { get; }
}
