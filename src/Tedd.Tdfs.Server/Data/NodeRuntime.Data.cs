using Tedd.Tdfs.Server.Data;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>Data-plane plug-in of the node runtime (INTERNALS.md §1.3): node-wide data jobs.</summary>
internal sealed partial class NodeRuntime
{
    partial void StartDataJobs(NodeServices services)
    {
        // Inventory reports drive garbage collection: the volume leaders classify, this node deletes (ARCHITECTURE §8).
        var reporter = new InventoryReporter(services);
        services.Jobs.StartPeriodic("inventory", services.Timings.InventoryInterval, reporter.RunAsync, services.Timings.InventoryInterval);
    }
}
