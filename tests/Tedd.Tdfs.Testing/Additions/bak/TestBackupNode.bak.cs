using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

// Harness additions requested by the BAK suite (docs/api-additions/bak.md).
public sealed partial class TestBackupNode
{
    /// <summary>
    /// Makes every inventory/Bloom-filter hint this backup node reports for <paramref name="volume"/> (replies to
    /// <c>BackupInventory</c>) additionally claim that it holds each id in <paramref name="objectIds"/>, whether or not
    /// it does (a simulated false positive). Nothing is stored and the node's own closure verification, receive state
    /// and published state are not affected: the claim only falsifies the hint sent to the exporter. Passing an empty
    /// collection clears the claims. Survives until cleared or until the node restarts. Used by BAK-007.
    /// </summary>
    public Task SetFalseInventoryClaimsAsync(string volume, IReadOnlyCollection<ObjectId> objectIds) =>
        throw Stub.NotYet("TestBackupNode.SetFalseInventoryClaimsAsync");

    /// <summary>
    /// Stops the node gracefully when it is running (no-op when crashed/stopped) and restarts it from its durable
    /// image with options transformed by <paramref name="reconfigure"/> (for example a smaller
    /// <see cref="BackupNodeOptions.KeepLastSnapshots"/>). The reconfiguration persists for later
    /// <see cref="RestartAsync()"/> calls. Completes once the node serves clients again. Used by BAK-018, BAK-027.
    /// </summary>
    public Task RestartAsync(Func<BackupNodeOptions, BackupNodeOptions> reconfigure) =>
        throw Stub.NotYet("TestBackupNode.RestartAsync(reconfigure)");
}
