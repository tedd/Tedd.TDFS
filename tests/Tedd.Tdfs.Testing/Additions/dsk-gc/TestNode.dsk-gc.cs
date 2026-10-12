namespace Tedd.Tdfs.Testing;

/// <summary>Harness additions requested by the DSK/GC suites (see docs/api-additions/dsk-gc.md).</summary>
public sealed partial class TestNode
{
    /// <summary>
    /// Runs one complete object-store compaction pass on this node now, in addition to background maintenance, and
    /// completes when the pass has finished (new segments durable and published, old segments deleted).
    /// </summary>
    /// <remarks>
    /// Required behaviour: the pass is posted to the node loop (it runs on the node's own scheduler, so it interleaves
    /// with garbage collection, repair and client traffic exactly like a background pass, including in deterministic
    /// mode); every compaction fault site (<see cref="FaultSites.CompactionCopy"/>, <see cref="FaultSites.CompactionPublish"/>,
    /// <see cref="FaultSites.CompactionDelete"/>) is reported to the node's fault injector, so armed faults fire inside
    /// it; when an armed fault crashes the node during the pass, the returned task faults (with any exception) instead
    /// of hanging. Compacting a store without dead records is a no-op that still completes.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The node is not <see cref="TestNodeState.Running"/>.</exception>
    public Task CompactStorageAsync() => throw Stub.NotYet("TestNode.CompactStorageAsync");
}
