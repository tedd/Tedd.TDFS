namespace Tedd.Tdfs.Testing;

/// <summary>Harness additions requested by the DSK/GC suites (see docs/api-additions/dsk-gc.md).</summary>
public sealed partial class TestNode
{
    /// <summary>
    /// Runs one complete object-store compaction pass on this node now, in addition to background maintenance, and
    /// completes when the pass has finished (new segments durable and published, old segments deleted).
    /// </summary>
    /// <remarks>
    /// <para>Consolidated definition requested by the DSK/GC suites (DSK/GC compaction-interleaving tests) and the CHA suite
    /// (CHA-009, which previously asked for an identical <c>CompactObjectStoreAsync</c>); see docs/api-additions/dsk-gc.md
    /// and docs/api-additions/cha-mut.md.</para>
    /// <para>Required behaviour: every object store of the node runs <c>ObjectStore.CompactAsync</c>; the pass is posted
    /// to the node loop (it runs on the node's own scheduler, so it interleaves with garbage collection, repair and client
    /// traffic exactly like a background pass, including in deterministic mode). Every segment that contains at least one
    /// dead (deleted/tombstoned) record — including the currently active segment, which is sealed first — is compacted, so a
    /// pass after any object deletion always exercises the copy, publish and delete steps: live records are copied to a new
    /// segment, the new segment is flushed and published, and the old segment is deleted. Every compaction fault site
    /// (<see cref="FaultSites.CompactionCopy"/>, <see cref="FaultSites.CompactionPublish"/>,
    /// <see cref="FaultSites.CompactionDelete"/>) is reported to the node's fault injector for every segment compacted, so
    /// armed faults fire inside it. Compacting a store without dead records is a no-op that completes immediately without
    /// hitting compaction fault points.</para>
    /// <para>Interruption: when the node crashes or stops during the pass (an armed fault such as
    /// <see cref="FaultAction.CrashNode"/>, or <see cref="CrashAsync"/>/<see cref="StopAsync"/> while the pass is held by a
    /// Block fault), the returned task faults (with any exception) instead of hanging or completing, so an interrupted pass
    /// never reports completion; <see cref="State"/> then reports the crash/stop. Callers that compact several nodes while a
    /// crash fault is armed must therefore tolerate the exception for the node that went down.</para>
    /// <para><paramref name="ct"/> only cancels waiting for the pass; a pass already posted continues on the node.</para>
    /// </remarks>
    /// <param name="ct">Cancels waiting for the pass.</param>
    /// <exception cref="InvalidOperationException">The node is not <see cref="TestNodeState.Running"/> when called.</exception>
    public async Task CompactStorageAsync(CancellationToken ct = default)
    {
        Server.NodeInspector inspector = Inspect;
        Task pass = inspector.CompactObjectStoreAsync();
        Cluster.Record($"node {Name}: compaction pass requested");
        Task finished = ct.CanBeCanceled ? pass.WaitAsync(ct) : pass;
        await finished;
        if (State is TestNodeState.Crashed or TestNodeState.Stopped)
            throw new InvalidOperationException($"Node {Name} went down ({State}) while its compaction pass ran; the pass is not complete.");
        Cluster.Record($"node {Name}: compaction pass finished");
    }
}
