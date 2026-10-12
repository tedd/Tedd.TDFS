using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

// Additions requested by the CHA/MUT suites (see docs/api-additions/cha-mut.md).
public sealed partial class TestNode
{
    /// <summary>
    /// Forces one object-store compaction pass on this node (CHA-009): every object store of the node runs
    /// <c>ObjectStore.CompactAsync</c> on the node loop, copying live records out of segments that contain dead
    /// (deleted/tombstoned) records, flushing the new segment, publishing it and deleting the old one, passing the
    /// <see cref="FaultSites.CompactionCopy"/>, <see cref="FaultSites.CompactionPublish"/> and
    /// <see cref="FaultSites.CompactionDelete"/> fault points for every segment it compacts.
    /// </summary>
    /// <remarks>
    /// Every segment that contains at least one dead record is compacted, including the currently active segment (which
    /// is sealed first), so a pass after any object deletion always exercises the copy, publish and delete steps.
    /// The returned task completes when the pass finishes. If the node crashes or is stopped during the pass (for example
    /// because an armed fault crashed it) the task completes without throwing; <see cref="State"/> then reports the
    /// crash. A pass with nothing to compact completes immediately without hitting compaction fault points.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The node is not <see cref="TestNodeState.Running"/> when called.</exception>
    public Task CompactObjectStoreAsync(CancellationToken ct = default) =>
        throw Stub.NotYet("TestNode.CompactObjectStoreAsync");

    /// <summary>The software version this node currently runs (<see cref="TestSoftwareVersion.Current"/> unless restarted otherwise).</summary>
    public TestSoftwareVersion SoftwareVersion => throw Stub.NotYet("TestNode.SoftwareVersion");

    /// <summary>
    /// Restarts the node from its durable image (incarnation + 1, like <see cref="RestartAsync"/>) emulating the build
    /// described by <paramref name="version"/> (rolling upgrade/downgrade tests, CHA-011).
    /// </summary>
    /// <remarks>
    /// The restarted node advertises exactly <paramref name="version"/>'s protocol range and feature set in every
    /// handshake (node-to-node and client), reports them through the control group (cluster status / feature
    /// eligibility), and refuses to interpret messages, log entries, checkpoints or on-disk formats that require a feature
    /// outside its set (it answers <see cref="DfsErrorCode.Unsupported"/> / stops serving the affected group rather than
    /// misreading state). Restarting into a version that cannot open the durable image (for example after an
    /// irreversible feature activation) either throws <see cref="DfsException"/> from this method with the refusal, or
    /// leaves the node running without serving or voting for incompatible state; it must never silently misread it.
    /// </remarks>
    /// <param name="version">The build to emulate.</param>
    /// <param name="reconfigure">Optional node-options transform, as for <see cref="RestartAsync"/>.</param>
    public Task RestartWithSoftwareAsync(TestSoftwareVersion version, Func<NodeOptions, NodeOptions>? reconfigure = null) =>
        throw Stub.NotYet("TestNode.RestartWithSoftwareAsync");
}
