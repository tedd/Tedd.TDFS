using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Admin;

// Maintenance-plane administration: scrub requests, usage overlays and the removal safety check (control leader).
internal sealed partial class AdminService
{
    /// <summary>Optional provider of per-pool stored bytes (the control leader aggregates the volume leaders' summaries).</summary>
    public Func<string, long?>? PoolUsageProvider { get; set; }

    /// <summary>Optional provider of the non-members still holding a pool's objects (reported as draining).</summary>
    public Func<string, IReadOnlyList<string>>? PoolStrayHoldersProvider { get; set; }

    /// <summary>Optional check of a non-forced node removal (throws the refusal; may wait briefly for the replica state).</summary>
    public Func<MemberRecord, CancellationToken, Task>? RemovalCheck { get; set; }

    /// <summary>Optional provider of a node's reported device usage.</summary>
    public Func<NodeId, long?>? NodeUsageProvider { get; set; }

    /// <summary>
    /// <c>AdminOp.StartScrub</c> (ClusterAdmin, control leader): requests an immediate full scrub pass on the named node or
    /// on every live member. The control leader delivers the request with its next usage-report reply; it scrubs itself
    /// immediately. Returns once the request is recorded (scrubbing runs in the background).
    /// </summary>
    public ValueTask<AdminResultMessage> StartScrubAsync(RpcContext context, AdminRequestMessage message)
    {
        RequireClusterAdmin(context.Principal ?? string.Empty);
        AdminStartScrubBody body = message.ReadBody<AdminStartScrubBody>();
        RequireLeader();
        NodeId? target = body.Node is { } name ? RequireMember(name).Id : null;
        if (_s.Maintenance is not { } plane)
            throw new DfsException(DfsErrorCode.Unsupported, "Scrubbing is not available on this node.");
        plane.Control.RequestScrub(target);
        if (target is null || target.Value == _s.NodeId)
            plane.Scrubber.RequestFullPass();
        return new ValueTask<AdminResultMessage>(Result(message.Op, new AdminEmptyBody()));
    }

    /// <summary>A node entry with the device usage the node last reported (admin listings).</summary>
    private NodeEntry WithReportedUsage(NodeEntry entry)
        => NodeUsageProvider?.Invoke(entry.Id) is { } used ? entry with { UsedBytes = used } : entry;
}
