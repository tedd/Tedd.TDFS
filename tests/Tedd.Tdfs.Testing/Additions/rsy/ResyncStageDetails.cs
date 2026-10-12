namespace Tedd.Tdfs.Testing;

/// <summary>
/// <see cref="FaultContext.Detail"/> values that the server reports at <see cref="FaultSites.RaftSnapshotInstall"/> (stage
/// F16) on the <b>receiving</b> replica while it catches up from a peer checkpoint. Required behaviour of the server: at
/// each sub-stage below it calls <see cref="IFaultInjector.HitAsync"/> with
/// <c>Site = FaultSites.RaftSnapshotInstall</c>, <c>Node</c> = the receiving node's name, <c>Volume</c> = the volume name
/// of the group (null for the control group), <c>Operation = DfsOperationKind.RaftSnapshot</c> and <c>Detail</c> = the
/// constant. Hits are made before the sub-stage's effect becomes durable/visible, so a crash at the hit leaves only
/// previously completed durable progress. Added for the RSY suite (RSY-002, RSY-012, RSY-013; docs/api-additions/rsy.md).
/// </summary>
public static class ResyncStageDetails
{
    /// <summary>A checkpoint chunk was received (before it is persisted); hit once per chunk, <see cref="FaultContext.Occurrence"/> = 1-based chunk number.</summary>
    public const string Receive = "receive";

    /// <summary>All chunks of a checkpoint are durably received; before checksum/identity/lineage verification of the whole image.</summary>
    public const string Verify = "verify";

    /// <summary>The checkpoint verified; before it durably replaces the local state/log prefix.</summary>
    public const string Install = "install";

    /// <summary>The checkpoint is installed; before each post-checkpoint log entry (the suffix) is appended/applied during the same catch-up.</summary>
    public const string Suffix = "suffix";

    /// <summary>Installed (plus any suffix received so far); before the new state machine root is activated (served, counted as caught up).</summary>
    public const string Activate = "activate";

    /// <summary>All sub-stages in protocol order.</summary>
    public static IReadOnlyList<string> All { get; } = [Receive, Verify, Install, Suffix, Activate];
}
