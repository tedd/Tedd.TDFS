namespace Tedd.Tdfs.Consensus;

/// <summary>A membership change proposal (one at a time; effective when appended).</summary>
/// <param name="Kind">Change kind.</param>
/// <param name="Node">Affected node.</param>
public sealed record RaftConfChange(RaftConfChangeKind Kind, NodeId Node);
