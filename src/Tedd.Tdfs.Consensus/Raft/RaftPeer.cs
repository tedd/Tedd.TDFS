namespace Tedd.Tdfs.Consensus;

/// <summary>A member of a Raft group.</summary>
/// <param name="Id">The member's node id.</param>
public readonly record struct RaftPeer(NodeId Id);
