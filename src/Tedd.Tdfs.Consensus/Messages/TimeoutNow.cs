namespace Tedd.Tdfs.Consensus;

/// <summary>Leadership transfer: tells a caught-up follower to campaign immediately.</summary>
/// <param name="From">Current leader.</param>
/// <param name="To">Transfer target.</param>
/// <param name="Term">Leader's term.</param>
public sealed record TimeoutNow(NodeId From, NodeId To, ulong Term) : RaftMessage(From, To, Term);
