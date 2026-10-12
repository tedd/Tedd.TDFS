namespace Tedd.Tdfs.Consensus;

/// <summary>
/// Base of all Raft messages. Messages are group-agnostic; the host wraps them with the group id and the sender's
/// incarnation when sending them over the network.
/// </summary>
/// <param name="From">Sender.</param>
/// <param name="To">Recipient.</param>
/// <param name="Term">Sender's term (for PreVote requests: the term the sender would campaign in).</param>
public abstract record RaftMessage(NodeId From, NodeId To, ulong Term);
