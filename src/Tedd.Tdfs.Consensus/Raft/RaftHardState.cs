namespace Tedd.Tdfs.Consensus;

/// <summary>State that must be persisted before dependent messages are sent.</summary>
/// <param name="Term">Current term.</param>
/// <param name="Vote">Candidate voted for in <paramref name="Term"/>, if any.</param>
/// <param name="Commit">Highest index known to be committed.</param>
public readonly record struct RaftHardState(ulong Term, NodeId? Vote, ulong Commit);
