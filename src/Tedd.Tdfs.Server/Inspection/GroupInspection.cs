using Tedd.Tdfs.Consensus;

namespace Tedd.Tdfs.Server;

/// <summary>State of one Raft group as seen by one node.</summary>
/// <param name="Group">"control" or the volume name.</param>
/// <param name="Role">This node's role.</param>
/// <param name="Term">Current term.</param>
/// <param name="Leader">Known leader's node name.</param>
/// <param name="CommitIndex">Commit index.</param>
/// <param name="AppliedIndex">Applied index.</param>
/// <param name="LastIndex">Last log index.</param>
/// <param name="SnapshotIndex">Index of the latest Raft snapshot (checkpoint).</param>
/// <param name="Voters">Voter node names.</param>
/// <param name="Learners">Learner node names.</param>
public sealed record GroupInspection(string Group, RaftRole Role, ulong Term, string? Leader, ulong CommitIndex, ulong AppliedIndex,
    ulong LastIndex, ulong SnapshotIndex, IReadOnlyList<string> Voters, IReadOnlyList<string> Learners);
