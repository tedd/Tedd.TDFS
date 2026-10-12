namespace Tedd.Tdfs;

/// <summary>Status of a consensus group (control or volume).</summary>
/// <param name="Group">Group name (<c>control</c> or the volume name).</param>
/// <param name="Voters">Voter node names.</param>
/// <param name="Learners">Learner node names.</param>
/// <param name="Leader">Current leader node name, if any.</param>
/// <param name="Term">Current term.</param>
/// <param name="CommitIndex">Commit index.</param>
/// <param name="AppliedIndex">Applied index.</param>
/// <param name="HasQuorum">True when a majority of configured voters is reachable.</param>
public sealed record DfsGroupStatus(
    string Group,
    IReadOnlyList<string> Voters,
    IReadOnlyList<string> Learners,
    string? Leader,
    ulong Term,
    ulong CommitIndex,
    ulong AppliedIndex,
    bool HasQuorum);
