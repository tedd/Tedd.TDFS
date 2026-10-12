namespace Tedd.Tdfs.Consensus;

/// <summary>Membership of a Raft group. Equality is structural (same voters and learners in the same order).</summary>
/// <param name="Voters">Voting members.</param>
/// <param name="Learners">Non-voting members.</param>
public sealed record RaftConfState(IReadOnlyList<NodeId> Voters, IReadOnlyList<NodeId> Learners)
{
    /// <summary>A configuration without members.</summary>
    public static RaftConfState Empty { get; } = new([], []);

    /// <summary>Number of votes forming a majority of the configured voters.</summary>
    public int Quorum => (Voters.Count / 2) + 1;

    /// <summary>True when <paramref name="id"/> is a voter.</summary>
    public bool IsVoter(NodeId id) => Voters.Contains(id);

    /// <summary>True when <paramref name="id"/> is a learner.</summary>
    public bool IsLearner(NodeId id) => Learners.Contains(id);

    /// <summary>True when <paramref name="id"/> is a voter or a learner.</summary>
    public bool Contains(NodeId id) => IsVoter(id) || IsLearner(id);

    /// <summary>Structural equality.</summary>
    public bool Equals(RaftConfState? other)
        => other is not null && Voters.SequenceEqual(other.Voters) && Learners.SequenceEqual(other.Learners);

    /// <summary>Structural hash.</summary>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (NodeId v in Voters)
            hash.Add(v);
        hash.Add(-1);
        foreach (NodeId l in Learners)
            hash.Add(l);
        return hash.ToHashCode();
    }

    /// <summary>Readable form listing voters and learners.</summary>
    public override string ToString()
        => $"Voters=[{string.Join(", ", Voters)}] Learners=[{string.Join(", ", Learners)}]";
}
