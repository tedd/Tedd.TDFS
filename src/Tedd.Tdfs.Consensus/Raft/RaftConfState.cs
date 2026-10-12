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

    /// <summary>
    /// Returns the membership after applying <paramref name="change"/>. Changes that do not apply (adding an existing
    /// member, promoting a node that is not a learner, removing a non-member) leave the membership unchanged, so every
    /// replica derives the same membership from the same log. New members are appended at the end of their list.
    /// </summary>
    public RaftConfState Apply(RaftConfChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        switch (change.Kind)
        {
            case RaftConfChangeKind.AddLearner:
                if (Contains(change.Node))
                    return this;
                return new RaftConfState(Voters.ToArray(), [.. Learners, change.Node]);
            case RaftConfChangeKind.PromoteLearner:
                if (!IsLearner(change.Node))
                    return this;
                return new RaftConfState([.. Voters, change.Node], Learners.Where(l => l != change.Node).ToArray());
            case RaftConfChangeKind.RemoveNode:
                if (!Contains(change.Node))
                    return this;
                return new RaftConfState(Voters.Where(v => v != change.Node).ToArray(), Learners.Where(l => l != change.Node).ToArray());
            default:
                return this;
        }
    }

    /// <summary>
    /// True when <paramref name="change"/> would modify this membership and keep at least one voter (the checks a
    /// leader applies before proposing).
    /// </summary>
    public bool CanApply(RaftConfChange change, out string? reason)
    {
        ArgumentNullException.ThrowIfNull(change);
        reason = null;
        if (change.Node.IsEmpty)
        {
            reason = "The node id is empty.";
            return false;
        }

        switch (change.Kind)
        {
            case RaftConfChangeKind.AddLearner:
                if (Contains(change.Node))
                    reason = $"Node {change.Node} is already a member.";
                break;
            case RaftConfChangeKind.PromoteLearner:
                if (!IsLearner(change.Node))
                    reason = $"Node {change.Node} is not a learner.";
                break;
            case RaftConfChangeKind.RemoveNode:
                if (!Contains(change.Node))
                    reason = $"Node {change.Node} is not a member.";
                else if (IsVoter(change.Node) && Voters.Count == 1)
                    reason = "The last voter cannot be removed.";
                break;
            default:
                reason = $"Unknown change kind {change.Kind}.";
                break;
        }

        return reason is null;
    }

    /// <summary>Validates the membership: no null lists, no empty ids, no duplicates, no node both voter and learner.</summary>
    /// <exception cref="ArgumentException">The membership is malformed.</exception>
    public void Validate()
    {
        if (Voters is null || Learners is null)
            throw new ArgumentException("Voters and Learners must not be null.");
        var seen = new HashSet<NodeId>();
        foreach (NodeId id in Voters.Concat(Learners))
        {
            if (id.IsEmpty)
                throw new ArgumentException("Membership must not contain the empty node id.");
            if (!seen.Add(id))
                throw new ArgumentException($"Node {id} appears more than once in the membership.");
        }
    }

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
