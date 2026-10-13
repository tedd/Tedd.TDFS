namespace Tedd.Tdfs.Consensus;

/// <summary>Optional host-provided collaborators and limits of a <see cref="RaftNode"/>.</summary>
public sealed record RaftNodeOptions
{
    /// <summary>Default options (no snapshot source, no mutations).</summary>
    public static RaftNodeOptions Default { get; } = new();

    /// <summary>
    /// Supplies snapshot payloads for lagging followers. Without a source a leader cannot repair a follower whose next
    /// entry has been compacted (it keeps probing at every heartbeat).
    /// </summary>
    public IRaftSnapshotSource? SnapshotSource { get; init; }

    /// <summary>
    /// Index already applied to the host's state machine at construction. Null means the log's snapshot index (the host
    /// restored its state machine from that snapshot). Must lie within [snapshot index, commit].
    /// </summary>
    public ulong? Applied { get; init; }

    /// <summary>
    /// True for a member that lost its durable state (an empty-disk replacement keeping its identity, OFF-020): it neither
    /// votes nor campaigns until its log has caught up with the commit index a leader announced to it after the restart.
    /// Granting votes with an empty log could elect a candidate missing entries this member had acknowledged before the
    /// loss (its forgotten vote and log were part of earlier majorities). See <see cref="RaftNode.IsRebuilding"/>.
    /// </summary>
    public bool Rebuilding { get; init; }

    /// <summary>Maximum payload bytes per <see cref="InstallSnapshot"/> chunk.</summary>
    public int SnapshotChunkBytes { get; init; } = 1 << 20;

    /// <summary>Largest snapshot payload a follower accepts (bytes); larger transfers are rejected.</summary>
    public long MaxSnapshotBytes { get; init; } = Array.MaxLength;

    /// <summary>
    /// Negative-control switches (MUT-*). Only <see cref="SafetyMutations.QuorumFromReachableNodes"/> affects the Raft
    /// core: majorities (commit, election, ReadIndex, CheckQuorum) are computed from the voters heard from within the last
    /// two maximum election timeouts instead of all configured voters. Never enable in production.
    /// </summary>
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;

    /// <summary>Validates the options.</summary>
    /// <exception cref="ArgumentException">A value is out of range.</exception>
    public void Validate()
    {
        if (SnapshotChunkBytes < 1)
            throw new ArgumentException($"SnapshotChunkBytes must be at least 1 (was {SnapshotChunkBytes}).");
        if (MaxSnapshotBytes < 0 || MaxSnapshotBytes > Array.MaxLength)
            throw new ArgumentException($"MaxSnapshotBytes must be within [0, {Array.MaxLength}] (was {MaxSnapshotBytes}).");
        if (Mutations is null)
            throw new ArgumentException("Mutations must not be null.");
    }
}
