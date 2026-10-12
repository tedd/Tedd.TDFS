namespace Tedd.Tdfs.Consensus;

/// <summary>Static configuration of one <see cref="RaftNode"/>.</summary>
/// <param name="Self">This node's id.</param>
/// <param name="ElectionTicksMin">Minimum randomized election timeout, in ticks.</param>
/// <param name="ElectionTicksMax">Maximum randomized election timeout, in ticks (inclusive upper bound of the range).</param>
/// <param name="HeartbeatTicks">Leader heartbeat interval, in ticks.</param>
/// <param name="PreVote">Run a PreVote round before incrementing the term.</param>
/// <param name="CheckQuorum">A leader steps down when it has not heard from a quorum within an election timeout.</param>
/// <param name="MaxEntriesPerAppend">Maximum entries carried by one append message.</param>
/// <param name="MaxInflightAppends">Maximum unacknowledged append messages per follower.</param>
public sealed record RaftConfig(NodeId Self, int ElectionTicksMin = 30, int ElectionTicksMax = 60, int HeartbeatTicks = 5,
    bool PreVote = true, bool CheckQuorum = true, int MaxEntriesPerAppend = 256, int MaxInflightAppends = 64)
{
    /// <summary>Validates the configuration.</summary>
    /// <exception cref="ArgumentException">A value is out of range or inconsistent.</exception>
    public void Validate()
    {
        if (ElectionTicksMin < 1)
            throw new ArgumentException($"ElectionTicksMin must be at least 1 (was {ElectionTicksMin}).");
        if (ElectionTicksMax < ElectionTicksMin)
            throw new ArgumentException($"ElectionTicksMax ({ElectionTicksMax}) must be at least ElectionTicksMin ({ElectionTicksMin}).");
        if (HeartbeatTicks < 1)
            throw new ArgumentException($"HeartbeatTicks must be at least 1 (was {HeartbeatTicks}).");
        if (HeartbeatTicks >= ElectionTicksMin)
            throw new ArgumentException($"HeartbeatTicks ({HeartbeatTicks}) must be lower than ElectionTicksMin ({ElectionTicksMin}).");
        if (MaxEntriesPerAppend < 1)
            throw new ArgumentException($"MaxEntriesPerAppend must be at least 1 (was {MaxEntriesPerAppend}).");
        if (MaxInflightAppends < 1)
            throw new ArgumentException($"MaxInflightAppends must be at least 1 (was {MaxInflightAppends}).");
    }
}
