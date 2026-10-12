using Tedd.Tdfs.Consensus;

namespace Tedd.Tdfs.Testing;

/// <summary>Configuration of a <see cref="RaftSimulation"/>.</summary>
public sealed record RaftSimulationOptions
{
    /// <summary>Number of initial voters (indices 0..Voters-1).</summary>
    public int Voters { get; init; } = 3;

    /// <summary>Number of initial learners (indices Voters..Voters+Learners-1).</summary>
    public int Learners { get; init; }

    /// <summary>Seed for node ids, election timeouts and network faults.</summary>
    public ulong Seed { get; init; }

    /// <summary>Safety mutations applied to every node (negative controls only).</summary>
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;

    /// <summary>Lower bound of the randomized election timeout (ticks).</summary>
    public int ElectionTicksMin { get; init; } = 10;

    /// <summary>Exclusive upper bound of the randomized election timeout (ticks).</summary>
    public int ElectionTicksMax { get; init; } = 20;

    /// <summary>Leader heartbeat interval (ticks).</summary>
    public int HeartbeatTicks { get; init; } = 2;

    /// <summary>Enables PreVote on every node.</summary>
    public bool PreVote { get; init; } = true;

    /// <summary>Enables CheckQuorum on every node.</summary>
    public bool CheckQuorum { get; init; } = true;

    /// <summary>Maximum entries per append message.</summary>
    public int MaxEntriesPerAppend { get; init; } = 32;

    /// <summary>Maximum in-flight appends per follower.</summary>
    public int MaxInflightAppends { get; init; } = 8;

    /// <summary>
    /// Compact a node's durable log once this many applied entries follow its snapshot (0 disables automatic compaction).
    /// </summary>
    public int SnapshotThreshold { get; init; }

    /// <summary>Snapshot chunk size (bytes) used by leaders.</summary>
    public int SnapshotChunkBytes { get; init; } = 256;

    /// <summary>Maximum extra delivery delay (ticks) applied to messages while reordering is enabled.</summary>
    public int MaxReorderDelayTicks { get; init; } = 3;

    /// <summary>Record every dropped, duplicated or delivered message in <see cref="RaftSimulation.Trace"/>.</summary>
    public bool TraceMessages { get; init; }

    /// <summary>Validates the options.</summary>
    /// <exception cref="ArgumentException">A value is out of range.</exception>
    public void Validate()
    {
        if (Voters < 1)
            throw new ArgumentOutOfRangeException(nameof(Voters), Voters, "At least one voter is required.");
        if (Learners < 0)
            throw new ArgumentOutOfRangeException(nameof(Learners), Learners, "Learners must not be negative.");
        if (Voters + Learners > 64)
            throw new ArgumentOutOfRangeException(nameof(Voters), Voters + Learners, "At most 64 nodes are supported.");
        if (SnapshotThreshold < 0)
            throw new ArgumentOutOfRangeException(nameof(SnapshotThreshold), SnapshotThreshold, "Must not be negative.");
        if (MaxReorderDelayTicks < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxReorderDelayTicks), MaxReorderDelayTicks, "Must not be negative.");
        if (Mutations is null)
            throw new ArgumentException("Mutations must not be null.", nameof(Mutations));
        BuildConfig(NodeId.New(new SeededRandom(1))).Validate();
        new RaftNodeOptions { SnapshotChunkBytes = SnapshotChunkBytes }.Validate();
    }

    /// <summary>The Raft configuration used for node <paramref name="self"/>.</summary>
    public RaftConfig BuildConfig(NodeId self)
        => new(self, ElectionTicksMin, ElectionTicksMax, HeartbeatTicks, PreVote, CheckQuorum, MaxEntriesPerAppend, MaxInflightAppends);
}

/// <summary>Where an armed crash interrupts a node's processing of its next <see cref="RaftReady"/>.</summary>
public enum RaftCrashPoint
{
    /// <summary>Before anything of the ready is persisted (its hard state, entries and snapshot are lost).</summary>
    BeforePersist,

    /// <summary>After the ready is persisted, before its messages are sent.</summary>
    AfterPersistBeforeSend,

    /// <summary>After the messages are sent, before committed entries are applied.</summary>
    AfterSendBeforeApply,
}

/// <summary>A confirmed ReadIndex observed by a simulated host.</summary>
/// <param name="Index">Read index (commit index confirmed by the leader).</param>
/// <param name="Context">Request context passed to <see cref="RaftSimulation.ReadIndex"/>.</param>
/// <param name="Term">Leader's term when the read was confirmed.</param>
/// <param name="Tick">Simulation tick at which the read state was received.</param>
public sealed record SimReadState(ulong Index, ulong Context, ulong Term, long Tick);
