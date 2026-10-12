namespace Tedd.Tdfs.Server;

/// <summary>Timing, retention and sizing parameters of a node (ARCHITECTURE.md §16).</summary>
/// <param name="Tick">Raft tick interval.</param>
/// <param name="HeartbeatTicks">Leader heartbeat interval in ticks.</param>
/// <param name="ElectionTicksMin">Minimum randomized election timeout in ticks.</param>
/// <param name="ElectionTicksMax">Maximum randomized election timeout in ticks.</param>
/// <param name="LeaseDuration">Handle lease duration (renewed every third of it).</param>
/// <param name="SessionTimeout">Client session expiry without keep-alive.</param>
/// <param name="MaxOutcomesPerSession">Retained operation outcomes per session.</param>
/// <param name="OutcomeRetention">How long outcomes of an expired session remain queryable.</param>
/// <param name="GcInterval">Garbage-collection pass interval.</param>
/// <param name="InventoryInterval">Storage inventory report / repair scheduling interval.</param>
/// <param name="ScrubInterval">Background scrub pass interval.</param>
/// <param name="RaftSnapshotThreshold">Applied entries after which a Raft checkpoint is taken and the log compacted.</param>
/// <param name="ChunkSize">Logical update unit (client chunking of writes).</param>
/// <param name="MaxObjectSize">Largest stored object.</param>
/// <param name="PinDuration">Lifetime of read pins (handles, snapshot readers) without renewal.</param>
/// <param name="EnumerationPinDuration">Lifetime of directory-enumeration continuation pins.</param>
/// <param name="RepairBytesPerSecond">Repair/re-replication rate limit.</param>
public sealed record NodeTimings(TimeSpan Tick, int HeartbeatTicks, int ElectionTicksMin, int ElectionTicksMax,
    TimeSpan LeaseDuration, TimeSpan SessionTimeout, int MaxOutcomesPerSession, TimeSpan OutcomeRetention,
    TimeSpan GcInterval, TimeSpan InventoryInterval, TimeSpan ScrubInterval, int RaftSnapshotThreshold,
    int ChunkSize, int MaxObjectSize, TimeSpan PinDuration, TimeSpan EnumerationPinDuration, long RepairBytesPerSecond)
{
    /// <summary>Production defaults.</summary>
    public static NodeTimings Production { get; } = new(
        Tick: TimeSpan.FromMilliseconds(10),
        HeartbeatTicks: 5,
        ElectionTicksMin: 30,
        ElectionTicksMax: 60,
        LeaseDuration: TimeSpan.FromSeconds(30),
        SessionTimeout: TimeSpan.FromMinutes(5),
        MaxOutcomesPerSession: 4096,
        OutcomeRetention: TimeSpan.FromHours(1),
        GcInterval: TimeSpan.FromSeconds(60),
        InventoryInterval: TimeSpan.FromSeconds(30),
        ScrubInterval: TimeSpan.FromHours(1),
        RaftSnapshotThreshold: 10_000,
        ChunkSize: 1 << 20,
        MaxObjectSize: 4 << 20,
        PinDuration: TimeSpan.FromSeconds(30),
        EnumerationPinDuration: TimeSpan.FromSeconds(60),
        RepairBytesPerSecond: 256L << 20);

    /// <summary>Fast timings for tests.</summary>
    public static NodeTimings Fast { get; } = new(
        Tick: TimeSpan.FromMilliseconds(5),
        HeartbeatTicks: 5,
        ElectionTicksMin: 30,
        ElectionTicksMax: 60,
        LeaseDuration: TimeSpan.FromSeconds(2),
        SessionTimeout: TimeSpan.FromSeconds(10),
        MaxOutcomesPerSession: 256,
        OutcomeRetention: TimeSpan.FromSeconds(30),
        GcInterval: TimeSpan.FromMilliseconds(250),
        InventoryInterval: TimeSpan.FromMilliseconds(250),
        ScrubInterval: TimeSpan.FromSeconds(1),
        RaftSnapshotThreshold: 200,
        ChunkSize: 64 << 10,
        MaxObjectSize: 1 << 20,
        PinDuration: TimeSpan.FromSeconds(2),
        EnumerationPinDuration: TimeSpan.FromSeconds(10),
        RepairBytesPerSecond: 64L << 20);

    /// <summary>Validates ranges and relations between the values.</summary>
    /// <exception cref="ArgumentException">A value is out of range or inconsistent.</exception>
    public void Validate()
    {
        static void Positive(TimeSpan value, string name)
        {
            if (value <= TimeSpan.Zero)
                throw new ArgumentException($"{name} must be positive (was {value}).");
        }

        static void AtLeastOne(long value, string name)
        {
            if (value < 1)
                throw new ArgumentException($"{name} must be at least 1 (was {value}).");
        }

        Positive(Tick, nameof(Tick));
        AtLeastOne(HeartbeatTicks, nameof(HeartbeatTicks));
        AtLeastOne(ElectionTicksMin, nameof(ElectionTicksMin));
        if (ElectionTicksMax < ElectionTicksMin)
            throw new ArgumentException($"ElectionTicksMax ({ElectionTicksMax}) must be at least ElectionTicksMin ({ElectionTicksMin}).");
        if (HeartbeatTicks >= ElectionTicksMin)
            throw new ArgumentException($"HeartbeatTicks ({HeartbeatTicks}) must be lower than ElectionTicksMin ({ElectionTicksMin}).");
        Positive(LeaseDuration, nameof(LeaseDuration));
        Positive(SessionTimeout, nameof(SessionTimeout));
        AtLeastOne(MaxOutcomesPerSession, nameof(MaxOutcomesPerSession));
        Positive(OutcomeRetention, nameof(OutcomeRetention));
        Positive(GcInterval, nameof(GcInterval));
        Positive(InventoryInterval, nameof(InventoryInterval));
        Positive(ScrubInterval, nameof(ScrubInterval));
        AtLeastOne(RaftSnapshotThreshold, nameof(RaftSnapshotThreshold));
        AtLeastOne(ChunkSize, nameof(ChunkSize));
        AtLeastOne(MaxObjectSize, nameof(MaxObjectSize));
        if (ChunkSize > MaxObjectSize)
            throw new ArgumentException($"ChunkSize ({ChunkSize}) must not exceed MaxObjectSize ({MaxObjectSize}).");
        Positive(PinDuration, nameof(PinDuration));
        Positive(EnumerationPinDuration, nameof(EnumerationPinDuration));
        AtLeastOne(RepairBytesPerSecond, nameof(RepairBytesPerSecond));
    }
}
