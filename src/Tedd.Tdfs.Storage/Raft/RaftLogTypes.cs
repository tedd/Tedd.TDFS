using System.Buffers.Binary;

namespace Tedd.Tdfs.Storage;

/// <summary>A durable Raft log entry (storage-level shape; the consensus layer maps its own entry type onto it).</summary>
/// <param name="Index">Log index (≥ 1).</param>
/// <param name="Term">Term in which the entry was proposed.</param>
/// <param name="Kind">Entry kind as defined by the consensus layer (opaque to storage).</param>
/// <param name="Data">Entry payload.</param>
public sealed record LogEntryRecord(ulong Index, ulong Term, byte Kind, ReadOnlyMemory<byte> Data);

/// <summary>Durable Raft hard state.</summary>
/// <param name="Term">Current term.</param>
/// <param name="Vote">Node voted for in <paramref name="Term"/>, if any.</param>
/// <param name="Commit">Highest index known committed (never beyond the durable log or snapshot).</param>
public readonly record struct RaftLogHardState(ulong Term, NodeId? Vote, ulong Commit)
{
    /// <summary>The initial hard state (term 0, no vote, commit 0).</summary>
    public static RaftLogHardState Initial => default;

    internal const int EncodedSize = 1 + 8 + 1 + 16 + 8;

    internal byte[] Encode()
    {
        var buffer = new byte[EncodedSize];
        Span<byte> s = buffer;
        s[0] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(s[1..], Term);
        s[9] = Vote.HasValue ? (byte)1 : (byte)0;
        (Vote?.Value ?? Guid.Empty).TryWriteBytes(s[10..]);
        BinaryPrimitives.WriteUInt64LittleEndian(s[26..], Commit);
        return buffer;
    }

    internal static RaftLogHardState Decode(ReadOnlySpan<byte> data, string path)
    {
        if (data.Length != EncodedSize || data[0] != 1 || data[9] > 1)
            throw new DfsIntegrityException($"Raft hard state '{path}' is malformed.");
        NodeId? vote = data[9] == 1 ? new NodeId(new Guid(data.Slice(10, 16))) : null;
        return new RaftLogHardState(BinaryPrimitives.ReadUInt64LittleEndian(data[1..]), vote, BinaryPrimitives.ReadUInt64LittleEndian(data[26..]));
    }
}

/// <summary>Metadata of the state-machine snapshot that replaces the log prefix up to <paramref name="Index"/>.</summary>
/// <param name="Index">Last log index covered by the snapshot (0 = none).</param>
/// <param name="Term">Term of the entry at <paramref name="Index"/>.</param>
/// <param name="ConfState">Membership as of <paramref name="Index"/>, encoded by the consensus layer (opaque to storage).</param>
public sealed record RaftLogSnapshotMeta(ulong Index, ulong Term, ReadOnlyMemory<byte> ConfState)
{
    /// <summary>No snapshot (index 0, term 0, empty membership).</summary>
    public static RaftLogSnapshotMeta Empty { get; } = new(0, 0, ReadOnlyMemory<byte>.Empty);

    /// <summary>Value equality including the membership bytes.</summary>
    public bool Equals(RaftLogSnapshotMeta? other)
        => other is not null && Index == other.Index && Term == other.Term && ConfState.Span.SequenceEqual(other.ConfState.Span);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Index, Term, ConfState.Length);

    internal byte[] Encode(bool resetPending)
    {
        var buffer = new byte[1 + 1 + 8 + 8 + 4 + ConfState.Length];
        Span<byte> s = buffer;
        s[0] = 1;
        s[1] = resetPending ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt64LittleEndian(s[2..], Index);
        BinaryPrimitives.WriteUInt64LittleEndian(s[10..], Term);
        BinaryPrimitives.WriteInt32LittleEndian(s[18..], ConfState.Length);
        ConfState.Span.CopyTo(s[22..]);
        return buffer;
    }

    internal static RaftLogSnapshotMeta Decode(ReadOnlySpan<byte> data, string path, out bool resetPending)
    {
        if (data.Length < 22 || data[0] != 1 || data[1] > 1)
            throw new DfsIntegrityException($"Raft snapshot metadata '{path}' is malformed.");
        int confLength = BinaryPrimitives.ReadInt32LittleEndian(data[18..]);
        if (confLength < 0 || confLength != data.Length - 22)
            throw new DfsIntegrityException($"Raft snapshot metadata '{path}' is malformed.");
        resetPending = data[1] == 1;
        return new RaftLogSnapshotMeta(BinaryPrimitives.ReadUInt64LittleEndian(data[2..]), BinaryPrimitives.ReadUInt64LittleEndian(data[10..]),
            data.Slice(22, confLength).ToArray());
    }
}

/// <summary>Configuration of a <see cref="RaftLogStore"/>.</summary>
public sealed record RaftLogStoreOptions
{
    /// <summary>Target size of one journal segment (default 16 MiB).</summary>
    public long SegmentSize { get; init; } = 16 << 20;

    /// <summary>Record cipher for log entries at rest; null stores plaintext.</summary>
    public IRecordCipher? Cipher { get; init; }

    /// <summary>Largest accepted entry payload (default 64 MiB).</summary>
    public int MaxEntrySize { get; init; } = JournalOptions.DefaultMaxRecordSize - EntryHeaderSize;

    /// <summary>Fault injector (fault site <see cref="FaultSites.RecoveryJournalReplay"/> is hit before replay).</summary>
    public IFaultInjector Faults { get; init; } = NoFaults.Instance;

    /// <summary>Node name reported in fault contexts.</summary>
    public string NodeName { get; init; } = "local";

    /// <summary>Raft group name reported in fault contexts (for example "control" or a volume name).</summary>
    public string? GroupName { get; init; }

    internal const int EntryHeaderSize = 9;
}
