using System.Buffers;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Raft;

/// <summary>Encoding of <see cref="RaftConfState"/> for durable snapshot metadata and checkpoints.</summary>
internal static class RaftConfCodec
{
    private const byte Version = 1;

    /// <summary>Encodes voters and learners in order: <c>u8 version | list&lt;NodeId&gt; voters | list&lt;NodeId&gt; learners</c>.</summary>
    public static byte[] Encode(RaftConfState conf)
    {
        ArgumentNullException.ThrowIfNull(conf);
        var buffer = new ArrayBufferWriter<byte>(4 + (16 * (conf.Voters.Count + conf.Learners.Count)));
        var writer = new WireWriter(buffer);
        writer.WriteByte(Version);
        writer.WriteNodeIdList(conf.Voters);
        writer.WriteNodeIdList(conf.Learners);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Decodes <see cref="Encode"/> output; empty input is the empty membership.</summary>
    /// <exception cref="DfsIntegrityException">The bytes are malformed.</exception>
    public static RaftConfState Decode(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return RaftConfState.Empty;
        try
        {
            var reader = new WireReader(data, WireLimits.Default);
            if (reader.ReadByte() != Version)
                throw new DfsIntegrityException("Unsupported Raft membership encoding version.");
            IReadOnlyList<NodeId> voters = reader.ReadNodeIdList();
            IReadOnlyList<NodeId> learners = reader.ReadNodeIdList();
            reader.EnsureEnd();
            return new RaftConfState(voters, learners);
        }
        catch (DfsProtocolException ex)
        {
            throw new DfsIntegrityException("Malformed Raft membership encoding: " + ex.Message, ex);
        }
    }

    /// <summary>Writes a membership inline.</summary>
    public static void Write(ref WireWriter writer, RaftConfState conf)
    {
        writer.WriteNodeIdList(conf.Voters);
        writer.WriteNodeIdList(conf.Learners);
    }

    /// <summary>Reads a membership written by <see cref="Write"/>.</summary>
    public static RaftConfState Read(ref WireReader reader)
    {
        IReadOnlyList<NodeId> voters = reader.ReadNodeIdList();
        IReadOnlyList<NodeId> learners = reader.ReadNodeIdList();
        return new RaftConfState(voters, learners);
    }
}

/// <summary>
/// Adapts a durable <see cref="RaftLogStore"/> (Storage) to the <see cref="IRaftLogView"/> a <see cref="RaftNode"/> reads
/// (Consensus), and performs the durable writes of the <see cref="RaftReady"/> contract: snapshots and entries before the
/// hard state, all flushed before any dependent message is sent (CON-004).
/// </summary>
/// <remarks>
/// The view exposes the store as it is. Entries appended by <see cref="AppendAsync"/> become visible immediately (they are
/// the entries the node holds in its unstable tail with identical content) and are durable once <see cref="FlushAsync"/>
/// completed; the node only consults the view below its unstable offset, and <see cref="RaftNode.Advance"/> is called after
/// the flush.
/// </remarks>
internal sealed class RaftStorageAdapter : IRaftLogView
{
    private RaftLogSnapshotMeta? _cachedSource;
    private RaftSnapshotMeta _cachedMeta = RaftSnapshotMeta.Empty;

    /// <summary>Wraps <paramref name="store"/>.</summary>
    public RaftStorageAdapter(RaftLogStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        Store = store;
    }

    /// <summary>The durable store.</summary>
    public RaftLogStore Store { get; }

    /// <inheritdoc />
    public ulong FirstIndex => Store.FirstIndex;

    /// <inheritdoc />
    public ulong LastIndex => Store.LastIndex;

    /// <inheritdoc />
    public RaftSnapshotMeta Snapshot
    {
        get
        {
            RaftLogSnapshotMeta source = Store.Snapshot;
            if (!ReferenceEquals(source, _cachedSource))
            {
                _cachedMeta = source.Index == 0 && source.ConfState.IsEmpty
                    ? RaftSnapshotMeta.Empty
                    : new RaftSnapshotMeta(source.Index, source.Term, RaftConfCodec.Decode(source.ConfState.Span));
                _cachedSource = source;
            }

            return _cachedMeta;
        }
    }

    /// <summary>The persisted hard state in consensus form.</summary>
    public RaftHardState HardState
    {
        get
        {
            RaftLogHardState hs = Store.HardState;
            return new RaftHardState(hs.Term, hs.Vote, hs.Commit);
        }
    }

    /// <inheritdoc />
    public ulong TermAt(ulong index) => Store.TermAt(index);

    /// <inheritdoc />
    public IReadOnlyList<RaftEntry> Entries(ulong from, ulong toExclusive, int maxCount)
    {
        IReadOnlyList<LogEntryRecord> records = Store.Entries(from, toExclusive, maxCount);
        var result = new RaftEntry[records.Count];
        for (int i = 0; i < records.Count; i++)
            result[i] = ToEntry(records[i]);
        return result;
    }

    /// <summary>Appends entries (durable truncation of a conflicting suffix first; not durable until <see cref="FlushAsync"/>).</summary>
    public ValueTask AppendAsync(IReadOnlyList<RaftEntry> entries, CancellationToken ct = default)
    {
        if (entries.Count == 0)
            return ValueTask.CompletedTask;
        var records = new LogEntryRecord[entries.Count];
        for (int i = 0; i < entries.Count; i++)
        {
            RaftEntry e = entries[i];
            records[i] = new LogEntryRecord(e.Index, e.Term, (byte)e.Kind, e.Data);
        }

        return Store.AppendAsync(records, ct);
    }

    /// <summary>Durability barrier for appended entries.</summary>
    public ValueTask FlushAsync(CancellationToken ct = default) => Store.FlushAsync(ct);

    /// <summary>Durably replaces the hard state.</summary>
    public ValueTask SaveHardStateAsync(RaftHardState state, CancellationToken ct = default)
        => Store.SaveHardStateAsync(new RaftLogHardState(state.Term, state.Vote, state.Commit), ct);

    /// <summary>Durably records a snapshot through <paramref name="meta"/> and drops the entries it covers.</summary>
    public ValueTask CompactThroughAsync(RaftSnapshotMeta meta, CancellationToken ct = default)
        => Store.CompactThroughAsync(meta.Index, meta.Term, RaftConfCodec.Encode(meta.ConfState), ct);

    private static RaftEntry ToEntry(LogEntryRecord record)
    {
        var kind = (RaftEntryKind)record.Kind;
        if (kind is not (RaftEntryKind.Normal or RaftEntryKind.ConfChange or RaftEntryKind.Noop))
            throw new DfsIntegrityException($"Raft log entry {record.Index} has an unknown kind {record.Kind}.");
        return new RaftEntry(record.Index, record.Term, kind, record.Data);
    }
}
