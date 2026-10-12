using System.Collections.Frozen;
using System.Collections.Immutable;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Value codecs shared by all protocol records: extension blocks, identifiers, optional values, collections and the
/// public Core value types (policies, labels, limits, capabilities, retention, backup checkpoints).
/// </summary>
/// <remarks>
/// Canonical form: sets and maps are written in ascending ordinal key order and readers reject unsorted or duplicate
/// entries, so one logical value has exactly one encoding (FMT-012). Every count is bounded by
/// <see cref="WireLimits.MaxCollectionCount"/> and by the bytes remaining in the input, so hostile counts cannot force
/// large allocations (SEC-014/015).
/// </remarks>
public static class WireCodecExtensions
{
    // ---------------------------------------------------------------- extension blocks

    /// <summary>Writes a terminal extension block (empty when <paramref name="extensions"/> is null or empty).</summary>
    public static void WriteExtensionBlock(this ref WireWriter writer, IReadOnlyList<WireExtension>? extensions)
    {
        if (extensions is null || extensions.Count == 0)
        {
            writer.WriteNoExtensions();
            return;
        }

        writer.WriteExtensions(extensions as WireExtension[] ?? extensions.ToArray());
    }

    /// <summary>
    /// Reads a terminal extension block. This version understands no extension fields: unknown optional extensions
    /// are returned (so they can be preserved when the record is re-encoded) and an unknown required extension throws
    /// <see cref="DfsProtocolException"/> with <see cref="DfsErrorCode.Unsupported"/> (FMT-002/003).
    /// </summary>
    public static IReadOnlyList<WireExtension> ReadExtensionBlock(this ref WireReader reader)
    {
        if (reader.TryReadEmptyExtensions())
            return [];

        List<WireExtension>? list = null;
        reader.ReadExtensions(FrozenSet<uint>.Empty, e => (list ??= []).Add(e));
        return list is null ? [] : list.ToArray();
    }

    // ---------------------------------------------------------------- integers

    /// <summary>Writes a signed 32-bit value as a zigzag varint.</summary>
    public static void WriteVarInt32(this ref WireWriter writer, int value) => writer.WriteVarInt(value);

    /// <summary>Reads a zigzag varint that must fit in a signed 32-bit integer.</summary>
    public static int ReadVarInt32(this ref WireReader reader)
    {
        long value = reader.ReadVarInt();
        if (value is < int.MinValue or > int.MaxValue)
            throw WireReader.Malformed($"Value {value} exceeds the 32-bit range.");
        return (int)value;
    }

    /// <summary>Reads a varuint that must fit in an unsigned 16-bit integer.</summary>
    public static ushort ReadVarUInt16(this ref WireReader reader)
    {
        ulong value = reader.ReadVarUInt();
        if (value > ushort.MaxValue)
            throw WireReader.Malformed($"Value {value} exceeds the 16-bit range.");
        return (ushort)value;
    }

    // ---------------------------------------------------------------- enums

    /// <summary>
    /// Writes an enum that receivers decode strictly (<see cref="WireReader.ReadEnum{TEnum}"/>), failing fast on the
    /// sender for an undefined value that every receiver would reject.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not a defined member.</exception>
    public static void WriteDefinedEnum<TEnum>(this ref WireWriter writer, TEnum value) where TEnum : struct, Enum
    {
        if (!Enum.IsDefined(value))
            throw new ArgumentOutOfRangeException(nameof(value), value, $"Undefined {typeof(TEnum).Name} value.");
        writer.WriteEnum(value);
    }

    // ---------------------------------------------------------------- identifiers

    /// <summary>Writes an <see cref="InodeId"/> as a varuint.</summary>
    public static void WriteInodeId(this ref WireWriter writer, InodeId value) => writer.WriteVarUInt(value.Value);

    /// <summary>Reads an <see cref="InodeId"/>.</summary>
    public static InodeId ReadInodeId(this ref WireReader reader) => new(reader.ReadVarUInt());

    /// <summary>Writes a <see cref="HandleId"/> as a varuint.</summary>
    public static void WriteHandleId(this ref WireWriter writer, HandleId value) => writer.WriteVarUInt(value.Value);

    /// <summary>Reads a <see cref="HandleId"/>.</summary>
    public static HandleId ReadHandleId(this ref WireReader reader) => new(reader.ReadVarUInt());

    /// <summary>Writes a <see cref="StagingId"/> as a varuint.</summary>
    public static void WriteStagingId(this ref WireWriter writer, StagingId value) => writer.WriteVarUInt(value.Value);

    /// <summary>Reads a <see cref="StagingId"/>.</summary>
    public static StagingId ReadStagingId(this ref WireReader reader) => new(reader.ReadVarUInt());

    /// <summary>Writes a <see cref="SnapshotId"/> as a GUID.</summary>
    public static void WriteSnapshotId(this ref WireWriter writer, SnapshotId value) => writer.WriteGuid(value.Value);

    /// <summary>Reads a <see cref="SnapshotId"/>.</summary>
    public static SnapshotId ReadSnapshotId(this ref WireReader reader) => new(reader.ReadGuid());

    /// <summary>Writes a <see cref="DfsTransactionId"/> (its operation id).</summary>
    public static void WriteTransactionId(this ref WireWriter writer, DfsTransactionId value) => writer.WriteOperationId(value.Operation);

    /// <summary>Reads a <see cref="DfsTransactionId"/>.</summary>
    public static DfsTransactionId ReadTransactionId(this ref WireReader reader) => new(reader.ReadOperationId());

    // ---------------------------------------------------------------- optional values (bool presence flag + value)

    /// <summary>Writes a presence flag followed by the node id when present.</summary>
    public static void WriteOptionalNodeId(this ref WireWriter writer, NodeId? value)
    {
        writer.WriteBool(value.HasValue);
        if (value.HasValue)
            writer.WriteNodeId(value.Value);
    }

    /// <summary>Reads a presence flag followed by a node id when present.</summary>
    public static NodeId? ReadOptionalNodeId(this ref WireReader reader) => reader.ReadBool() ? reader.ReadNodeId() : null;

    /// <summary>Writes a presence flag followed by the commit token when present.</summary>
    public static void WriteOptionalCommitToken(this ref WireWriter writer, CommitToken? value)
    {
        writer.WriteBool(value.HasValue);
        if (value.HasValue)
            writer.WriteCommitToken(value.Value);
    }

    /// <summary>Reads a presence flag followed by a commit token when present.</summary>
    public static CommitToken? ReadOptionalCommitToken(this ref WireReader reader) => reader.ReadBool() ? reader.ReadCommitToken() : null;

    /// <summary>Writes a presence flag followed by the transaction id when present.</summary>
    public static void WriteOptionalTransactionId(this ref WireWriter writer, DfsTransactionId? value)
    {
        writer.WriteBool(value.HasValue);
        if (value.HasValue)
            writer.WriteTransactionId(value.Value);
    }

    /// <summary>Reads a presence flag followed by a transaction id when present.</summary>
    public static DfsTransactionId? ReadOptionalTransactionId(this ref WireReader reader) => reader.ReadBool() ? reader.ReadTransactionId() : null;

    /// <summary>Writes a presence flag followed by the timestamp when present.</summary>
    public static void WriteOptionalDateTimeOffset(this ref WireWriter writer, DateTimeOffset? value)
    {
        writer.WriteBool(value.HasValue);
        if (value.HasValue)
            writer.WriteDateTimeOffset(value.Value);
    }

    /// <summary>Reads a presence flag followed by a UTC timestamp when present.</summary>
    public static DateTimeOffset? ReadOptionalDateTimeOffset(this ref WireReader reader) => reader.ReadBool() ? reader.ReadDateTimeOffset() : null;

    /// <summary>Writes a presence flag followed by the duration when present.</summary>
    public static void WriteOptionalTimeSpan(this ref WireWriter writer, TimeSpan? value)
    {
        writer.WriteBool(value.HasValue);
        if (value.HasValue)
            writer.WriteTimeSpan(value.Value);
    }

    /// <summary>Reads a presence flag followed by a duration when present.</summary>
    public static TimeSpan? ReadOptionalTimeSpan(this ref WireReader reader) => reader.ReadBool() ? reader.ReadTimeSpan() : null;

    /// <summary>Writes a presence flag followed by the snapshot id when present.</summary>
    public static void WriteOptionalSnapshotId(this ref WireWriter writer, SnapshotId? value)
    {
        writer.WriteBool(value.HasValue);
        if (value.HasValue)
            writer.WriteSnapshotId(value.Value);
    }

    /// <summary>Reads a presence flag followed by a snapshot id when present.</summary>
    public static SnapshotId? ReadOptionalSnapshotId(this ref WireReader reader) => reader.ReadBool() ? reader.ReadSnapshotId() : null;

    /// <summary>Writes a required nested record.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    public static void WriteRecord<T>(this ref WireWriter writer, T value) where T : IWireRecord<T>
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value), $"A required {typeof(T).Name} is missing.");
        value.Write(ref writer);
    }

    /// <summary>Reads a required nested record.</summary>
    public static T ReadRecord<T>(this ref WireReader reader) where T : IWireRecord<T> => T.Read(ref reader);

    /// <summary>Writes a presence flag followed by the record when non-null.</summary>
    public static void WriteOptional<T>(this ref WireWriter writer, T? value) where T : class, IWireRecord<T>
    {
        writer.WriteBool(value is not null);
        value?.Write(ref writer);
    }

    /// <summary>Reads a presence flag followed by a record when present.</summary>
    public static T? ReadOptional<T>(this ref WireReader reader) where T : class, IWireRecord<T>
        => reader.ReadBool() ? T.Read(ref reader) : null;

    // ---------------------------------------------------------------- byte fields

    /// <summary>Reads a length-prefixed byte field into a new array (the result does not alias the input frame).</summary>
    public static ReadOnlyMemory<byte> ReadBytesCopy(this ref WireReader reader)
    {
        ReadOnlySpan<byte> span = reader.ReadBytes();
        return span.IsEmpty ? ReadOnlyMemory<byte>.Empty : span.ToArray();
    }

    /// <summary>Reads a length-prefixed byte field that must be exactly <paramref name="size"/> bytes long.</summary>
    public static ReadOnlyMemory<byte> ReadFixedBytes(this ref WireReader reader, int size, string field)
    {
        ReadOnlySpan<byte> span = reader.ReadBytes();
        if (span.Length != size)
            throw WireReader.Malformed($"{field} must be exactly {size} bytes (was {span.Length}).");
        return span.ToArray();
    }

    /// <summary>Reads a length-prefixed byte field that must be empty or exactly <paramref name="size"/> bytes long.</summary>
    public static ReadOnlyMemory<byte> ReadEmptyOrFixedBytes(this ref WireReader reader, int size, string field)
    {
        ReadOnlySpan<byte> span = reader.ReadBytes();
        if (span.Length != 0 && span.Length != size)
            throw WireReader.Malformed($"{field} must be empty or exactly {size} bytes (was {span.Length}).");
        return span.IsEmpty ? ReadOnlyMemory<byte>.Empty : span.ToArray();
    }

    /// <summary>Writes a list of byte fields.</summary>
    public static void WriteBytesList(this ref WireWriter writer, IReadOnlyList<ReadOnlyMemory<byte>>? items)
    {
        int count = items?.Count ?? 0;
        writer.WriteCount(count);
        for (int i = 0; i < count; i++)
            writer.WriteBytes(items![i].Span);
    }

    /// <summary>Reads a list of byte fields.</summary>
    public static IReadOnlyList<ReadOnlyMemory<byte>> ReadBytesList(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: 1);
        if (count == 0)
            return [];
        var result = new ReadOnlyMemory<byte>[count];
        for (int i = 0; i < count; i++)
            result[i] = reader.ReadBytesCopy();
        return result;
    }

    // ---------------------------------------------------------------- collections

    /// <summary>Writes a list of protocol records (null is written as an empty list).</summary>
    /// <exception cref="ArgumentException">The list contains a null element.</exception>
    public static void WriteList<T>(this ref WireWriter writer, IReadOnlyList<T>? items) where T : IWireRecord<T>
    {
        int count = items?.Count ?? 0;
        writer.WriteCount(count);
        for (int i = 0; i < count; i++)
        {
            T item = items![i];
            if (item is null)
                throw new ArgumentException($"A {typeof(T).Name} list must not contain null elements.", nameof(items));
            item.Write(ref writer);
        }
    }

    /// <summary>Reads a list of protocol records.</summary>
    public static IReadOnlyList<T> ReadList<T>(this ref WireReader reader) where T : IWireRecord<T>
    {
        int count = reader.ReadCount(Math.Max(1, T.MinEncodedSize));
        if (count == 0)
            return [];
        var result = new T[count];
        for (int i = 0; i < count; i++)
            result[i] = T.Read(ref reader);
        return result;
    }

    /// <summary>Writes a list of strings (null is written as an empty list).</summary>
    public static void WriteStringList(this ref WireWriter writer, IReadOnlyList<string>? items)
    {
        int count = items?.Count ?? 0;
        writer.WriteCount(count);
        for (int i = 0; i < count; i++)
            writer.WriteString(items![i] ?? throw new ArgumentException("A string list must not contain null elements.", nameof(items)));
    }

    /// <summary>Reads a list of strings.</summary>
    public static IReadOnlyList<string> ReadStringList(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: 1);
        if (count == 0)
            return [];
        var result = new string[count];
        for (int i = 0; i < count; i++)
            result[i] = reader.ReadString();
        return result;
    }

    /// <summary>Writes a set of strings in ascending ordinal order (canonical form).</summary>
    public static void WriteStringSet(this ref WireWriter writer, IEnumerable<string>? items)
    {
        string[] sorted = items is null ? [] : items.ToArray();
        Array.Sort(sorted, StringComparer.Ordinal);
        for (int i = 0; i < sorted.Length; i++)
        {
            if (sorted[i] is null)
                throw new ArgumentException("A string set must not contain null elements.", nameof(items));
            if (i > 0 && string.Equals(sorted[i - 1], sorted[i], StringComparison.Ordinal))
                throw new ArgumentException($"A string set must not contain duplicates ('{sorted[i]}').", nameof(items));
        }

        writer.WriteStringList(sorted);
    }

    /// <summary>Reads a set of strings; entries must be strictly ascending in ordinal order.</summary>
    public static IReadOnlySet<string> ReadStringSet(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: 1);
        if (count == 0)
            return ImmutableSortedSet.Create<string>(StringComparer.Ordinal);
        var builder = ImmutableSortedSet.CreateBuilder<string>(StringComparer.Ordinal);
        string? previous = null;
        for (int i = 0; i < count; i++)
        {
            string value = reader.ReadString();
            if (previous is not null && string.CompareOrdinal(previous, value) >= 0)
                throw WireReader.Malformed("String set entries must be distinct and in ascending ordinal order.");
            builder.Add(value);
            previous = value;
        }

        return builder.ToImmutable();
    }

    /// <summary>Writes a list of node ids.</summary>
    public static void WriteNodeIdList(this ref WireWriter writer, IReadOnlyList<NodeId>? items)
    {
        int count = items?.Count ?? 0;
        writer.WriteCount(count);
        for (int i = 0; i < count; i++)
            writer.WriteNodeId(items![i]);
    }

    /// <summary>Reads a list of node ids.</summary>
    public static IReadOnlyList<NodeId> ReadNodeIdList(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: 16);
        if (count == 0)
            return [];
        var result = new NodeId[count];
        for (int i = 0; i < count; i++)
            result[i] = reader.ReadNodeId();
        return result;
    }

    /// <summary>Writes a list of object ids.</summary>
    public static void WriteObjectIdList(this ref WireWriter writer, IReadOnlyList<ObjectId>? items)
    {
        int count = items?.Count ?? 0;
        writer.WriteCount(count);
        for (int i = 0; i < count; i++)
            writer.WriteObjectId(items![i]);
    }

    /// <summary>Reads a list of object ids.</summary>
    public static IReadOnlyList<ObjectId> ReadObjectIdList(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: ObjectId.ByteLength);
        if (count == 0)
            return [];
        var result = new ObjectId[count];
        for (int i = 0; i < count; i++)
            result[i] = reader.ReadObjectId();
        return result;
    }

    /// <summary>Writes a list of handle ids.</summary>
    public static void WriteHandleIdList(this ref WireWriter writer, IReadOnlyList<HandleId>? items)
    {
        int count = items?.Count ?? 0;
        writer.WriteCount(count);
        for (int i = 0; i < count; i++)
            writer.WriteHandleId(items![i]);
    }

    /// <summary>Reads a list of handle ids.</summary>
    public static IReadOnlyList<HandleId> ReadHandleIdList(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: 1);
        if (count == 0)
            return [];
        var result = new HandleId[count];
        for (int i = 0; i < count; i++)
            result[i] = reader.ReadHandleId();
        return result;
    }

    /// <summary>Writes a list of unsigned 64-bit values as varuints.</summary>
    public static void WriteUInt64List(this ref WireWriter writer, IReadOnlyList<ulong>? items)
    {
        int count = items?.Count ?? 0;
        writer.WriteCount(count);
        for (int i = 0; i < count; i++)
            writer.WriteVarUInt(items![i]);
    }

    /// <summary>Reads a list of unsigned 64-bit values.</summary>
    public static IReadOnlyList<ulong> ReadUInt64List(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: 1);
        if (count == 0)
            return [];
        var result = new ulong[count];
        for (int i = 0; i < count; i++)
            result[i] = reader.ReadVarUInt();
        return result;
    }

    /// <summary>Writes a string-to-string map in ascending ordinal key order (canonical form).</summary>
    public static void WriteStringMap(this ref WireWriter writer, IReadOnlyDictionary<string, string>? map)
    {
        KeyValuePair<string, string>[] entries = SortedEntries(map);
        writer.WriteCount(entries.Length);
        foreach (KeyValuePair<string, string> entry in entries)
        {
            writer.WriteString(entry.Key);
            writer.WriteString(entry.Value ?? throw new ArgumentException($"Map value for '{entry.Key}' must not be null.", nameof(map)));
        }
    }

    /// <summary>Reads a string-to-string map; keys must be strictly ascending in ordinal order.</summary>
    public static IReadOnlyDictionary<string, string> ReadStringMap(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: 2);
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        string? previous = null;
        for (int i = 0; i < count; i++)
        {
            string key = reader.ReadString();
            CheckAscending(previous, key);
            result.Add(key, reader.ReadString());
            previous = key;
        }

        return result;
    }

    /// <summary>Writes a map of named storage policies in ascending ordinal name order (canonical form).</summary>
    public static void WritePolicyMap(this ref WireWriter writer, IReadOnlyDictionary<string, DfsStoragePolicy>? map)
    {
        KeyValuePair<string, DfsStoragePolicy>[] entries = SortedEntries(map);
        writer.WriteCount(entries.Length);
        foreach (KeyValuePair<string, DfsStoragePolicy> entry in entries)
        {
            writer.WriteString(entry.Key);
            writer.WriteStoragePolicy(entry.Value);
        }
    }

    /// <summary>Reads a map of named storage policies; names must be strictly ascending in ordinal order.</summary>
    public static IReadOnlyDictionary<string, DfsStoragePolicy> ReadPolicyMap(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: 5);
        var result = new SortedDictionary<string, DfsStoragePolicy>(StringComparer.Ordinal);
        string? previous = null;
        for (int i = 0; i < count; i++)
        {
            string key = reader.ReadString();
            CheckAscending(previous, key);
            result.Add(key, reader.ReadStoragePolicy());
            previous = key;
        }

        return result;
    }

    // ---------------------------------------------------------------- Core value types

    /// <summary>Writes topology labels (region, zone, rack, host, device).</summary>
    public static void WriteTopologyLabels(this ref WireWriter writer, DfsTopologyLabels value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteString(value.Region);
        writer.WriteString(value.Zone);
        writer.WriteString(value.Rack);
        writer.WriteString(value.Host);
        writer.WriteString(value.Device);
    }

    /// <summary>Reads topology labels.</summary>
    public static DfsTopologyLabels ReadTopologyLabels(this ref WireReader reader)
    {
        string region = reader.ReadString();
        string zone = reader.ReadString();
        string rack = reader.ReadString();
        string host = reader.ReadString();
        string device = reader.ReadString();
        return new DfsTopologyLabels(region, zone, rack, host, device);
    }

    /// <summary>
    /// Writes a storage policy as four zigzag varints followed by the optional pool name (presence flag + string)
    /// (values are not validated; receivers do that).
    /// </summary>
    public static void WriteStoragePolicy(this ref WireWriter writer, DfsStoragePolicy value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteVarInt32(value.TargetCopies);
        writer.WriteVarInt32(value.TargetDistinctZones);
        writer.WriteVarInt32(value.DurableCopiesBeforeAck);
        writer.WriteVarInt32(value.DurableZonesBeforeAck);
        writer.WriteBool(value.Pool is not null);
        if (value.Pool is not null)
            writer.WriteString(value.Pool);
    }

    /// <summary>Reads a storage policy (not validated: an inconsistent policy is a semantic error for the receiver).</summary>
    public static DfsStoragePolicy ReadStoragePolicy(this ref WireReader reader)
    {
        int copies = reader.ReadVarInt32();
        int zones = reader.ReadVarInt32();
        int ackCopies = reader.ReadVarInt32();
        int ackZones = reader.ReadVarInt32();
        string? pool = reader.ReadBool() ? reader.ReadString() : null;
        return new DfsStoragePolicy
        {
            TargetCopies = copies,
            TargetDistinctZones = zones,
            DurableCopiesBeforeAck = ackCopies,
            DurableZonesBeforeAck = ackZones,
            Pool = pool,
        };
    }

    /// <summary>Writes a presence flag followed by the policy when non-null.</summary>
    public static void WriteOptionalStoragePolicy(this ref WireWriter writer, DfsStoragePolicy? value)
    {
        writer.WriteBool(value is not null);
        if (value is not null)
            writer.WriteStoragePolicy(value);
    }

    /// <summary>Reads a presence flag followed by a policy when present.</summary>
    public static DfsStoragePolicy? ReadOptionalStoragePolicy(this ref WireReader reader) => reader.ReadBool() ? reader.ReadStoragePolicy() : null;

    /// <summary>Writes a metadata policy (voters, placement, optional explicit voter node names).</summary>
    public static void WriteMetadataPolicy(this ref WireWriter writer, DfsMetadataPolicy value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteVarInt32(value.Voters);
        writer.WriteEnum(value.Placement);
        writer.WriteBool(value.ExplicitVoterNodes is not null);
        if (value.ExplicitVoterNodes is not null)
            writer.WriteStringList(value.ExplicitVoterNodes);
    }

    /// <summary>Reads a metadata policy (placement is range-checked but not validated; receivers call <see cref="DfsMetadataPolicy.Validate"/>).</summary>
    public static DfsMetadataPolicy ReadMetadataPolicy(this ref WireReader reader)
    {
        int voters = reader.ReadVarInt32();
        DfsVoterPlacement placement = reader.ReadEnumRaw<DfsVoterPlacement>();
        IReadOnlyList<string>? explicitNodes = reader.ReadBool() ? reader.ReadStringList() : null;
        return new DfsMetadataPolicy { Voters = voters, Placement = placement, ExplicitVoterNodes = explicitNodes };
    }

    /// <summary>Writes volume limits.</summary>
    public static void WriteVolumeLimits(this ref WireWriter writer, DfsVolumeLimits value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteVarInt(value.MaxFileLength);
        writer.WriteVarInt(value.MaxPublishBytes);
        writer.WriteTimeSpan(value.MaxTransactionDuration);
        writer.WriteVarInt32(value.MaxTransactionTouchedInodes);
        writer.WriteVarInt32(value.MaxTransactionDescriptorBytes);
        writer.WriteVarInt(value.MaxTransactionStagedBytes);
        writer.WriteVarInt32(value.MaxPinsPerSession);
        writer.WriteVarInt32(value.MaxHandlesPerSession);
    }

    /// <summary>Reads volume limits.</summary>
    public static DfsVolumeLimits ReadVolumeLimits(this ref WireReader reader)
    {
        long maxFileLength = reader.ReadVarInt();
        long maxPublishBytes = reader.ReadVarInt();
        TimeSpan maxTxDuration = reader.ReadTimeSpan();
        int maxTouched = reader.ReadVarInt32();
        int maxDescriptor = reader.ReadVarInt32();
        long maxStaged = reader.ReadVarInt();
        int maxPins = reader.ReadVarInt32();
        int maxHandles = reader.ReadVarInt32();
        return new DfsVolumeLimits
        {
            MaxFileLength = maxFileLength,
            MaxPublishBytes = maxPublishBytes,
            MaxTransactionDuration = maxTxDuration,
            MaxTransactionTouchedInodes = maxTouched,
            MaxTransactionDescriptorBytes = maxDescriptor,
            MaxTransactionStagedBytes = maxStaged,
            MaxPinsPerSession = maxPins,
            MaxHandlesPerSession = maxHandles,
        };
    }

    /// <summary>Writes server capabilities (features in canonical order).</summary>
    public static void WriteServerCapabilities(this ref WireWriter writer, DfsServerCapabilities value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteVarInt32(value.ProtocolVersion);
        writer.WriteStringSet(value.Features);
        writer.WriteVarInt(value.MaxFileLength);
        writer.WriteVarInt32(value.MaxPathBytes);
        writer.WriteVarInt32(value.MaxComponentBytes);
        writer.WriteVarInt(value.MaxPublishBytes);
        writer.WriteVarInt32(value.InlineThreshold);
        writer.WriteVarInt32(value.ChunkSize);
        writer.WriteVarInt32(value.MaxObjectSize);
        writer.WriteVolumeLimits(value.TransactionLimits);
        writer.WriteTimeSpan(value.LeaseDuration);
    }

    /// <summary>Reads server capabilities.</summary>
    public static DfsServerCapabilities ReadServerCapabilities(this ref WireReader reader)
    {
        int version = reader.ReadVarInt32();
        IReadOnlySet<string> features = reader.ReadStringSet();
        long maxFileLength = reader.ReadVarInt();
        int maxPathBytes = reader.ReadVarInt32();
        int maxComponentBytes = reader.ReadVarInt32();
        long maxPublishBytes = reader.ReadVarInt();
        int inlineThreshold = reader.ReadVarInt32();
        int chunkSize = reader.ReadVarInt32();
        int maxObjectSize = reader.ReadVarInt32();
        DfsVolumeLimits limits = reader.ReadVolumeLimits();
        TimeSpan lease = reader.ReadTimeSpan();
        return new DfsServerCapabilities
        {
            ProtocolVersion = version,
            Features = features,
            MaxFileLength = maxFileLength,
            MaxPathBytes = maxPathBytes,
            MaxComponentBytes = maxComponentBytes,
            MaxPublishBytes = maxPublishBytes,
            InlineThreshold = inlineThreshold,
            ChunkSize = chunkSize,
            MaxObjectSize = maxObjectSize,
            TransactionLimits = limits,
            LeaseDuration = lease,
        };
    }

    /// <summary>Writes a retention (optional expiry, hold flag).</summary>
    public static void WriteRetention(this ref WireWriter writer, DfsRetention value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteOptionalDateTimeOffset(value.ExpiresUtc);
        writer.WriteBool(value.Hold);
    }

    /// <summary>Reads a retention.</summary>
    public static DfsRetention ReadRetention(this ref WireReader reader)
    {
        DateTimeOffset? expires = reader.ReadOptionalDateTimeOffset();
        bool hold = reader.ReadBool();
        return new DfsRetention(expires, hold);
    }

    /// <summary>Writes a presence flag followed by the retention when non-null.</summary>
    public static void WriteOptionalRetention(this ref WireWriter writer, DfsRetention? value)
    {
        writer.WriteBool(value is not null);
        if (value is not null)
            writer.WriteRetention(value);
    }

    /// <summary>Reads a presence flag followed by a retention when present.</summary>
    public static DfsRetention? ReadOptionalRetention(this ref WireReader reader) => reader.ReadBool() ? reader.ReadRetention() : null;

    /// <summary>Writes a backup checkpoint.</summary>
    public static void WriteBackupCheckpoint(this ref WireWriter writer, DfsBackupCheckpoint value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteString(value.Target);
        writer.WriteSnapshotId(value.SnapshotId);
        writer.WriteString(value.SnapshotName);
        writer.WriteCommitToken(value.CoveredCommit);
        writer.WriteOptionalDateTimeOffset(value.RetentionExpiryUtc);
        writer.WriteBool(value.Held);
        writer.WriteBool(value.IsExactSnapshot);
    }

    /// <summary>Reads a backup checkpoint.</summary>
    public static DfsBackupCheckpoint ReadBackupCheckpoint(this ref WireReader reader)
    {
        string target = reader.ReadString();
        SnapshotId snapshot = reader.ReadSnapshotId();
        string name = reader.ReadString();
        CommitToken covered = reader.ReadCommitToken();
        DateTimeOffset? expiry = reader.ReadOptionalDateTimeOffset();
        bool held = reader.ReadBool();
        bool exact = reader.ReadBool();
        return new DfsBackupCheckpoint(target, snapshot, name, covered, expiry, held, exact);
    }

    /// <summary>Writes a presence flag followed by the checkpoint when non-null.</summary>
    public static void WriteOptionalBackupCheckpoint(this ref WireWriter writer, DfsBackupCheckpoint? value)
    {
        writer.WriteBool(value is not null);
        if (value is not null)
            writer.WriteBackupCheckpoint(value);
    }

    /// <summary>Reads a presence flag followed by a checkpoint when present.</summary>
    public static DfsBackupCheckpoint? ReadOptionalBackupCheckpoint(this ref WireReader reader) => reader.ReadBool() ? reader.ReadBackupCheckpoint() : null;

    /// <summary>Writes credentials (principal and secret). Only used inside TLS-protected administrative requests.</summary>
    public static void WriteCredentials(this ref WireWriter writer, DfsCredentials value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteString(value.Principal);
        writer.WriteString(value.Secret);
    }

    /// <summary>Reads credentials.</summary>
    public static DfsCredentials ReadCredentials(this ref WireReader reader)
    {
        string principal = reader.ReadString();
        string secret = reader.ReadString();
        return new DfsCredentials(principal, secret);
    }

    /// <summary>Writes a presence flag followed by the credentials when non-null.</summary>
    public static void WriteOptionalCredentials(this ref WireWriter writer, DfsCredentials? value)
    {
        writer.WriteBool(value is not null);
        if (value is not null)
            writer.WriteCredentials(value);
    }

    /// <summary>Reads a presence flag followed by credentials when present.</summary>
    public static DfsCredentials? ReadOptionalCredentials(this ref WireReader reader) => reader.ReadBool() ? reader.ReadCredentials() : null;

    /// <summary>Writes a list of unsigned 32-bit values as fixed 4-byte little-endian integers (checksums).</summary>
    public static void WriteUInt32List(this ref WireWriter writer, IReadOnlyList<uint>? items)
    {
        int count = items?.Count ?? 0;
        writer.WriteCount(count);
        for (int i = 0; i < count; i++)
            writer.WriteUInt32(items![i]);
    }

    /// <summary>Reads a list of fixed-width unsigned 32-bit values.</summary>
    public static IReadOnlyList<uint> ReadUInt32List(this ref WireReader reader)
    {
        int count = reader.ReadCount(minElementBytes: sizeof(uint));
        if (count == 0)
            return [];
        var result = new uint[count];
        for (int i = 0; i < count; i++)
            result[i] = reader.ReadUInt32();
        return result;
    }

    // ---------------------------------------------------------------- helpers

    private static KeyValuePair<string, TValue>[] SortedEntries<TValue>(IReadOnlyDictionary<string, TValue>? map)
    {
        if (map is null || map.Count == 0)
            return [];
        KeyValuePair<string, TValue>[] entries = map.ToArray();
        Array.Sort(entries, static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        foreach (KeyValuePair<string, TValue> entry in entries)
        {
            if (entry.Key is null)
                throw new ArgumentException("Map keys must not be null.", nameof(map));
        }

        return entries;
    }

    private static void CheckAscending(string? previous, string key)
    {
        if (previous is not null && string.CompareOrdinal(previous, key) >= 0)
            throw WireReader.Malformed("Map keys must be distinct and in ascending ordinal order.");
    }
}
