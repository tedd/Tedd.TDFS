using System.Collections.Immutable;

namespace Tedd.Tdfs.Server.Volumes.State;

/// <summary>Kind of an inode.</summary>
internal enum InodeKind : byte
{
    File = 0,
    Directory = 1,
}

/// <summary>
/// One immutable extent of a stream (ARCHITECTURE §5.1): logical range <c>[Offset, Offset + Length)</c> maps to object bytes
/// <c>[ObjectOffset, ObjectOffset + Length)</c>. <see cref="ObjectLength"/>, <see cref="Checksum"/> and
/// <see cref="Encoding"/> describe the whole object (needed to verify it).
/// </summary>
/// <param name="Offset">Logical offset.</param>
/// <param name="Length">Logical length (&gt; 0).</param>
/// <param name="Object">Object holding the bytes.</param>
/// <param name="ObjectOffset">Offset of the extent's first byte inside the object.</param>
/// <param name="ObjectLength">Full plaintext length of the object.</param>
/// <param name="Checksum">XxHash128 of the whole object plaintext.</param>
/// <param name="Encoding">Object encoding.</param>
/// <param name="PlacementEpoch">Placement epoch the object was placed under.</param>
/// <param name="Staging">Staging session the object was uploaded under (GC classification hint).</param>
/// <param name="Locations">Nodes that issued durable receipts at publication (read hints, receipt order).</param>
internal sealed record ExtentRef(long Offset, long Length, ObjectId Object, long ObjectOffset, long ObjectLength, UInt128 Checksum, byte Encoding,
    ulong PlacementEpoch, StagingId Staging, ImmutableArray<NodeId> Locations) : IContentSegment
{
    /// <summary>End of the logical range (exclusive).</summary>
    public long End => Offset + Length;

    /// <summary>A copy covering <c>[start, end)</c> of this extent (clamped to it).</summary>
    public ExtentRef Slice(long start, long end)
    {
        long s = Math.Max(start, Offset);
        long e = Math.Min(end, End);
        if (s == Offset && e == End)
            return this;
        return this with { Offset = s, Length = e - s, ObjectOffset = ObjectOffset + (s - Offset) };
    }

    /// <inheritdoc />
    public IContentSegment SliceSegment(long start, long end) => Slice(start, end);
}

/// <summary>
/// One stream of an inode (ARCHITECTURE §17): the default stream (<c>""</c>) carries the file contents; named streams have
/// their own data, length and revision. Contents are either inline bytes (exactly <see cref="Length"/> bytes) or a sorted,
/// non-overlapping extent list (gaps below the length are holes that read as zeroes).
/// </summary>
/// <param name="Length">Logical length.</param>
/// <param name="IsInline">True when the bytes are stored inline (no objects).</param>
/// <param name="Inline">Inline bytes (exactly <see cref="Length"/> bytes when <see cref="IsInline"/>; empty otherwise). Never mutated.</param>
/// <param name="Extents">Extents in ascending offset order (empty when inline).</param>
/// <param name="Revision">Raft index of the last change of this stream.</param>
/// <param name="CreatedIndex">Raft index at which the stream was created.</param>
internal sealed record StreamRecord(long Length, bool IsInline, ReadOnlyMemory<byte> Inline, ImmutableList<ExtentRef> Extents, ulong Revision, ulong CreatedIndex)
{
    /// <summary>An empty (inline, zero-length) stream created at <paramref name="index"/>.</summary>
    public static StreamRecord Empty(ulong index) => new(0, true, ReadOnlyMemory<byte>.Empty, ImmutableList<ExtentRef>.Empty, index, index);

    /// <summary>True when the stream has holes (ranges below the length without extents).</summary>
    public bool IsSparse
    {
        get
        {
            if (IsInline || Length == 0)
                return false;
            long cursor = 0;
            foreach (ExtentRef e in Extents)
            {
                if (e.Offset > cursor)
                    return true;
                cursor = e.End;
            }

            return cursor < Length;
        }
    }

    /// <summary>Distinct objects referenced by the stream.</summary>
    public IEnumerable<ObjectId> Objects()
    {
        ObjectId previous = default;
        foreach (ExtentRef e in Extents)
        {
            if (e.Object != previous)
                yield return e.Object;
            previous = e.Object;
        }
    }
}

/// <summary>
/// An immutable inode version (ARCHITECTURE §5.1, §17). Every modification produces a new record whose
/// <see cref="Revision"/> is the index of the modifying entry, so <c>(Id, Revision)</c> identifies a version uniquely
/// (checkpoints store each distinct version once). Writer generations and the detached flag are kept outside the record
/// (<see cref="VolumeState.WriterGenerations"/>, <see cref="VolumeState.Detached"/>): they change without creating a new
/// revision.
/// </summary>
/// <param name="Id">Inode id (root = 1).</param>
/// <param name="Kind">File or directory.</param>
/// <param name="Revision">Raft index of the last modification (content, any stream, metadata, ACL, name).</param>
/// <param name="CreatedIndex">Raft index of creation.</param>
/// <param name="CreatedUtc">Creation time (leader clock carried in the entry).</param>
/// <param name="ModifiedUtc">Last modification time (leader clock carried in the entry).</param>
/// <param name="Security">Owner, explicit entries (canonical order, no inherited entries) and the protection flag.</param>
/// <param name="PolicyName">Named storage policy (empty = the volume default).</param>
/// <param name="ParentId">Parent directory (0 for the root).</param>
/// <param name="Name">Entry name in the parent (empty for the root).</param>
/// <param name="Streams">Streams by name: files always have the default stream <c>""</c>; directories only named streams.</param>
/// <param name="Children">Directory entries (ordinal names → inode ids); empty for files.</param>
internal sealed record InodeRecord(ulong Id, InodeKind Kind, ulong Revision, ulong CreatedIndex, DateTimeOffset CreatedUtc, DateTimeOffset ModifiedUtc,
    DfsSecurityDescriptor Security, string PolicyName, ulong ParentId, string Name, ImmutableSortedDictionary<string, StreamRecord> Streams,
    ImmutableSortedDictionary<string, ulong> Children)
{
    /// <summary>Root inode id.</summary>
    public const ulong RootId = 1;

    /// <summary>
    /// Raft index of the last change of the inode's own metadata — creation, owner, ACL, storage policy, name or parent — as
    /// opposed to <see cref="Revision"/>, which also advances for changes of any stream or of the children.
    /// </summary>
    public ulong MetadataRevision { get; init; }

    /// <summary>
    /// The revision reported to clients for the inode itself (<c>FileInfoData.Revision</c> of a plain path, directory
    /// listings, inspection): for a file the latest change of its default stream or of its metadata — a write to a named
    /// stream does not change it (ADS-011), an ACL or policy change does (ACL-07, NS-018); for a directory
    /// <see cref="Revision"/> (entries and metadata).
    /// </summary>
    public ulong ReportedRevision => IsDirectory ? Revision : Math.Max(MetadataRevision, DefaultStream?.Revision ?? 0);

    /// <summary>The revision reported for <paramref name="stream"/> (<c>""</c> = the inode's own reported revision).</summary>
    public ulong RevisionOf(string stream) => stream.Length == 0 ? ReportedRevision : GetStream(stream)?.Revision ?? 0;

    /// <summary>True for directories.</summary>
    public bool IsDirectory => Kind == InodeKind.Directory;

    /// <summary>The owner identity.</summary>
    public DfsIdentity Owner => Security.Owner;

    /// <summary>The default stream of a file (null for directories).</summary>
    public StreamRecord? DefaultStream => Streams.GetValueOrDefault(DfsStreamPath.DefaultStreamName);

    /// <summary>Number of named streams.</summary>
    public int NamedStreamCount => Streams.ContainsKey(DfsStreamPath.DefaultStreamName) ? Streams.Count - 1 : Streams.Count;

    /// <summary>The stream with <paramref name="name"/>, or null.</summary>
    public StreamRecord? GetStream(string name) => Streams.GetValueOrDefault(name);

    /// <summary>Every distinct object referenced by any stream.</summary>
    public IEnumerable<ObjectId> Objects()
    {
        foreach (StreamRecord s in Streams.Values)
        {
            foreach (ObjectId o in s.Objects())
                yield return o;
        }
    }

    /// <summary>Empty stream map.</summary>
    public static ImmutableSortedDictionary<string, StreamRecord> NoStreams { get; } = ImmutableSortedDictionary.Create<string, StreamRecord>(StringComparer.Ordinal);

    /// <summary>Empty children map.</summary>
    public static ImmutableSortedDictionary<string, ulong> NoChildren { get; } = ImmutableSortedDictionary.Create<string, ulong>(StringComparer.Ordinal);
}
