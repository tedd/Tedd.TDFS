namespace Tedd.Tdfs.Server.Volumes.State;

/// <summary>
/// Identifies one stream of an inode: <c>("" )</c> is the default stream, any other name a named stream (Addendum A,
/// ADS-02). Sharing, writer reservations and fencing generations are keyed by it (ARCHITECTURE §17).
/// </summary>
/// <param name="Inode">Inode id.</param>
/// <param name="Stream">Stream name (<see cref="DfsStreamPath.DefaultStreamName"/> for the default stream).</param>
internal readonly record struct StreamKey(ulong Inode, string Stream) : IComparable<StreamKey>
{
    /// <summary>Ordinal ordering by inode, then stream name.</summary>
    public int CompareTo(StreamKey other)
    {
        int c = Inode.CompareTo(other.Inode);
        return c != 0 ? c : string.CompareOrdinal(Stream, other.Stream);
    }

    /// <summary>True for the default (unnamed) stream.</summary>
    public bool IsDefault => Stream.Length == 0;

    /// <inheritdoc />
    public override string ToString() => IsDefault ? Inode.ToString(System.Globalization.CultureInfo.InvariantCulture)
        : Inode.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + Stream;
}

/// <summary>Deterministic comparer of <see cref="StreamKey"/>.</summary>
internal sealed class StreamKeyComparer : IComparer<StreamKey>
{
    public static readonly StreamKeyComparer Instance = new();

    public int Compare(StreamKey x, StreamKey y) => x.CompareTo(y);
}

/// <summary>Deterministic comparer of operation ids (session, then sequence).</summary>
internal sealed class OperationIdComparer : IComparer<DfsOperationId>
{
    public static readonly OperationIdComparer Instance = new();

    public int Compare(DfsOperationId x, DfsOperationId y)
    {
        int c = x.Session.CompareTo(y.Session);
        return c != 0 ? c : x.Sequence.CompareTo(y.Sequence);
    }
}
