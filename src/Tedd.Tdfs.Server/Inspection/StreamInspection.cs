namespace Tedd.Tdfs.Server;

/// <summary>Applied metadata of one named stream of a file (Addendum A, ADS-02).</summary>
/// <param name="Name">Stream name.</param>
/// <param name="Length">Stream length.</param>
/// <param name="Revision">Raft index of the stream's last modification.</param>
/// <param name="Extents">Extent map in offset order (empty for inline streams).</param>
public sealed record StreamInspection(string Name, long Length, ulong Revision, IReadOnlyList<ExtentInspection> Extents);
