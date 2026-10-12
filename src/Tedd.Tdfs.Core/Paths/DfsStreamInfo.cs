namespace Tedd.Tdfs;

/// <summary>A named alternate data stream of a file or directory (Addendum A, ADS-03).</summary>
/// <param name="Name">Stream name (NFC; without the file name or ':').</param>
/// <param name="Length">Stream length in bytes.</param>
/// <param name="Revision">Revision of the stream (log index of its last modification).</param>
public sealed record DfsStreamInfo(string Name, long Length, ulong Revision);
