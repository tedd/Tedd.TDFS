namespace Tedd.Tdfs.Server;

/// <summary>One extent of a file.</summary>
/// <param name="Offset">File offset.</param>
/// <param name="Length">Extent length.</param>
/// <param name="Object">Backing object.</param>
/// <param name="ObjectOffset">Offset within the object.</param>
/// <param name="ObjectLength">Total object length.</param>
public sealed record ExtentInspection(long Offset, long Length, ObjectId Object, long ObjectOffset, long ObjectLength);
