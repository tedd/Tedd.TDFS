namespace Tedd.Tdfs.Storage;

/// <summary>Configuration of an <see cref="ObjectStore"/>.</summary>
/// <param name="SegmentSize">Target size of a packed segment file before a new one is started.</param>
/// <param name="ReservedBytes">Space kept free for journals, checkpoints, compaction and repair; uploads fail with capacity errors before touching it.</param>
/// <param name="MaxObjectSize">Largest accepted object (plaintext bytes).</param>
/// <param name="Cipher">Record cipher for encryption at rest; null stores plaintext.</param>
public sealed record ObjectStoreOptions(long SegmentSize = 64 << 20, long ReservedBytes = 16 << 20, int MaxObjectSize = 4 << 20, IRecordCipher? Cipher = null);
