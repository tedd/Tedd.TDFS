namespace Tedd.Tdfs.Storage;

/// <summary>Configuration of a <see cref="Journal"/>.</summary>
/// <param name="SegmentSize">Target size of one journal segment file.</param>
/// <param name="Cipher">Record cipher for encryption at rest; null stores plaintext.</param>
public sealed record JournalOptions(long SegmentSize = 16 << 20, IRecordCipher? Cipher = null);
