namespace Tedd.Tdfs.Storage;

/// <summary>Configuration of a <see cref="Journal"/>.</summary>
/// <param name="SegmentSize">Target size of one journal segment file.</param>
/// <param name="Cipher">Record cipher for encryption at rest; null stores plaintext.</param>
public sealed record JournalOptions(long SegmentSize = 16 << 20, IRecordCipher? Cipher = null)
{
    /// <summary>Default <see cref="MaxRecordSize"/> (64 MiB).</summary>
    public const int DefaultMaxRecordSize = 64 << 20;

    /// <summary>
    /// Index of the first record when the journal directory holds no segment (a new journal, or one whose every segment
    /// was dropped). Ignored when segments exist: their headers are authoritative. Callers that compact (for example
    /// the Raft log store after a snapshot) pass the first index they expect to retain. Default 1.
    /// </summary>
    public long FirstIndex { get; init; } = 1;

    /// <summary>Largest accepted record payload in bytes (plaintext). Default <see cref="DefaultMaxRecordSize"/>.</summary>
    public int MaxRecordSize { get; init; } = DefaultMaxRecordSize;
}
