namespace Tedd.Tdfs.Storage;

/// <summary>Configuration of an <see cref="ObjectStore"/>.</summary>
/// <param name="SegmentSize">Target size of a packed segment file before a new one is started.</param>
/// <param name="ReservedBytes">Space kept free for journals, checkpoints, compaction and repair; uploads fail with capacity errors before touching it.</param>
/// <param name="MaxObjectSize">Largest accepted object (plaintext bytes).</param>
/// <param name="Cipher">Record cipher for encryption at rest; null stores plaintext.</param>
public sealed record ObjectStoreOptions(long SegmentSize = 64 << 20, long ReservedBytes = 16 << 20, int MaxObjectSize = 4 << 20, IRecordCipher? Cipher = null)
{
    /// <summary>
    /// Negative-control switches (MUT-* tests only). The store honours <see cref="SafetyMutations.AcknowledgeBeforeFlush"/>
    /// (puts return before the flush barrier) and <see cref="SafetyMutations.SkipChecksumVerification"/> (reads return
    /// stored plaintext bytes without verifying unit checksums).
    /// </summary>
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;

    /// <summary>Node name reported in fault-injection contexts.</summary>
    public string NodeName { get; init; } = "local";
}
