namespace Tedd.Tdfs.Storage;

/// <summary>Configuration of an <see cref="ObjectStore"/>.</summary>
/// <param name="SegmentSize">Target size of a packed segment file before a new one is started.</param>
/// <param name="ReservedBytes">Space kept free for journals, checkpoints, compaction and repair; uploads fail with capacity errors before touching it.</param>
/// <param name="MaxObjectSize">Largest accepted object (plaintext bytes).</param>
/// <param name="Cipher">
/// Node-wide record cipher for encryption at rest; null stores plaintext unless <see cref="VolumeKeys"/> is set. With
/// <see cref="VolumeKeys"/> it only decrypts records sealed before per-volume keys were used (<see cref="ObjectEncoding.SealedUnits"/>).
/// </param>
public sealed record ObjectStoreOptions(long SegmentSize = 64 << 20, long ReservedBytes = 16 << 20, int MaxObjectSize = 4 << 20, IRecordCipher? Cipher = null)
{
    /// <summary>
    /// Negative-control switches (MUT-* tests only). The store honours <see cref="SafetyMutations.AcknowledgeBeforeFlush"/>
    /// (puts return before the flush barrier) and <see cref="SafetyMutations.SkipChecksumVerification"/> (reads return
    /// stored plaintext bytes without verifying unit checksums).
    /// </summary>
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;

    /// <summary>
    /// Per-volume data-encryption keys (ARCHITECTURE §11). When set, new records are sealed with a key version of their
    /// owning volume (<see cref="ObjectEncoding.SealedVolumeUnits"/>, the version recorded in the record header) instead of
    /// <see cref="Cipher"/>.
    /// </summary>
    public IVolumeKeyProvider? VolumeKeys { get; init; }

    /// <summary>Node name reported in fault-injection contexts.</summary>
    public string NodeName { get; init; } = "local";

    /// <summary>
    /// Bytes of the reserve that <see cref="ObjectAdmission.Repair"/> puts must leave free (metadata progress and one
    /// compaction step); null means half of <see cref="ReservedBytes"/>.
    /// </summary>
    public long? RepairFloorBytes { get; init; }

    /// <summary>The effective repair floor: <see cref="RepairFloorBytes"/>, or half of <see cref="ReservedBytes"/>, never above the reserve.</summary>
    /// <remarks>
    /// The lower half of the reserve always stays free for metadata progress and compaction; the upper half is the repair
    /// reserve: after a failure, re-replication may fill it, while normal admission never does.
    /// </remarks>
    public long EffectiveRepairFloorBytes => Math.Clamp(RepairFloorBytes ?? (ReservedBytes / 2), 0, ReservedBytes);

    /// <summary>
    /// Configured capacity of the store's node (null = the medium's capacity). Admission uses the smaller of this and the
    /// medium's capacity, so a node configured smaller than its device (for example a metadata witness configured with no
    /// data capacity) never admits payload beyond its configuration.
    /// </summary>
    public long? CapacityLimitBytes { get; init; }
}
