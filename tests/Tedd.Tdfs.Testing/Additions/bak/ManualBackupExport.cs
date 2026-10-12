namespace Tedd.Tdfs.Testing;

/// <summary>
/// Step-by-step driver of one late-sync export of a live snapshot to a backup node (BAK additions; created by
/// <see cref="TestCluster.BeginManualBackupExportAsync"/>). Each step sends the real wire message the automatic exporter
/// would send (<c>BackupBegin</c>, <c>BackupInventory</c>, <c>BackupPutManifest</c>, <c>BackupPutObject</c>,
/// <c>BackupPublish</c>) and returns the destination's answer; steps may be called in any order and repeated, so tests
/// can deliver the manifest before data, withhold or tamper with dependencies, resume after a destination restart and
/// re-deliver duplicates. The driver never retries on its own and never skips a step the test did not request.
/// </summary>
public sealed class ManualBackupExport : IAsyncDisposable
{
    internal ManualBackupExport()
    {
    }

    /// <summary>Source volume name.</summary>
    public string Volume => throw Stub.NotYet("ManualBackupExport.Volume");

    /// <summary>Exported snapshot name.</summary>
    public string SnapshotName => throw Stub.NotYet("ManualBackupExport.SnapshotName");

    /// <summary>Exported snapshot id.</summary>
    public SnapshotId SnapshotId => throw Stub.NotYet("ManualBackupExport.SnapshotId");

    /// <summary>Commit boundary of the exported snapshot.</summary>
    public CommitToken Boundary => throw Stub.NotYet("ManualBackupExport.Boundary");

    /// <summary>Target (registration) name of the destination backup node.</summary>
    public string TargetName => throw Stub.NotYet("ManualBackupExport.TargetName");

    /// <summary>Lineage id this transfer runs under.</summary>
    public Guid LineageId => throw Stub.NotYet("ManualBackupExport.LineageId");

    /// <summary>
    /// The snapshot's complete object dependency closure computed from the pinned source snapshot root (every object
    /// referenced by any extent of any file; sorted, distinct; inline files contribute nothing).
    /// </summary>
    public IReadOnlyList<ObjectId> Closure => throw Stub.NotYet("ManualBackupExport.Closure");

    /// <summary>
    /// Sends <c>BackupBegin</c> (lineage, volume id/epoch, snapshot id, boundary). Returns <see cref="DfsErrorCode.None"/>
    /// when accepted, otherwise the destination's error code (e.g. <see cref="DfsErrorCode.EpochMismatch"/>). May be
    /// called again after a destination restart to resume the same transfer.
    /// </summary>
    public Task<DfsErrorCode> BeginAsync() => throw Stub.NotYet("ManualBackupExport.BeginAsync");

    /// <summary>
    /// Sends <c>BackupInventory</c> for <see cref="Closure"/> and returns the subset the destination's hint claims to
    /// hold (the raw hint, including false positives; see <see cref="TestBackupNode.SetFalseInventoryClaimsAsync"/>).
    /// </summary>
    public Task<IReadOnlySet<ObjectId>> RequestInventoryAsync() => throw Stub.NotYet("ManualBackupExport.RequestInventoryAsync");

    /// <summary>
    /// Sends the complete (encrypted) snapshot manifest, optionally tampered in transit as described by
    /// <paramref name="tamper"/> (framing stays valid; only content verification can detect the change). Returns
    /// <see cref="DfsErrorCode.None"/> once the destination durably staged it, otherwise its error code.
    /// </summary>
    public Task<DfsErrorCode> SendManifestAsync(BackupManifestTamper tamper = BackupManifestTamper.None) =>
        throw Stub.NotYet("ManualBackupExport.SendManifestAsync");

    /// <summary>
    /// Reads object <paramref name="id"/> from a verified live replica and sends it with <c>BackupPutObject</c>,
    /// optionally tampered in transit as described by <paramref name="tamper"/>. Returns <see cref="DfsErrorCode.None"/>
    /// only after the destination reports the object durably persisted and verified; otherwise its error code
    /// (<see cref="DfsErrorCode.Integrity"/> for detected corruption). Sending an object that is not part of
    /// <see cref="Closure"/> throws <see cref="ArgumentException"/>.
    /// </summary>
    public Task<DfsErrorCode> SendObjectAsync(ObjectId id, BackupObjectTamper tamper = BackupObjectTamper.None) =>
        throw Stub.NotYet("ManualBackupExport.SendObjectAsync");

    /// <summary>
    /// Sends <c>BackupPublish</c>. The destination verifies the full dependency closure itself (never trusting
    /// inventory hints) and either publishes atomically (durable pointer replacement, then acknowledgement) or refuses
    /// and lists the closure objects it lacks.
    /// </summary>
    public Task<BackupPublishAttempt> PublishAsync() => throw Stub.NotYet("ManualBackupExport.PublishAsync");

    /// <summary>Abandons the transfer if it was not published, releases the transfer pin and resumes the automatic exporter.</summary>
    public ValueTask DisposeAsync() => throw Stub.NotYet("ManualBackupExport.DisposeAsync");
}

/// <summary>In-transit tampering applied by <see cref="ManualBackupExport.SendObjectAsync"/>.</summary>
public enum BackupObjectTamper
{
    /// <summary>Genuine object.</summary>
    None,

    /// <summary>One payload byte is flipped after the sender computed its checksum.</summary>
    FlipPayloadByte,

    /// <summary>The genuine bytes/checksum of another closure object are sent labelled with this object's id.</summary>
    WrongObjectId,

    /// <summary>The encoding/key-version descriptor is altered (payload unchanged).</summary>
    WrongEncoding,

    /// <summary>The payload is truncated by one byte while the declared length is kept.</summary>
    TruncatedPayload,

    /// <summary>The declared checksum is replaced with the checksum of different bytes.</summary>
    WrongChecksum,
}

/// <summary>In-transit tampering applied by <see cref="ManualBackupExport.SendManifestAsync"/>.</summary>
public enum BackupManifestTamper
{
    /// <summary>Genuine manifest.</summary>
    None,

    /// <summary>One extent is re-pointed to an object id that exists nowhere (dangling reference).</summary>
    DanglingObjectReference,

    /// <summary>One extent's recorded object checksum is altered.</summary>
    AlteredExtentChecksum,

    /// <summary>One ciphertext byte of the encrypted manifest is flipped.</summary>
    FlipCiphertextByte,

    /// <summary>The manifest is truncated (one file record dropped) while its declared counts are kept.</summary>
    DroppedFileRecord,
}

/// <summary>Result of <see cref="ManualBackupExport.PublishAsync"/>.</summary>
/// <param name="Error"><see cref="DfsErrorCode.None"/> when published; otherwise the refusal code.</param>
/// <param name="Published">True when the destination atomically published the snapshot and acknowledged it.</param>
/// <param name="MissingObjects">Closure objects the destination found missing or invalid (empty when published).</param>
/// <param name="Checkpoint">The acknowledgement when published.</param>
public sealed record BackupPublishAttempt(DfsErrorCode Error, bool Published, IReadOnlyList<ObjectId> MissingObjects, DfsBackupCheckpoint? Checkpoint);
