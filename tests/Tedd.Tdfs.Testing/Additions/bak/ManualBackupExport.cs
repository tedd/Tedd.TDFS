using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Backup;
using Tedd.Tdfs.Server.Volumes.State;

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
    private readonly TestCluster _cluster;
    private readonly NodeHost _host;
    private readonly ManualBackupTransfer _transfer;
    private readonly IRandomSource _random;
    private bool _disposed;

    internal ManualBackupExport(TestCluster cluster, NodeHost host, ManualBackupTransfer transfer, IRandomSource random)
    {
        _cluster = cluster;
        _host = host;
        _transfer = transfer;
        _random = random;
    }

    /// <summary>Source volume name.</summary>
    public string Volume => _transfer.Volume;

    /// <summary>Exported snapshot name.</summary>
    public string SnapshotName => _transfer.SnapshotName;

    /// <summary>Exported snapshot id.</summary>
    public SnapshotId SnapshotId => _transfer.SnapshotId;

    /// <summary>Commit boundary of the exported snapshot.</summary>
    public CommitToken Boundary => _transfer.Boundary;

    /// <summary>Target (registration) name of the destination backup node.</summary>
    public string TargetName => _transfer.Target;

    /// <summary>Lineage id this transfer runs under.</summary>
    public Guid LineageId => _transfer.Lineage;

    /// <summary>
    /// The snapshot's complete object dependency closure computed from the pinned source snapshot root (every object
    /// referenced by any extent of any file; sorted, distinct; inline files contribute nothing).
    /// </summary>
    public IReadOnlyList<ObjectId> Closure => _transfer.Closure;

    /// <summary>
    /// Sends <c>BackupBegin</c> (lineage, volume id/epoch, snapshot id, boundary). Returns <see cref="DfsErrorCode.None"/>
    /// when accepted, otherwise the destination's error code (e.g. <see cref="DfsErrorCode.EpochMismatch"/>). May be
    /// called again after a destination restart to resume the same transfer.
    /// </summary>
    public async Task<DfsErrorCode> BeginAsync()
    {
        DfsErrorCode code = await RunAsync(() => _transfer.BeginTransferAsync());
        _cluster.Record($"manual export {Volume}/{SnapshotName} -> {TargetName}: begin {code}");
        return code;
    }

    /// <summary>
    /// Sends <c>BackupInventory</c> for <see cref="Closure"/> and returns the subset the destination's hint claims to
    /// hold (the raw hint, including false positives; see <see cref="TestBackupNode.SetFalseInventoryClaimsAsync"/>).
    /// </summary>
    public Task<IReadOnlySet<ObjectId>> RequestInventoryAsync() => RunAsync(() => _transfer.RequestInventoryAsync());

    /// <summary>
    /// Sends the complete (encrypted) snapshot manifest, optionally tampered in transit as described by
    /// <paramref name="tamper"/> (framing stays valid; only content verification can detect the change). Returns
    /// <see cref="DfsErrorCode.None"/> once the destination durably staged it, otherwise its error code.
    /// </summary>
    public async Task<DfsErrorCode> SendManifestAsync(BackupManifestTamper tamper = BackupManifestTamper.None)
    {
        if (!Enum.IsDefined(tamper))
            throw new ArgumentOutOfRangeException(nameof(tamper), tamper, "Unknown manifest tamper.");
        ObjectId dangling = ObjectId.New(_random);
        Func<BackupManifest, BackupManifest>? plain = tamper switch
        {
            BackupManifestTamper.DanglingObjectReference => m => AlterFirstExtent(m, e => e with { Object = dangling }),
            BackupManifestTamper.AlteredExtentChecksum => m => AlterFirstExtent(m, e => e with { Checksum = e.Checksum ^ UInt128.One }),
            BackupManifestTamper.DroppedFileRecord => DropFileRecord,
            _ => null,
        };
        Func<byte[], byte[]>? sealedBytes = tamper == BackupManifestTamper.FlipCiphertextByte ? FlipCiphertextByte : null;
        DfsErrorCode code = await RunAsync(() => _transfer.SendManifestAsync(plain, sealedBytes));
        _cluster.Record($"manual export {Volume}/{SnapshotName} -> {TargetName}: manifest ({tamper}) {code}");
        return code;
    }

    /// <summary>
    /// Reads object <paramref name="id"/> from a verified live replica and sends it with <c>BackupPutObject</c>,
    /// optionally tampered in transit as described by <paramref name="tamper"/>. Returns <see cref="DfsErrorCode.None"/>
    /// only after the destination reports the object durably persisted and verified; otherwise its error code
    /// (<see cref="DfsErrorCode.Integrity"/> for detected corruption). Sending an object that is not part of
    /// <see cref="Closure"/> throws <see cref="ArgumentException"/>.
    /// </summary>
    public async Task<DfsErrorCode> SendObjectAsync(ObjectId id, BackupObjectTamper tamper = BackupObjectTamper.None)
    {
        if (!Closure.Contains(id))
            throw new ArgumentException($"Object {id} is not part of the closure of snapshot '{SnapshotName}'.", nameof(id));
        if (!Enum.IsDefined(tamper))
            throw new ArgumentOutOfRangeException(nameof(tamper), tamper, "Unknown object tamper.");
        ObjectId other = Closure.FirstOrDefault(o => o != id);
        Func<ManualBackupObject, Task<ManualBackupObject>>? alter = tamper switch
        {
            BackupObjectTamper.FlipPayloadByte => o => Task.FromResult(o with { Data = FlipMiddleByte(o.Data) }),
            BackupObjectTamper.WrongObjectId => async o =>
            {
                if (other.IsEmpty)
                    throw new InvalidOperationException("The closure has no other object to substitute.");
                ManualBackupObject substitute = await _transfer.ReadObjectAsync(other);
                return substitute with { Object = o.Object };
            },
            BackupObjectTamper.WrongEncoding => o => Task.FromResult(o with { Encoding = unchecked((byte)(o.Encoding + 1)) }),
            BackupObjectTamper.TruncatedPayload => o => Task.FromResult(o with { Data = o.Data[..Math.Max(0, o.Data.Length - 1)] }),
            BackupObjectTamper.WrongChecksum => o => Task.FromResult(o with { Checksum = ObjectChecksum.Compute([.. o.Data.Span, 0x42]) }),
            _ => null,
        };
        DfsErrorCode code = await RunAsync(() => _transfer.SendObjectAsync(id, alter));
        _cluster.Record($"manual export {Volume}/{SnapshotName} -> {TargetName}: object {id} ({tamper}) {code}");
        return code;
    }

    /// <summary>
    /// Sends <c>BackupPublish</c>. The destination verifies the full dependency closure itself (never trusting
    /// inventory hints) and either publishes atomically (durable pointer replacement, then acknowledgement) or refuses
    /// and lists the closure objects it lacks.
    /// </summary>
    public async Task<BackupPublishAttempt> PublishAsync()
    {
        ManualPublishOutcome outcome = await RunAsync(() => _transfer.PublishAsync());
        _cluster.Record($"manual export {Volume}/{SnapshotName} -> {TargetName}: publish {(outcome.Published ? "published" : outcome.Error.ToString())}");
        return new BackupPublishAttempt(outcome.Error, outcome.Published, outcome.Missing, outcome.Checkpoint);
    }

    /// <summary>Abandons the transfer if it was not published, releases the transfer pin and resumes the automatic exporter.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        if (!_host.IsRunning)
            return;
        await RunAsync(async () =>
        {
            await _transfer.DisposeAsync();
            return true;
        });
        _cluster.Record($"manual export {Volume}/{SnapshotName} -> {TargetName}: disposed");
    }

    private Task<T> RunAsync<T>(Func<Task<T>> work) => _host.InvokeAsync(_ => work());

    private static ReadOnlyMemory<byte> FlipMiddleByte(ReadOnlyMemory<byte> data)
    {
        byte[] copy = data.ToArray();
        if (copy.Length > 0)
            copy[copy.Length / 2] ^= 0x5A;
        return copy;
    }

    private static BackupManifest AlterFirstExtent(BackupManifest manifest, Func<ExtentRef, ExtentRef> alter)
    {
        var inodes = manifest.Inodes.ToList();
        for (int i = 0; i < inodes.Count; i++)
        {
            InodeRecord inode = inodes[i];
            foreach (KeyValuePair<string, StreamRecord> pair in inode.Streams)
            {
                if (pair.Value.IsInline || pair.Value.Extents.IsEmpty)
                    continue;
                StreamRecord stream = pair.Value with { Extents = pair.Value.Extents.SetItem(0, alter(pair.Value.Extents[0])) };
                inodes[i] = inode with { Streams = inode.Streams.SetItem(pair.Key, stream) };
                return manifest with { Inodes = inodes };
            }
        }

        throw new InvalidOperationException("The manifest has no extent to alter.");
    }

    private static BackupManifest DropFileRecord(BackupManifest manifest)
    {
        var inodes = manifest.Inodes.ToList();
        int index = inodes.FindIndex(i => !i.IsDirectory && i.Id != InodeRecord.RootId);
        if (index < 0)
            throw new InvalidOperationException("The manifest has no file record to drop.");
        inodes.RemoveAt(index);
        return manifest with { Inodes = inodes };
    }

    private static byte[] FlipCiphertextByte(byte[] sealedBytes)
    {
        byte[] copy = sealedBytes.ToArray();
        copy[copy.Length / 2] ^= 0x01;
        return copy;
    }
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
