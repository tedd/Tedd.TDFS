using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// Receiving side of the late-sync export protocol (ARCHITECTURE §12, PROTOCOL.md §8.6): <c>BackupBegin</c> stages a resumable
/// transfer under the source authority and lineage it accepts, <c>BackupInventory</c> answers a hint only, <c>BackupPutObject</c>
/// verifies and durably stores objects, <c>BackupPutManifest</c> receives, authenticates, validates and stages the manifest, and
/// <c>BackupPublish</c> verifies the complete dependency closure against the store itself, persists the snapshot, atomically
/// replaces the lineage's published pointer and only then acknowledges. Exports of a superseded authority are rejected
/// (BAK-026). <c>BackupRetention</c> queries and extends destination retention under the node's own authorization.
/// </summary>
internal sealed class BackupReceiver
{
    /// <summary>Largest manifest accepted.</summary>
    public const long MaxManifestBytes = 1L << 30;

    private readonly BackupServices _s;

    public BackupReceiver(BackupServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>The volume id under which the node's object store keeps every object (objects are deduplicated by content).</summary>
    public static VolumeId StoreVolume { get; } = new(new Guid("6b6b0bf2-5d8e-4a83-9b1e-7d1a3c0e2a51"));

    private BackupCatalog Catalog => _s.Catalog;

    // ------------------------------------------------------------------------------------------------- BackupBegin

    /// <summary><c>BackupBegin</c>: accepts (or resumes) the transfer of one snapshot.</summary>
    public async Task<IWireMessage> BeginAsync(string principal, BackupBeginMessage m, CancellationToken ct)
    {
        _s.Principals.RequireExport(principal);
        CheckKey(m.Key);
        if (string.IsNullOrEmpty(m.VolumeName) || string.IsNullOrEmpty(m.SnapshotName))
            throw new ArgumentException("A transfer names its volume and snapshot.");
        if (m.Boundary.IsNone || m.Boundary.ClusterId != m.Key.SourceClusterId || m.Boundary.VolumeId != m.Key.Volume || m.Boundary.VolumeEpoch != m.Key.Epoch)
            throw new ArgumentException("The snapshot boundary does not belong to the transfer's volume and epoch.");
        _s.Keys.RequireUnlocked("receiving a transfer");
        BackupVolume volume = Catalog.GetOrAdd(m.VolumeName);
        using IDisposable held = await Catalog.Locks.AcquireAsync(volume.Name, ct);
        var authority = new BackupAuthority(m.Key.Volume, m.Key.Epoch);
        if (volume.IsRetired(authority))
            throw new DfsEpochMismatchException($"Volume '{volume.Name}' {authority.Volume} epoch {authority.Epoch} was superseded by a newer authority; its exports are rejected.");

        bool adopt = false;
        bool reseed = false;
        if (volume.Authority is not { } current)
        {
            if (!m.FullReseed && m.BaseSnapshot is not null)
                reseed = true;
            else
                adopt = true;
        }
        else if (current != authority)
        {
            if (m.FullReseed)
            {
                volume.Retired.Add(current);
                adopt = true;
                _s.Info("backup", BackupServices.Inv($"Volume '{volume.Name}': authority {authority.Volume} epoch {authority.Epoch} supersedes {current.Volume} epoch {current.Epoch}."));
            }
            else
            {
                reseed = true;
            }
        }
        else if (volume.CurrentLineage != m.Key.LineageId)
        {
            if (m.FullReseed)
                adopt = true;
            else
                reseed = true;
        }

        if (reseed)
        {
            return new BackupBeginResultMessage(false, false, true, volume.CurrentLineage, volume.Pointer?.Id, Catalog.HintCount, 0);
        }

        if (adopt)
        {
            volume.Authority = authority;
            volume.CurrentLineage = m.Key.LineageId;
            await Catalog.SaveVolumeAsync(volume, ct);
        }

        if (volume.FindById(m.Key.Snapshot) is { Condemned: false } published && published.Authority == authority)
            return new BackupBeginResultMessage(true, true, false, volume.CurrentLineage, published.Id, Catalog.HintCount, 0);

        if (volume.Staged is { } staged && staged.Key == m.Key)
        {
            staged.Exporter = principal;
            return new BackupBeginResultMessage(true, false, false, volume.CurrentLineage, volume.Pointer?.Id, Catalog.HintCount, staged.ReceivedBytes);
        }

        if (volume.Staged is not null)
            await Catalog.DiscardStagedAsync(volume, ct);
        var transfer = new StagedTransfer
        {
            Key = m.Key, VolumeName = volume.Name, ClusterName = m.SourceClusterName, SnapshotName = m.SnapshotName, Boundary = m.Boundary,
            CreatedUtc = m.CreatedUtc, Label = m.Label, Adapters = m.Adapters, RequestedRetention = m.Retention, Exporter = principal,
        };
        await Catalog.SaveTransferAsync(volume, transfer, ct);
        volume.Staged = transfer;
        Catalog.Touch();
        _s.Debug("backup", BackupServices.Inv($"Volume '{volume.Name}': staged transfer of snapshot '{m.SnapshotName}' (lineage {m.Key.LineageId})."));
        return new BackupBeginResultMessage(true, false, false, volume.CurrentLineage, volume.Pointer?.Id, Catalog.HintCount, 0);
    }

    // ------------------------------------------------------------------------------------------------- BackupInventory

    /// <summary><c>BackupInventory</c>: which listed objects the node does not believe it holds (a hint, never trusted for completeness).</summary>
    public IWireMessage Inventory(string principal, BackupInventoryMessage m)
    {
        _s.Principals.RequireExport(principal);
        CheckKey(m.Key);
        RequireCurrent(m.Key);
        var missing = m.Objects.Where(id => !Catalog.Holds(id)).Distinct().ToArray();
        return new BackupInventoryResultMessage(missing);
    }

    // ------------------------------------------------------------------------------------------------- BackupPutObject

    /// <summary><c>BackupPutObject</c>: verifies one object and stores it durably before reporting it durable.</summary>
    public async Task<IWireMessage> PutObjectAsync(string principal, BackupPutObjectMessage m, CancellationToken ct)
    {
        _s.Principals.RequireExport(principal);
        CheckKey(m.Key);
        ObjectStore store = _s.RequireStore("receiving objects");
        (BackupVolume volume, StagedTransfer? staged) = RequireCurrent(m.Key);
        if (staged is null && volume.FindById(m.Key.Snapshot) is null)
            throw new ArgumentException("No transfer is staged for this transfer key; send BackupBegin first.");
        if (m.Object.IsEmpty)
            throw new ArgumentException("The object id must not be empty.");
        if (m.Encoding != ObjectEncodings.Plain)
            throw new DfsIntegrityException(m.Object, $"Object {m.Object} uses unsupported encoding {m.Encoding}.");
        if (m.Length != m.Data.Length)
            throw new DfsIntegrityException(m.Object, $"Object {m.Object} declares {m.Length} bytes but {m.Data.Length} arrived.");
        if (ObjectChecksum.Compute(m.Data.Span) != m.Checksum)
            throw new DfsIntegrityException(m.Object, $"Object {m.Object} does not match its checksum.");
        if (staged?.Manifest is { } manifest)
        {
            int index = IndexOf(manifest.Closure, m.Object);
            if (index < 0)
                throw new DfsIntegrityException(m.Object, $"Object {m.Object} is not part of the snapshot's dependency closure.");
            BackupObjectRef expected = manifest.Closure[index];
            if (expected.Length != m.Length || expected.Checksum != m.Checksum || expected.Encoding != m.Encoding)
                throw new DfsIntegrityException(m.Object, $"Object {m.Object} does not match the manifest's description of it.");
        }

        if (staged is not null && staged.Received.Add(m.Object))
            staged.ReceivedBytes += m.Data.Length;

        // F18: the object's bytes arrived; nothing is durable yet.
        await _s.HitAsync(FaultStage.F18, FaultSites.BackupObjectReceived, volume.Name, DfsOperationKind.BackupTransfer, m.Object, ct: ct);
        ObjectRecordInfo? existing = store.TryGetInfo(m.Object);
        bool alreadyPresent = existing is not null && existing.Length == m.Length && existing.Checksum == m.Checksum;
        if (existing is not null && !alreadyPresent)
        {
            // Another payload is stored under this id (an earlier delivery was mislabelled). It is replaced only when the staged,
            // authenticated manifest vouches for the arriving bytes and no retained snapshot describes the stored ones.
            if (staged?.Manifest is null || DescribedByRetained(m.Object, existing))
                throw new DfsIntegrityException(m.Object, $"Object {m.Object} is already stored with different content.");
            await store.DeleteAsync(m.Object, ct);
            Catalog.ForgetStored(m.Object);
            _s.Warn("backup", BackupServices.Inv($"Volume '{volume.Name}': replaced a stored copy of object {m.Object} that did not match the snapshot's manifest."));
        }

        try
        {
            await store.PutAsync(m.Object, StoreVolume, default, m.Data, DfsOperationKind.BackupTransfer, ct);
        }
        catch (IOException ex) when (StorageErrors.IsDiskFull(ex) && ex is not DfsException)
        {
            throw new DfsCapacityException($"Backup node {_s.Name} is full.", ex);
        }

        staged?.Durable.Add(m.Object);
        Catalog.NoteStored(m.Object);
        return new BackupPutObjectResultMessage(true, alreadyPresent, staged?.ReceivedBytes ?? 0);
    }

    // ------------------------------------------------------------------------------------------------- BackupPutManifest

    /// <summary><c>BackupPutManifest</c>: one chunk of the sealed manifest; the complete manifest is authenticated, validated and staged.</summary>
    public async Task<IWireMessage> PutManifestAsync(string principal, BackupPutManifestMessage m, CancellationToken ct)
    {
        _s.Principals.RequireExport(principal);
        CheckKey(m.Key);
        _s.Keys.RequireUnlocked("receiving a manifest");
        if (m.Offset < 0 || m.TotalLength <= 0 || m.TotalLength > MaxManifestBytes || m.Offset > m.TotalLength || m.Data.Length > m.TotalLength - m.Offset)
            throw new ArgumentException("The manifest chunk is out of range.");
        (BackupVolume volume, _) = RequireCurrent(m.Key);

        // Serialized with BackupBegin of the same volume: a manifest sent together with its begin is staged right after it.
        using IDisposable held = await Catalog.Locks.AcquireAsync(volume.Name, ct);
        (volume, StagedTransfer? staged) = RequireCurrent(m.Key);
        if (staged is null)
        {
            if (volume.FindById(m.Key.Snapshot) is { Condemned: false })
                return new BackupPutManifestResultMessage(m.TotalLength, true);
            throw new ArgumentException("No transfer is staged for this transfer key; send BackupBegin first.");
        }
        if (staged.Manifest is not null && staged.ManifestIdentity == (m.ManifestChecksum, m.TotalLength))
            return new BackupPutManifestResultMessage(m.TotalLength, true);

        if (staged.ManifestBuffer is null || staged.ManifestChecksum != m.ManifestChecksum || staged.ManifestTotal != m.TotalLength)
        {
            if (m.Offset != 0)
                return new BackupPutManifestResultMessage(0, false);
            staged.ManifestBuffer = new byte[m.TotalLength];
            staged.ManifestReceived = 0;
            staged.ManifestTotal = m.TotalLength;
            staged.ManifestChecksum = m.ManifestChecksum;
        }

        if (m.Offset != staged.ManifestReceived)
            return new BackupPutManifestResultMessage(staged.ManifestReceived, false);
        m.Data.Span.CopyTo(staged.ManifestBuffer.AsSpan((int)m.Offset));
        staged.ManifestReceived += m.Data.Length;
        if (staged.ManifestReceived < staged.ManifestTotal)
            return new BackupPutManifestResultMessage(staged.ManifestReceived, false);

        byte[] sealedBytes = staged.ManifestBuffer;
        staged.ManifestBuffer = null;
        staged.ManifestReceived = 0;
        if (BackupManifest.Checksum(sealedBytes) != m.ManifestChecksum)
            throw new DfsIntegrityException("The received manifest does not match its checksum.");
        string secret = _s.Principals.SecretOf(principal)
            ?? throw new UnauthorizedAccessException($"Principal '{principal}' has no export key on this backup host.");
        byte[] plaintext = BackupManifest.Open(sealedBytes, BackupManifest.TransitKey(secret, m.Key), m.Key);
        BackupManifest manifest;
        try
        {
            manifest = BackupManifest.Decode(plaintext);
        }
        catch (DfsProtocolException ex) when (ex.ErrorCode != DfsErrorCode.Unsupported)
        {
            throw new DfsIntegrityException("The received manifest is malformed: " + ex.Message, ex);
        }

        if (manifest.GetValidationError() is { } error)
            throw new DfsIntegrityException("The received manifest is inconsistent: " + error + ".");
        if (manifest.SourceClusterId != m.Key.SourceClusterId || manifest.VolumeId != m.Key.Volume || manifest.Epoch != m.Key.Epoch
            || manifest.Lineage != m.Key.LineageId || manifest.SnapshotId != m.Key.Snapshot || manifest.Boundary != staged.Boundary
            || !string.Equals(manifest.VolumeName, staged.VolumeName, StringComparison.Ordinal)
            || !string.Equals(manifest.SnapshotName, staged.SnapshotName, StringComparison.Ordinal))
        {
            throw new DfsIntegrityException("The received manifest describes another snapshot than the staged transfer.");
        }

        await Catalog.SaveStagedManifestAsync(volume, manifest, ct);
        staged.Verified = false;
        Catalog.AdoptStagedManifest(staged, manifest);
        staged.ManifestIdentity = (m.ManifestChecksum, m.TotalLength);
        return new BackupPutManifestResultMessage(m.TotalLength, true);
    }

    // ------------------------------------------------------------------------------------------------- BackupPublish

    /// <summary>
    /// <c>BackupPublish</c>: verifies the complete dependency closure against the store (never an inventory hint), persists the
    /// snapshot, atomically replaces the lineage pointer and acknowledges; otherwise lists what is missing and publishes nothing.
    /// </summary>
    public async Task<IWireMessage> PublishAsync(string principal, BackupPublishMessage m, CancellationToken ct)
    {
        _s.Principals.RequireExport(principal);
        CheckKey(m.Key);
        _s.Keys.RequireUnlocked("publishing a snapshot");
        (BackupVolume volume, _) = RequireCurrent(m.Key);
        using IDisposable held = await Catalog.Locks.AcquireAsync(volume.Name, ct);
        if (volume.FindById(m.Key.Snapshot) is { Condemned: false } done && done.Authority == new BackupAuthority(m.Key.Volume, m.Key.Epoch))
            return Published(done);
        if (volume.Staged is not { } staged || staged.Key != m.Key)
            return new BackupPublishResultMessage(false, [], true, CommitToken.None, null, false);
        if (staged.Manifest is not { } manifest || staged.ManifestIdentity != (m.ManifestChecksum, m.ManifestLength))
            return new BackupPublishResultMessage(false, [], true, CommitToken.None, null, false);

        // F18: the destination is about to verify the dependency closure itself.
        await _s.HitAsync(FaultStage.F18, FaultSites.BackupBeforeClosureVerify, volume.Name, DfsOperationKind.BackupPublish, ct: ct);
        List<ObjectId> missing = _s.Mutations.PublishBackupBeforeClosure ? [] : VerifyClosure(manifest);
        if (missing.Count > 0)
        {
            foreach (ObjectId id in missing)
                staged.Durable.Remove(id);
            return new BackupPublishResultMessage(false, missing, false, CommitToken.None, null, false);
        }

        staged.Verified = true;
        DfsRetention retention = Combine(m.RequestedRetention, _s.Options.DefaultRetention);
        string snapKey = BackupCatalog.SnapshotKey(manifest.SnapshotId);
        BackupSnapshot snapshot;
        try
        {
            await Catalog.WriteSnapshotFilesAsync(volume, snapKey, manifest, retention, ct);

            // F19: everything is durable; the published pointer still names the previous snapshot.
            await _s.HitAsync(FaultStage.F19, FaultSites.BackupBeforePointer, volume.Name, DfsOperationKind.BackupPublish, ct: ct);
            snapshot = await Catalog.PublishPointerAsync(volume, snapKey, manifest, retention, ct);
        }
        catch (IOException ex) when (StorageErrors.IsDiskFull(ex) && ex is not DfsException)
        {
            throw new DfsCapacityException($"Backup node {_s.Name} is full.", ex);
        }

        // F19: the pointer replacement is durable.
        await _s.HitAsync(FaultStage.F19, FaultSites.BackupAfterPointer, volume.Name, DfsOperationKind.BackupPublish, ct: ct);
        await Catalog.DiscardStagedAsync(volume, ct);
        _s.Info("backup", BackupServices.Inv($"Volume '{volume.Name}': published snapshot '{manifest.SnapshotName}' covering {manifest.Boundary} (lineage {manifest.Lineage})."));

        // F19: published and durable; the acknowledgement is about to be sent.
        await _s.HitAsync(FaultStage.F19, FaultSites.BackupAck, volume.Name, DfsOperationKind.BackupPublish, ct: ct);
        return Published(snapshot);
    }

    /// <summary>The closure objects the store does not hold with exactly the manifest's length and checksum.</summary>
    public List<ObjectId> VerifyClosure(BackupManifest manifest)
    {
        ObjectStore store = _s.RequireStore("verifying a snapshot");
        var missing = new List<ObjectId>();
        foreach (BackupObjectRef o in manifest.Closure)
        {
            if (o.Encoding != ObjectEncodings.Plain || store.TryGetInfo(o.Id) is not { } info || info.Length != o.Length || info.Checksum != o.Checksum)
                missing.Add(o.Id);
        }

        return missing;
    }

    /// <summary>True when a retained snapshot's manifest describes exactly the stored copy of <paramref name="id"/>.</summary>
    private bool DescribedByRetained(ObjectId id, ObjectRecordInfo stored)
    {
        foreach (BackupVolume volume in Catalog.Volumes)
        {
            foreach (BackupSnapshot snapshot in volume.Snapshots)
            {
                if (snapshot.Manifest is not { } manifest)
                    return true;
                int index = IndexOf(manifest.Closure, id);
                if (index >= 0 && manifest.Closure[index].Length == stored.Length && manifest.Closure[index].Checksum == stored.Checksum)
                    return true;
            }
        }

        return false;
    }

    private static BackupPublishResultMessage Published(BackupSnapshot snapshot)
        => new(true, [], false, snapshot.Covered, snapshot.Retention.ExpiresUtc, snapshot.Retention.Hold);

    // ------------------------------------------------------------------------------------------------- BackupRetention

    /// <summary><c>BackupRetention</c>: retained snapshots (and a verified staged one), optionally extending retention.</summary>
    public async Task<IWireMessage> RetentionAsync(string principal, BackupRetentionMessage m, CancellationToken ct)
    {
        _s.Principals.RequireRead(principal, "query retention");
        if (m.SourceClusterId != _s.Options.SourceClusterId)
            throw new DfsClusterIdentityException($"This backup host serves cluster {_s.Options.SourceClusterId}, not {m.SourceClusterId}.");
        if (Catalog.Find(m.VolumeName) is not { } volume)
            return new BackupRetentionResultMessage([]);
        if (m.Extend is { } extend)
        {
            if (!_s.Principals.IsExporter(principal) && !_s.Principals.IsRetentionAdmin(principal))
                throw new UnauthorizedAccessException($"Principal '{principal}' may not change retention on this backup host.");
            using IDisposable held = await Catalog.Locks.AcquireAsync(volume.Name, ct);
            foreach (BackupSnapshot snapshot in volume.Visible.Where(s => m.SnapshotName is null || string.Equals(s.Name, m.SnapshotName, StringComparison.Ordinal)).ToArray())
                await Catalog.SetRetentionAsync(volume, snapshot, Extend(snapshot.Retention, extend), ct);
        }

        var result = new List<BackupRetainedSnapshotData>();
        foreach (BackupSnapshot snapshot in volume.Visible)
        {
            if (m.SnapshotName is null || string.Equals(snapshot.Name, m.SnapshotName, StringComparison.Ordinal))
                result.Add(new BackupRetainedSnapshotData(snapshot.Id, snapshot.Name, snapshot.Covered, snapshot.Retention, true));
        }

        if (volume.Staged is { Verified: true } staged && (m.SnapshotName is null || string.Equals(staged.SnapshotName, m.SnapshotName, StringComparison.Ordinal)))
            result.Add(new BackupRetainedSnapshotData(staged.Key.Snapshot, staged.SnapshotName, staged.Boundary, staged.RequestedRetention, false));
        return new BackupRetentionResultMessage(result);
    }

    // ------------------------------------------------------------------------------------------------- helpers

    /// <summary>The retention of a newly published snapshot: the source's request combined with the node's default.</summary>
    public static DfsRetention Combine(DfsRetention requested, DfsRetention @default)
    {
        DateTimeOffset? expires = (requested.ExpiresUtc, @default.ExpiresUtc) switch
        {
            ({ } a, { } b) => a > b ? a : b,
            ({ } a, null) => a,
            (null, { } b) => b,
            _ => null,
        };
        return new DfsRetention(expires, requested.Hold || @default.Hold);
    }

    /// <summary>Extends <paramref name="current"/> by <paramref name="extend"/>; never shortens or releases (BAK-022).</summary>
    public static DfsRetention Extend(DfsRetention current, DfsRetention extend)
    {
        DateTimeOffset? expires = current.ExpiresUtc;
        if (extend.ExpiresUtc is { } e && (expires is null || e > expires))
            expires = e;
        return new DfsRetention(expires, current.Hold || extend.Hold);
    }

    /// <summary>True when <paramref name="next"/> neither shortens <paramref name="current"/> nor releases its hold.</summary>
    public static bool IsExtension(DfsRetention current, DfsRetention next)
    {
        if (current.Hold && !next.Hold)
            return false;
        if (current.ExpiresUtc is { } old && (next.ExpiresUtc is not { } n || n < old))
            return false;
        return true;
    }

    private void CheckKey(BackupTransferKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (key.SourceClusterId != _s.Options.SourceClusterId)
            throw new DfsClusterIdentityException($"This backup host accepts exports of cluster {_s.Options.SourceClusterId}, not {key.SourceClusterId}.");
        if (key.Volume.Value == Guid.Empty || key.Epoch == 0 || key.LineageId == Guid.Empty || key.Snapshot.Value == Guid.Empty)
            throw new ArgumentException("The transfer key is incomplete.");
    }

    /// <summary>The volume a transfer key belongs to (current authority) and its staged transfer when the key matches.</summary>
    private (BackupVolume Volume, StagedTransfer? Staged) RequireCurrent(BackupTransferKey key)
    {
        var authority = new BackupAuthority(key.Volume, key.Epoch);
        foreach (BackupVolume volume in Catalog.Volumes)
        {
            if (volume.IsRetired(authority))
                throw new DfsEpochMismatchException($"Volume '{volume.Name}' {key.Volume} epoch {key.Epoch} was superseded; its exports are rejected.");
            if (volume.Authority == authority)
                return (volume, volume.Staged is { } staged && staged.Key == key ? staged : null);
        }

        throw new ArgumentException("No transfer of this volume was begun on this backup host.");
    }

    private static int IndexOf(IReadOnlyList<BackupObjectRef> closure, ObjectId id)
    {
        int lo = 0;
        int hi = closure.Count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int c = closure[mid].Id.Value.CompareTo(id.Value);
            if (c == 0)
                return mid;
            if (c < 0)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return -1;
    }
}
