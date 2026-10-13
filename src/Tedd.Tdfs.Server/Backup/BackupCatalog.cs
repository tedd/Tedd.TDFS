using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// The durable catalog of a backup node (ARCHITECTURE §12): per source volume name the current authority and lineage, the
/// retained published snapshots with their manifests and retention, and the staged transfer. Layout under the data directory:
/// <list type="bullet">
/// <item><description><c>catalog/v-{key}/volume</c> — name, authority, current lineage, retired authorities (atomic file);</description></item>
/// <item><description><c>catalog/v-{key}/p-{lineage}</c> — the published pointer of one lineage: the ordered list of its published
/// snapshots (atomic file; replacing it is the publication point);</description></item>
/// <item><description><c>catalog/v-{key}/s-{snapshot}.manifest</c> / <c>.retention</c> — the manifest (encrypted with the node's data
/// key) and the enforced retention of a snapshot;</description></item>
/// <item><description><c>staging/v-{key}/transfer</c> / <c>manifest</c> — the staged transfer's description and its verified manifest.</description></item>
/// </list>
/// Every catalog file is sealed with the node's data key and bound to its role and location (names of volumes, snapshots and
/// adapters are never stored in plaintext, FSD §14): a node started without its key material does not load its catalog
/// until a recovery key unlocks it (<see cref="IsLoaded"/>). A snapshot exists for clients only when a pointer lists it; files
/// no pointer references are left-overs of an interrupted publication and are removed when the catalog loads. Objects live
/// in the node's object store; the in-memory inventory hint (<see cref="Holds"/>) is never trusted for completeness —
/// publication verifies the closure against the store itself.
/// </summary>
internal sealed class BackupCatalog
{
    private const string CatalogDirectory = "catalog";
    private const string StagingDirectory = "staging";
    private const string VolumeFile = "volume";
    private const string TransferFile = "transfer";
    private const string StagedManifestFile = "manifest";
    private const uint VolumeMagic = 0x56424454;    // "TDBV"
    private const uint PointerMagic = 0x50424454;   // "TDBP"
    private const uint RetentionMagic = 0x52424454; // "TDBR"
    private const uint TransferMagic = 0x54424454;  // "TDBT"
    private const ushort FormatVersion = 2;

    private static readonly WireLimits CatalogLimits = new(MaxStringBytes: 1 << 20, MaxBytesLength: 64 << 20, MaxCollectionCount: 1 << 24);

    private readonly BackupServices _s;
    private readonly SortedDictionary<string, BackupVolume> _volumes = new(StringComparer.Ordinal);
    private readonly HashSet<ObjectId> _hint = [];
    private readonly KeyedAsyncLock<int> _storeGate = new();

    public BackupCatalog(BackupServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>Per-volume serialization of catalog changes.</summary>
    public KeyedAsyncLock<string> Locks { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Node-wide gate between object deletion and publication: garbage collection (and the replacement of a mislabelled stored
    /// copy) deletes stored objects only while holding it, and a publication holds it from its closure verification until its
    /// snapshot is listed by the published pointer (from then on the snapshot references the objects). An object verified for
    /// a publication can therefore never be deleted before the publication point. Acquired after a volume lock, never the
    /// other way round.
    /// </summary>
    public Task<IDisposable> AcquireStoreGateAsync(CancellationToken ct) => _storeGate.AcquireAsync(0, ct);

    /// <summary>Monotonic change counter (reported as the configuration index of the backup node's cluster map).</summary>
    public ulong Version { get; private set; } = 1;

    /// <summary>
    /// True once the catalog was loaded. A node without its key material cannot decrypt its catalog: until a recovery key unlocks
    /// it the catalog is empty and requests that need it fail (<see cref="RequireLoaded"/>).
    /// </summary>
    public bool IsLoaded { get; private set; }

    /// <summary>
    /// Requires the loaded catalog: <see cref="DfsRecoveryKeyMissingException"/> while the node has no key material, a retryable
    /// <see cref="DfsUnavailableException"/> while a recovery key's unlock is still loading it.
    /// </summary>
    public void RequireLoaded(string what)
    {
        _s.Keys.RequireUnlocked(what);
        if (!IsLoaded)
            throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"The catalog of backup node {_s.Name} is still being loaded ({what}).");
    }

    /// <summary>Every volume the node knows (ordinal name order).</summary>
    public IReadOnlyCollection<BackupVolume> Volumes => _volumes.Values;

    /// <summary>The volume named <paramref name="name"/>, or null.</summary>
    public BackupVolume? Find(string name) => _volumes.GetValueOrDefault(name);

    /// <summary>The volume named <paramref name="name"/>, created in memory when unknown (persisted with its first authority).</summary>
    public BackupVolume GetOrAdd(string name)
    {
        if (_volumes.TryGetValue(name, out BackupVolume? volume))
            return volume;
        volume = new BackupVolume(name, KeyOf(name));
        _volumes[name] = volume;
        return volume;
    }

    /// <summary>Records a catalog change (map consumers refresh).</summary>
    public void Touch() => Version++;

    // ------------------------------------------------------------------------------------------------- inventory hint

    /// <summary>Inventory hint: whether the node believes it holds a verified copy (never used for completeness).</summary>
    public bool Holds(ObjectId id) => _hint.Contains(id);

    /// <summary>Records a durably stored object in the inventory hint.</summary>
    public void NoteStored(ObjectId id) => _hint.Add(id);

    /// <summary>Removes an object from the inventory hint (its stored copy was deleted).</summary>
    public void ForgetStored(ObjectId id) => _hint.Remove(id);

    /// <summary>Objects the node believes it holds.</summary>
    public int HintCount => _hint.Count;

    // ------------------------------------------------------------------------------------------------- load

    /// <summary>
    /// Loads the catalog from the medium: volumes, pointers, retention, staged transfers, then the manifests. Needs the data key
    /// (the catalog is sealed with it): a locked node loads nothing (start-up without key material); the unlock by a recovery key
    /// loads it then.
    /// </summary>
    public async Task LoadAsync(CancellationToken ct)
    {
        if (IsLoaded || !_s.Keys.IsUnlocked)
            return;
        _volumes.Clear();
        IStorageMedium medium = _s.Medium;
        string catalog = _s.DataPath(CatalogDirectory);
        if (await medium.ExistsAsync(catalog, ct))
        {
            foreach (string dir in await medium.ListAsync(catalog, ct))
            {
                if (!dir.StartsWith("v-", StringComparison.Ordinal))
                    continue;
                await LoadVolumeAsync(StoragePath.Combine(catalog, dir), dir[2..], ct);
            }
        }

        string staging = _s.DataPath(StagingDirectory);
        if (await medium.ExistsAsync(staging, ct))
        {
            foreach (string dir in await medium.ListAsync(staging, ct))
            {
                if (dir.StartsWith("v-", StringComparison.Ordinal))
                    await LoadStagedAsync(StoragePath.Combine(staging, dir), dir[2..], ct);
            }
        }

        if (_s.Store is { } store)
        {
            foreach (ObjectRecordInfo info in store.List())
                _hint.Add(info.Id);
        }

        IsLoaded = true;
        Touch();
        await LoadManifestsAsync(ct);
    }

    /// <summary>
    /// Decrypts and loads every retained and staged manifest (start-up when unlocked, or after a recovery key unlocked the node
    /// while it serves). Each volume is loaded under its lock, so deletions and publications of that volume wait; until every
    /// manifest is loaded garbage collection does not run (<see cref="CollectGarbageAsync"/>).
    /// </summary>
    public async Task LoadManifestsAsync(CancellationToken ct)
    {
        foreach (BackupVolume volume in _volumes.Values.ToArray())
        {
            using IDisposable held = await Locks.AcquireAsync(volume.Name, ct);
            foreach (BackupSnapshot snapshot in volume.Snapshots.ToArray())
                snapshot.Manifest ??= await ReadManifestAsync(volume, snapshot.Key, ct);
            if (volume.Staged is { Manifest: null } staged)
            {
                byte[]? sealedBytes = await AtomicFile.ReadAsync(_s.Medium, StagingPath(volume, StagedManifestFile), ct);
                if (sealedBytes is not null)
                    AdoptStagedManifest(staged, BackupManifest.Decode(_s.Keys.OpenAtRest(sealedBytes, StagedContext(volume))));
            }
        }

        if (_s.Store is { } store)
        {
            foreach (ObjectRecordInfo info in store.List())
                _hint.Add(info.Id);
        }
    }

    private async Task LoadVolumeAsync(string directory, string key, CancellationToken ct)
    {
        IStorageMedium medium = _s.Medium;
        byte[]? raw = await AtomicFile.ReadAsync(medium, StoragePath.Combine(directory, VolumeFile), ct);
        string? name = null;
        BackupAuthority? authority = null;
        Guid lineage = Guid.Empty;
        var retired = new List<BackupAuthority>();
        if (raw is not null)
        {
            var r = new WireReader(Open(raw, VolumeContext(key), directory), CatalogLimits);
            Expect(ref r, VolumeMagic, directory);
            name = r.ReadString();
            if (r.ReadBool())
                authority = new BackupAuthority(r.ReadVolumeId(), r.ReadVarUInt());
            lineage = r.ReadGuid();
            int count = r.ReadCount(17);
            for (int i = 0; i < count; i++)
                retired.Add(new BackupAuthority(r.ReadVolumeId(), r.ReadVarUInt()));
            r.EnsureEnd();
        }

        var entries = new List<(PointerEntry Entry, Guid Lineage)>();
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        IReadOnlyList<string> files = await medium.ListAsync(directory, ct);
        foreach (string file in files)
        {
            if (!file.StartsWith("p-", StringComparison.Ordinal) || AtomicFile.IsTempName(file)
                || !Guid.TryParseExact(file.AsSpan(2), "N", out Guid fileLineage))
            {
                continue;
            }

            byte[]? pointer = await AtomicFile.ReadAsync(medium, StoragePath.Combine(directory, file), ct);
            if (pointer is null)
                continue;
            (string pointerName, Guid pointerLineage, List<PointerEntry> list) =
                ReadPointer(Open(pointer, PointerContext(key, fileLineage), directory), directory);
            if (pointerLineage != fileLineage)
                throw new DfsIntegrityException($"Backup catalog pointer '{file}' in '{directory}' names lineage {pointerLineage}.");
            name ??= pointerName;
            foreach (PointerEntry entry in list)
            {
                entries.Add((entry, pointerLineage));
                referenced.Add(entry.Key);
            }
        }

        if (name is null)
            return; // an empty directory left by an interrupted first write
        var volume = new BackupVolume(name, key) { Authority = authority, CurrentLineage = lineage, Persisted = raw is not null };
        volume.Retired.AddRange(retired);
        foreach ((PointerEntry entry, Guid entryLineage) in entries.OrderBy(e => e.Entry.Sequence))
        {
            var snapshot = new BackupSnapshot
            {
                Key = entry.Key, Id = entry.Id, Name = entry.Name, Covered = entry.Covered, Lineage = entryLineage, Sequence = entry.Sequence,
                CreatedUtc = entry.CreatedUtc, Label = entry.Label, Adapters = entry.Adapters,
            };
            byte[]? retention = await AtomicFile.ReadAsync(medium, SnapshotPath(volume, entry.Key, "retention"), ct);
            if (retention is not null)
                snapshot.Retention = ReadRetention(Open(retention, RetentionContext(key, entry.Key), directory), directory);
            volume.Snapshots.Add(snapshot);
            volume.NextSequence = Math.Max(volume.NextSequence, entry.Sequence + 1);
        }

        // Snapshot files no pointer references belong to a publication interrupted before its pointer was replaced.
        bool removed = false;
        foreach (string file in files)
        {
            if (!file.StartsWith("s-", StringComparison.Ordinal))
                continue;
            int dot = file.IndexOf('.', StringComparison.Ordinal);
            string snapKey = dot < 0 ? file[2..] : file[2..dot];
            if (!referenced.Contains(snapKey))
            {
                await medium.DeleteAsync(StoragePath.Combine(directory, file), ct);
                removed = true;
            }
        }

        if (removed)
            await medium.FlushDirectoryAsync(directory, ct);
        _volumes[name] = volume;
    }

    private async Task LoadStagedAsync(string directory, string key, CancellationToken ct)
    {
        byte[]? raw = await AtomicFile.ReadAsync(_s.Medium, StoragePath.Combine(directory, TransferFile), ct);
        if (raw is null)
            return;
        StagedTransfer staged = ReadTransfer(Open(raw, TransferContext(key), directory), directory);
        BackupVolume volume = GetOrAdd(staged.VolumeName);
        if (!string.Equals(volume.Key, key, StringComparison.Ordinal))
            throw new DfsIntegrityException($"The staged transfer in '{directory}' belongs to another volume.");
        if (volume.FindById(staged.Key.Snapshot) is { } published && published.Lineage == staged.Key.LineageId)
        {
            // Published before the crash; only the clean-up of the staging files was lost.
            await DeleteStagingFilesAsync(volume, ct);
            return;
        }

        volume.Staged = staged;
    }

    // ------------------------------------------------------------------------------------------------- persistence

    /// <summary>Durably records the volume's authority, lineage and retired authorities.</summary>
    public async Task SaveVolumeAsync(BackupVolume volume, CancellationToken ct)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        var w = new WireWriter(buffer);
        w.WriteUInt32(VolumeMagic);
        w.WriteUInt16(FormatVersion);
        w.WriteString(volume.Name);
        w.WriteBool(volume.Authority is not null);
        if (volume.Authority is { } a)
        {
            w.WriteVolumeId(a.Volume);
            w.WriteVarUInt(a.Epoch);
        }

        w.WriteGuid(volume.CurrentLineage);
        w.WriteCount(volume.Retired.Count);
        foreach (BackupAuthority r in volume.Retired)
        {
            w.WriteVolumeId(r.Volume);
            w.WriteVarUInt(r.Epoch);
        }

        await AtomicFile.WriteAsync(_s.Medium, VolumePath(volume, VolumeFile), Seal(buffer.WrittenSpan, VolumeContext(volume.Key)), ct);
        volume.Persisted = true;
        Touch();
    }

    /// <summary>Durably writes a snapshot's manifest and retention (before its publication point).</summary>
    public async Task WriteSnapshotFilesAsync(BackupVolume volume, string snapKey, BackupManifest manifest, DfsRetention retention, CancellationToken ct)
    {
        byte[] sealedManifest = _s.Keys.SealAtRest(manifest.Encode(), ManifestContext(volume, snapKey));
        await AtomicFile.WriteAsync(_s.Medium, SnapshotPath(volume, snapKey, "manifest"), sealedManifest, ct);
        await AtomicFile.WriteAsync(_s.Medium, SnapshotPath(volume, snapKey, "retention"),
            Seal(EncodeRetention(retention), RetentionContext(volume.Key, snapKey)), ct);
    }

    /// <summary>
    /// The publication point: atomically replaces the lineage's pointer with one that lists the new snapshot, then makes it
    /// visible in memory.
    /// </summary>
    public async Task<BackupSnapshot> PublishPointerAsync(BackupVolume volume, string snapKey, BackupManifest manifest, DfsRetention retention, CancellationToken ct)
    {
        var snapshot = new BackupSnapshot
        {
            Key = snapKey, Id = manifest.SnapshotId, Name = manifest.SnapshotName, Covered = manifest.Boundary, Lineage = manifest.Lineage,
            Sequence = volume.NextSequence, CreatedUtc = manifest.CreatedUtc, Label = manifest.Label, Adapters = manifest.Adapters,
            Retention = retention, Manifest = manifest,
        };
        List<BackupSnapshot> lineage = [.. volume.Snapshots.Where(s => !s.Condemned && s.Lineage == manifest.Lineage), snapshot];
        await WritePointerAsync(volume, manifest.Lineage, lineage, ct);
        volume.NextSequence++;
        volume.Snapshots.Add(snapshot);
        Touch();
        return snapshot;
    }

    /// <summary>Durably changes a snapshot's enforced retention.</summary>
    public async Task SetRetentionAsync(BackupVolume volume, BackupSnapshot snapshot, DfsRetention retention, CancellationToken ct)
    {
        await AtomicFile.WriteAsync(_s.Medium, SnapshotPath(volume, snapshot.Key, "retention"),
            Seal(EncodeRetention(retention), RetentionContext(volume.Key, snapshot.Key)), ct);
        snapshot.Retention = retention;
        Touch();
    }

    /// <summary>
    /// Removes a snapshot from the published set (deletion or pruning): its lineage pointer is rewritten without it; its files
    /// and objects go once no reader pins it any more.
    /// </summary>
    public async Task CondemnAsync(BackupVolume volume, BackupSnapshot snapshot, CancellationToken ct)
    {
        if (snapshot.Condemned)
            return;
        List<BackupSnapshot> lineage = [.. volume.Snapshots.Where(s => !s.Condemned && s.Lineage == snapshot.Lineage && !ReferenceEquals(s, snapshot))];
        await WritePointerAsync(volume, snapshot.Lineage, lineage, ct);
        snapshot.Condemned = true;
        Touch();
        await ReleaseCondemnedAsync(volume, ct);
    }

    /// <summary>Deletes the files of condemned snapshots no reader holds any more.</summary>
    public async Task ReleaseCondemnedAsync(BackupVolume volume, CancellationToken ct)
    {
        foreach (BackupSnapshot snapshot in volume.Snapshots.Where(s => s.Condemned && s.ReaderPins == 0).ToArray())
        {
            await _s.Medium.DeleteAsync(SnapshotPath(volume, snapshot.Key, "manifest"), ct);
            await _s.Medium.DeleteAsync(SnapshotPath(volume, snapshot.Key, "retention"), ct);
            await _s.Medium.FlushDirectoryAsync(VolumeDirectory(volume), ct);
            volume.Snapshots.Remove(snapshot);
            _s.Info("backup", BackupServices.Inv($"Removed snapshot '{snapshot.Name}' of volume '{volume.Name}' (lineage {snapshot.Lineage})."));
        }
    }

    private async Task WritePointerAsync(BackupVolume volume, Guid lineage, IReadOnlyList<BackupSnapshot> snapshots, CancellationToken ct)
    {
        string path = VolumePath(volume, "p-" + lineage.ToString("N"));
        if (snapshots.Count == 0)
        {
            await AtomicFile.DeleteAsync(_s.Medium, path, ct);
            return;
        }

        var buffer = new ArrayBufferWriter<byte>(512);
        var w = new WireWriter(buffer);
        w.WriteUInt32(PointerMagic);
        w.WriteUInt16(FormatVersion);
        w.WriteString(volume.Name);
        w.WriteGuid(lineage);
        w.WriteCount(snapshots.Count);
        foreach (BackupSnapshot s in snapshots)
        {
            w.WriteString(s.Key);
            w.WriteVarUInt(s.Sequence);
            w.WriteSnapshotId(s.Id);
            w.WriteString(s.Name);
            w.WriteCommitToken(s.Covered);
            w.WriteDateTimeOffset(s.CreatedUtc);
            w.WriteDefinedEnum(s.Label);
            w.WriteCount(s.Adapters.Count);
            foreach (AdapterRecordData a in s.Adapters)
                w.WriteRecord(a);
        }

        await AtomicFile.WriteAsync(_s.Medium, path, Seal(buffer.WrittenSpan, PointerContext(volume.Key, lineage)), ct);
    }

    /// <summary>Durably records a staged transfer's description (resumable across restarts).</summary>
    public async Task SaveTransferAsync(BackupVolume volume, StagedTransfer staged, CancellationToken ct)
    {
        var buffer = new ArrayBufferWriter<byte>(512);
        var w = new WireWriter(buffer);
        w.WriteUInt32(TransferMagic);
        w.WriteUInt16(FormatVersion);
        w.WriteRecord(staged.Key);
        w.WriteString(staged.VolumeName);
        w.WriteString(staged.ClusterName);
        w.WriteString(staged.SnapshotName);
        w.WriteCommitToken(staged.Boundary);
        w.WriteDateTimeOffset(staged.CreatedUtc);
        w.WriteDefinedEnum(staged.Label);
        w.WriteCount(staged.Adapters.Count);
        foreach (AdapterRecordData a in staged.Adapters)
            w.WriteRecord(a);
        w.WriteRetention(staged.RequestedRetention);
        w.WriteString(staged.Exporter);
        await AtomicFile.WriteAsync(_s.Medium, StagingPath(volume, TransferFile), Seal(buffer.WrittenSpan, TransferContext(volume.Key)), ct);
    }

    /// <summary>Durably stages a verified manifest of the staged transfer (sealed with the node's data key).</summary>
    public async Task SaveStagedManifestAsync(BackupVolume volume, BackupManifest manifest, CancellationToken ct)
    {
        byte[] sealedBytes = _s.Keys.SealAtRest(manifest.Encode(), StagedContext(volume));
        await AtomicFile.WriteAsync(_s.Medium, StagingPath(volume, StagedManifestFile), sealedBytes, ct);
    }

    /// <summary>Discards the staged transfer of <paramref name="volume"/> (its received objects become garbage unless referenced).</summary>
    public async Task DiscardStagedAsync(BackupVolume volume, CancellationToken ct)
    {
        volume.Staged = null;
        await DeleteStagingFilesAsync(volume, ct);
    }

    private async Task DeleteStagingFilesAsync(BackupVolume volume, CancellationToken ct)
    {
        await AtomicFile.DeleteAsync(_s.Medium, StagingPath(volume, StagedManifestFile), ct);
        await AtomicFile.DeleteAsync(_s.Medium, StagingPath(volume, TransferFile), ct);
    }

    /// <summary>Sets the staged transfer's verified manifest and recomputes which closure objects the store already holds.</summary>
    public void AdoptStagedManifest(StagedTransfer staged, BackupManifest manifest)
    {
        staged.Manifest = manifest;
        if (_s.Store is not { } store)
            return;
        foreach (BackupObjectRef o in manifest.Closure)
        {
            if (store.TryGetInfo(o.Id) is { } info && info.Length == o.Length && info.Checksum == o.Checksum)
                staged.Durable.Add(o.Id);
        }
    }

    // ------------------------------------------------------------------------------------------------- retention and GC

    /// <summary>
    /// Destination retention (FSD §12.4): per volume the newest <see cref="BackupNodeOptions.KeepLastSnapshots"/> snapshots
    /// are kept; older ones are pruned as a unit once their retention expired, they are not held and no reader pins them.
    /// </summary>
    public async Task EnforceRetentionAsync(CancellationToken ct)
    {
        if (!IsLoaded)
            return;
        DateTimeOffset now = _s.Time.GetUtcNow();
        int keep = Math.Max(1, _s.Options.KeepLastSnapshots);
        bool pruned = false;
        foreach (BackupVolume volume in _volumes.Values.ToArray())
        {
            using IDisposable held = await Locks.AcquireAsync(volume.Name, ct);
            List<BackupSnapshot> visible = volume.Ranked();
            for (int i = 0; i < visible.Count - keep; i++)
            {
                BackupSnapshot snapshot = visible[i];
                if (snapshot.IsProtected(now) || snapshot.ReaderPins > 0)
                    continue;
                await CondemnAsync(volume, snapshot, ct);
                pruned = true;
                _s.Info("backup", BackupServices.Inv($"Pruned snapshot '{snapshot.Name}' of volume '{volume.Name}' (outside the newest {keep}, retention expired)."));
            }

            await ReleaseCondemnedAsync(volume, ct);
        }

        if (pruned)
            await CollectGarbageAsync(ct);
    }

    /// <summary>
    /// Deletes stored objects no retained snapshot and no staged manifest references. Skipped while any transfer is staged
    /// (objects it received before its manifest arrived are referenced by nothing yet but must survive for the resume), and
    /// stopped as soon as one is staged meanwhile (its exporter may skip objects the inventory hint still lists). Skipped while
    /// any retained snapshot's manifest is not decrypted (after a recovery key unlocked the node, manifests load one by one):
    /// a snapshot without its manifest would look as if it referenced nothing. Runs under the store gate, so no publication
    /// verifies its closure against an object this collection is about to delete.
    /// </summary>
    public async Task CollectGarbageAsync(CancellationToken ct)
    {
        if (_s.Store is not { } store || !_s.Keys.IsUnlocked || AnyStaged() || AnyManifestMissing())
            return;
        using IDisposable gate = await AcquireStoreGateAsync(ct);
        if (AnyStaged() || AnyManifestMissing())
            return;
        HashSet<ObjectId> referenced = ReferencedObjects();
        long deleted = 0;
        foreach (ObjectRecordInfo info in store.List())
        {
            if (referenced.Contains(info.Id))
                continue;
            if (AnyStaged())
                break;

            // Forgotten by the hint first: from now on the inventory reports the object missing.
            _hint.Remove(info.Id);
            await store.DeleteAsync(info.Id, ct);
            deleted++;
        }

        if (deleted > 0)
            _s.Debug("backup", BackupServices.Inv($"Garbage collection removed {deleted} unreferenced object(s)."));
    }

    private bool AnyStaged() => _volumes.Values.Any(v => v.Staged is not null);

    private bool AnyManifestMissing() => _volumes.Values.Any(v => v.Snapshots.Any(s => s.Manifest is null));

    /// <summary>Objects referenced by retained snapshots (condemned ones still pinned included) and staged manifests.</summary>
    public HashSet<ObjectId> ReferencedObjects()
    {
        var result = new HashSet<ObjectId>();
        foreach (BackupVolume volume in _volumes.Values)
        {
            foreach (BackupSnapshot snapshot in volume.Snapshots)
                result.UnionWith(snapshot.Closure);
            if (volume.Staged?.Manifest is { } manifest)
                result.UnionWith(manifest.ClosureIds);
        }

        return result;
    }

    /// <summary>Per stored object, the number of retained snapshots (published or staged manifest) whose closure references it.</summary>
    public IReadOnlyDictionary<ObjectId, int> ReferenceCounts()
    {
        var counts = new Dictionary<ObjectId, int>();
        if (_s.Store is { } store)
        {
            foreach (ObjectRecordInfo info in store.List())
                counts[info.Id] = 0;
        }

        void Count(IEnumerable<ObjectId> closure)
        {
            foreach (ObjectId id in closure.Distinct())
                counts[id] = counts.GetValueOrDefault(id) + 1;
        }

        foreach (BackupVolume volume in _volumes.Values)
        {
            foreach (BackupSnapshot snapshot in volume.Snapshots)
                Count(snapshot.Closure);
            if (volume.Staged?.Manifest is { } manifest)
                Count(manifest.ClosureIds);
        }

        return counts;
    }

    /// <summary>Bytes received by staged transfers that no published snapshot references yet.</summary>
    public long StagedBytes()
    {
        if (_s.Store is not { } store)
            return 0;
        var published = new HashSet<ObjectId>();
        foreach (BackupVolume volume in _volumes.Values)
        {
            foreach (BackupSnapshot snapshot in volume.Snapshots)
                published.UnionWith(snapshot.Closure);
        }

        long bytes = 0;
        foreach (BackupVolume volume in _volumes.Values)
        {
            if (volume.Staged is not { } staged)
                continue;
            foreach (ObjectId id in staged.Received)
            {
                if (!published.Contains(id) && store.TryGetInfo(id) is { } info)
                    bytes += info.Length;
            }
        }

        return bytes;
    }

    /// <summary>Manifest of a retained snapshot read from its file.</summary>
    public async Task<BackupManifest> ReadManifestAsync(BackupVolume volume, string snapKey, CancellationToken ct)
    {
        byte[] sealedBytes = await AtomicFile.ReadAsync(_s.Medium, SnapshotPath(volume, snapKey, "manifest"), ct)
            ?? throw new DfsIntegrityException($"The manifest of snapshot {snapKey} of volume '{volume.Name}' is missing.");
        return BackupManifest.Decode(_s.Keys.OpenAtRest(sealedBytes, ManifestContext(volume, snapKey)));
    }

    // ------------------------------------------------------------------------------------------------- encodings and paths

    /// <summary>The file key of a snapshot.</summary>
    public static string SnapshotKey(SnapshotId id) => id.Value.ToString("N");

    /// <summary>
    /// The file key of a new publication of <paramref name="id"/> in <paramref name="volume"/>: the snapshot id, or — while a
    /// retained snapshot of the volume already uses that key (a deleted publication of the same snapshot that a reader still
    /// pins) — the id qualified by the publication sequence. Every retained snapshot owns its files alone, so removing a
    /// condemned predecessor never deletes the manifest or retention of a later publication of the same snapshot.
    /// </summary>
    public static string NewSnapshotKey(BackupVolume volume, SnapshotId id)
    {
        ArgumentNullException.ThrowIfNull(volume);
        string key = SnapshotKey(id);
        for (ulong suffix = volume.NextSequence; volume.Snapshots.Any(s => string.Equals(s.Key, key, StringComparison.Ordinal)); suffix++)
            key = SnapshotKey(id) + "-" + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return key;
    }

    private static string KeyOf(string name) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)).AsSpan(0, 16));

    private string VolumeDirectory(BackupVolume volume) => _s.DataPath(StoragePath.Combine(CatalogDirectory, "v-" + volume.Key));

    private string VolumePath(BackupVolume volume, string file) => StoragePath.Combine(VolumeDirectory(volume), file);

    private string SnapshotPath(BackupVolume volume, string snapKey, string extension) => VolumePath(volume, "s-" + snapKey + "." + extension);

    private string StagingPath(BackupVolume volume, string file) => _s.DataPath(StoragePath.Combine(StagingDirectory, StoragePath.Combine("v-" + volume.Key, file)));

    private static string ManifestContext(BackupVolume volume, string snapKey) => "manifest/" + volume.Key + "/" + snapKey;

    private static string StagedContext(BackupVolume volume) => "staged-manifest/" + volume.Key;

    private static string VolumeContext(string key) => "catalog-volume/" + key;

    private static string PointerContext(string key, Guid lineage) => "catalog-pointer/" + key + "/" + lineage.ToString("N");

    private static string RetentionContext(string key, string snapKey) => "catalog-retention/" + key + "/" + snapKey;

    private static string TransferContext(string key) => "catalog-transfer/" + key;

    /// <summary>Seals a catalog record with the node's data key, bound to <paramref name="context"/>.</summary>
    private byte[] Seal(ReadOnlySpan<byte> plain, string context) => _s.Keys.SealAtRest(plain, context);

    /// <summary>Opens a sealed catalog record; a foreign, moved or altered record fails with <see cref="DfsIntegrityException"/>.</summary>
    private byte[] Open(byte[] sealedBytes, string context, string where)
    {
        try
        {
            return _s.Keys.OpenAtRest(sealedBytes, context);
        }
        catch (DfsIntegrityException ex)
        {
            throw new DfsIntegrityException($"Backup catalog record in '{where}' cannot be authenticated: {ex.Message}", ex);
        }
    }

    private static void Expect(ref WireReader r, uint magic, string where)
    {
        if (r.ReadUInt32() != magic)
            throw new DfsIntegrityException($"Backup catalog file in '{where}' has an invalid magic number.");
        ushort version = r.ReadUInt16();
        if (version != FormatVersion)
            throw new DfsProtocolException(DfsErrorCode.Unsupported, $"Backup catalog file in '{where}' uses format {version}; this build reads {FormatVersion}.");
    }

    private static (string Name, Guid Lineage, List<PointerEntry> Entries) ReadPointer(byte[] raw, string where)
    {
        var r = new WireReader(raw, CatalogLimits);
        Expect(ref r, PointerMagic, where);
        string name = r.ReadString();
        Guid lineage = r.ReadGuid();
        int count = r.ReadCount(40);
        var entries = new List<PointerEntry>(count);
        for (int i = 0; i < count; i++)
        {
            string key = r.ReadString();
            ulong sequence = r.ReadVarUInt();
            SnapshotId id = r.ReadSnapshotId();
            string snapshotName = r.ReadString();
            CommitToken covered = r.ReadCommitToken();
            DateTimeOffset created = r.ReadDateTimeOffset();
            DfsConsistencyLabel label = r.ReadEnum<DfsConsistencyLabel>();
            int adapterCount = r.ReadCount(AdapterRecordData.MinEncodedSize);
            var adapters = new AdapterRecordData[adapterCount];
            for (int a = 0; a < adapterCount; a++)
                adapters[a] = r.ReadRecord<AdapterRecordData>();
            entries.Add(new PointerEntry(key, sequence, id, snapshotName, covered, created, label, adapters));
        }

        r.EnsureEnd();
        return (name, lineage, entries);
    }

    private static byte[] EncodeRetention(DfsRetention retention)
    {
        var buffer = new ArrayBufferWriter<byte>(32);
        var w = new WireWriter(buffer);
        w.WriteUInt32(RetentionMagic);
        w.WriteUInt16(FormatVersion);
        w.WriteRetention(retention);
        return buffer.WrittenSpan.ToArray();
    }

    private static DfsRetention ReadRetention(byte[] raw, string where)
    {
        var r = new WireReader(raw, CatalogLimits);
        Expect(ref r, RetentionMagic, where);
        DfsRetention retention = r.ReadRetention();
        r.EnsureEnd();
        return retention;
    }

    private static StagedTransfer ReadTransfer(byte[] raw, string where)
    {
        var r = new WireReader(raw, CatalogLimits);
        Expect(ref r, TransferMagic, where);
        BackupTransferKey key = r.ReadRecord<BackupTransferKey>();
        string volume = r.ReadString();
        string cluster = r.ReadString();
        string snapshot = r.ReadString();
        CommitToken boundary = r.ReadCommitToken();
        DateTimeOffset created = r.ReadDateTimeOffset();
        DfsConsistencyLabel label = r.ReadEnum<DfsConsistencyLabel>();
        int count = r.ReadCount(AdapterRecordData.MinEncodedSize);
        var adapters = new AdapterRecordData[count];
        for (int i = 0; i < count; i++)
            adapters[i] = r.ReadRecord<AdapterRecordData>();
        DfsRetention retention = r.ReadRetention();
        string exporter = r.ReadString();
        r.EnsureEnd();
        return new StagedTransfer
        {
            Key = key, VolumeName = volume, ClusterName = cluster, SnapshotName = snapshot, Boundary = boundary, CreatedUtc = created, Label = label,
            Adapters = adapters, RequestedRetention = retention, Exporter = exporter,
        };
    }

    private sealed record PointerEntry(string Key, ulong Sequence, SnapshotId Id, string Name, CommitToken Covered, DateTimeOffset CreatedUtc,
        DfsConsistencyLabel Label, IReadOnlyList<AdapterRecordData> Adapters);
}
