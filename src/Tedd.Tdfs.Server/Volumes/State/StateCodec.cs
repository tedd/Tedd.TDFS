using System.Buffers;
using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Server.Volumes.State;

/// <summary>
/// Canonical serialization of the volume state (checkpoints) and of its records (also used inside commands). Each distinct
/// inode version <c>(InodeId, Revision)</c> is stored once; the current root, snapshot roots, pinned enumeration roots and
/// version-pinned handles refer to versions by id and revision (ARCHITECTURE §5.7). Replicas with the same applied history
/// produce identical bytes; reading restores the sharing structure (one object per version).
/// </summary>
internal static class StateCodec
{
    /// <summary>Checkpoint format version.</summary>
    public const byte CheckpointFormat = 1;

    private static readonly WireLimits CheckpointLimits = new(MaxStringBytes: 1 << 20, MaxBytesLength: int.MaxValue, MaxCollectionCount: 1 << 28);

    // ------------------------------------------------------------------------------------------------- records

    public static void WriteExtent(ref WireWriter w, ExtentRef e)
    {
        w.WriteVarInt(e.Offset);
        w.WriteVarInt(e.Length);
        w.WriteObjectId(e.Object);
        w.WriteVarInt(e.ObjectOffset);
        w.WriteVarInt(e.ObjectLength);
        w.WriteUInt128(e.Checksum);
        w.WriteByte(e.Encoding);
        w.WriteVarUInt(e.PlacementEpoch);
        w.WriteVarUInt(e.Staging.Value);
        w.WriteCount(e.Locations.IsDefault ? 0 : e.Locations.Length);
        if (!e.Locations.IsDefault)
        {
            foreach (NodeId n in e.Locations)
                w.WriteNodeId(n);
        }
    }

    public static ExtentRef ReadExtent(ref WireReader r)
    {
        long offset = r.ReadVarInt();
        long length = r.ReadVarInt();
        ObjectId obj = r.ReadObjectId();
        long objectOffset = r.ReadVarInt();
        long objectLength = r.ReadVarInt();
        UInt128 checksum = r.ReadUInt128();
        byte encoding = r.ReadByte();
        ulong epoch = r.ReadVarUInt();
        var staging = new StagingId(r.ReadVarUInt());
        int count = r.ReadCount(16);
        var locations = ImmutableArray.CreateBuilder<NodeId>(count);
        for (int i = 0; i < count; i++)
            locations.Add(r.ReadNodeId());
        return new ExtentRef(offset, length, obj, objectOffset, objectLength, checksum, encoding, epoch, staging, locations.MoveToImmutable());
    }

    public static void WriteStream(ref WireWriter w, StreamRecord s)
    {
        w.WriteVarInt(s.Length);
        w.WriteBool(s.IsInline);
        w.WriteBytes(s.Inline.Span);
        w.WriteCount(s.Extents.Count);
        foreach (ExtentRef e in s.Extents)
            WriteExtent(ref w, e);
        w.WriteVarUInt(s.Revision);
        w.WriteVarUInt(s.CreatedIndex);
    }

    public static StreamRecord ReadStream(ref WireReader r)
    {
        long length = r.ReadVarInt();
        bool inline = r.ReadBool();
        ReadOnlyMemory<byte> bytes = r.ReadBytesCopy();
        int count = r.ReadCount(20);
        var extents = ImmutableList.CreateBuilder<ExtentRef>();
        for (int i = 0; i < count; i++)
            extents.Add(ReadExtent(ref r));
        return new StreamRecord(length, inline, bytes, extents.ToImmutable(), r.ReadVarUInt(), r.ReadVarUInt());
    }

    public static void WriteInode(ref WireWriter w, InodeRecord n)
    {
        w.WriteVarUInt(n.Id);
        w.WriteByte((byte)n.Kind);
        w.WriteVarUInt(n.Revision);
        w.WriteVarUInt(n.MetadataRevision);
        w.WriteVarUInt(n.CreatedIndex);
        w.WriteDateTimeOffset(n.CreatedUtc);
        w.WriteDateTimeOffset(n.ModifiedUtc);
        w.WriteSecurityDescriptor(n.Security);
        w.WriteString(n.PolicyName);
        w.WriteVarUInt(n.ParentId);
        w.WriteString(n.Name);
        w.WriteCount(n.Streams.Count);
        foreach (KeyValuePair<string, StreamRecord> s in n.Streams)
        {
            w.WriteString(s.Key);
            WriteStream(ref w, s.Value);
        }

        w.WriteCount(n.Children.Count);
        foreach (KeyValuePair<string, ulong> c in n.Children)
        {
            w.WriteString(c.Key);
            w.WriteVarUInt(c.Value);
        }
    }

    public static InodeRecord ReadInode(ref WireReader r)
    {
        ulong id = r.ReadVarUInt();
        var kind = (InodeKind)r.ReadByte();
        if (kind is not (InodeKind.File or InodeKind.Directory))
            throw WireReader.Malformed($"Unknown inode kind {(byte)kind}.");
        ulong revision = r.ReadVarUInt();
        ulong metadataRevision = r.ReadVarUInt();
        ulong created = r.ReadVarUInt();
        DateTimeOffset createdUtc = r.ReadDateTimeOffset();
        DateTimeOffset modifiedUtc = r.ReadDateTimeOffset();
        DfsSecurityDescriptor security = r.ReadSecurityDescriptor();
        string policy = r.ReadString();
        ulong parent = r.ReadVarUInt();
        string name = r.ReadString();
        int streamCount = r.ReadCount(10);
        var streams = ImmutableSortedDictionary.CreateBuilder<string, StreamRecord>(StringComparer.Ordinal);
        for (int i = 0; i < streamCount; i++)
        {
            string streamName = r.ReadString();
            streams[streamName] = ReadStream(ref r);
        }

        int childCount = r.ReadCount(2);
        var children = ImmutableSortedDictionary.CreateBuilder<string, ulong>(StringComparer.Ordinal);
        for (int i = 0; i < childCount; i++)
        {
            string childName = r.ReadString();
            children[childName] = r.ReadVarUInt();
        }

        return new InodeRecord(id, kind, revision, created, createdUtc, modifiedUtc, security, policy, parent, name, streams.ToImmutable(), children.ToImmutable())
        {
            MetadataRevision = metadataRevision,
        };
    }

    public static void WriteAdapter(ref WireWriter w, AdapterRecord a)
    {
        w.WriteString(a.Id);
        w.WriteString(a.Version);
        w.WriteStringMap(a.RestoreRequirements);
    }

    public static AdapterRecord ReadAdapter(ref WireReader r)
    {
        string id = r.ReadString();
        string version = r.ReadString();
        IReadOnlyDictionary<string, string> requirements = r.ReadStringMap();
        return new AdapterRecord(id, version, ImmutableSortedDictionary.CreateRange(StringComparer.Ordinal, requirements));
    }

    public static void WriteBackupProgress(ref WireWriter w, BackupProgressRecord p)
    {
        w.WriteString(p.Target);
        w.WriteOptionalString(p.LastSnapshot);
        w.WriteSnapshotId(p.LastSnapshotId);
        w.WriteVarUInt(p.CoveredIndex);
        w.WriteOptionalDateTimeOffset(p.RetentionExpiryUtc);
        w.WriteBool(p.Held);
        w.WriteBool(p.ReseedRequired);
        w.WriteGuid(p.Lineage);
        w.WriteDateTimeOffset(p.UpdatedUtc);
    }

    public static BackupProgressRecord ReadBackupProgress(ref WireReader r)
        => new(r.ReadString(), r.ReadOptionalString(), r.ReadSnapshotId(), r.ReadVarUInt(), r.ReadOptionalDateTimeOffset(), r.ReadBool(), r.ReadBool(),
            r.ReadGuid(), r.ReadDateTimeOffset());

    // ------------------------------------------------------------------------------------------------- checkpoint

    /// <summary>Writes the canonical checkpoint of <paramref name="state"/>.</summary>
    public static void WriteCheckpoint(IBufferWriter<byte> output, VolumeState state)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(state);
        var versions = new SortedDictionary<(ulong Id, ulong Revision), InodeRecord>();
        void AddRoot(ImmutableSortedDictionary<ulong, InodeRecord> root)
        {
            foreach (InodeRecord n in root.Values)
                versions.TryAdd((n.Id, n.Revision), n);
        }

        AddRoot(state.Inodes);
        foreach (SnapshotRecord s in state.Snapshots.Values)
            AddRoot(s.Root);
        foreach (PinRecord p in state.Pins.Values)
        {
            if (p.Root is not null)
                AddRoot(p.Root);
        }

        foreach (HandleRecord h in state.Handles.Values)
        {
            if (h.PinnedInode is { } pinned)
                versions.TryAdd((pinned.Id, pinned.Revision), pinned);
        }

        if (state.Restore is { } restore)
            AddRoot(restore.Staged);
        foreach (RestoreIntentRecord intent in state.Restores.Values)
            AddRoot(intent.Root);

        var w = new WireWriter(output);
        w.WriteByte(CheckpointFormat);
        WriteConfig(ref w, state.Config);
        w.WriteVarUInt(state.AppliedIndex);
        w.WriteDateTimeOffset(state.LeaderTime);

        w.WriteCount(versions.Count);
        foreach (InodeRecord n in versions.Values)
            WriteInode(ref w, n);
        WriteRoot(ref w, state.Inodes);
        WriteUInt64Set(ref w, state.Detached);

        w.WriteCount(state.WriterGenerations.Count);
        foreach (KeyValuePair<StreamKey, ulong> g in state.WriterGenerations)
        {
            w.WriteVarUInt(g.Key.Inode);
            w.WriteString(g.Key.Stream);
            w.WriteVarUInt(g.Value);
        }

        w.WriteVarUInt(state.NextInodeId);
        w.WriteVarUInt(state.NextHandleId);
        w.WriteVarUInt(state.NextStagingId);
        w.WriteVarUInt(state.NextSessionId);
        w.WriteVarUInt(state.NextPinId);

        w.WriteCount(state.Handles.Count);
        foreach (HandleRecord h in state.Handles.Values)
            WriteHandle(ref w, h);

        w.WriteCount(state.Sessions.Count);
        foreach (SessionRecord s in state.Sessions.Values)
            WriteSession(ref w, s);
        w.WriteVarUInt(state.RetiredSessionsBelow);

        w.WriteCount(state.Transactions.Count);
        foreach (TxRecord t in state.Transactions.Values)
            WriteTx(ref w, t);

        w.WriteCount(state.Staging.Count);
        foreach (StagingRecord s in state.Staging.Values)
        {
            w.WriteVarUInt(s.Id);
            w.WriteVarUInt(s.Session);
            w.WriteVarUInt(s.Handle);
            w.WriteOperationId(s.Transaction);
            w.WriteString(s.Principal);
            w.WriteByte((byte)s.State);
            w.WriteVarUInt(s.CreatedIndex);
            w.WriteVarUInt(s.ClosedIndex);
            w.WriteDateTimeOffset(s.CreatedUtc);
        }

        w.WriteCount(state.Snapshots.Count);
        foreach (SnapshotRecord s in state.Snapshots.Values)
        {
            w.WriteSnapshotId(s.Id);
            w.WriteString(s.Name);
            w.WriteVarUInt(s.BoundaryIndex);
            w.WriteByte((byte)s.Label);
            w.WriteCount(s.Adapters.Count);
            foreach (AdapterRecord a in s.Adapters)
                WriteAdapter(ref w, a);
            w.WriteDateTimeOffset(s.CreatedUtc);
            w.WriteOptionalDateTimeOffset(s.ExpiresUtc);
            w.WriteBool(s.Hold);
            WriteRoot(ref w, s.Root);
        }

        w.WriteCount(state.Pins.Count);
        foreach (PinRecord p in state.Pins.Values)
        {
            w.WriteVarUInt(p.Id);
            w.WriteByte((byte)p.Kind);
            w.WriteVarUInt(p.Session);
            w.WriteString(p.Principal);
            w.WriteVarUInt(p.CreatedIndex);
            w.WriteVarUInt(p.PinIndex);
            w.WriteVarUInt(p.Handle);
            w.WriteOptionalString(p.Snapshot);
            w.WriteVarUInt(p.Directory);
            w.WriteString(p.Binding);
            w.WriteBool(p.Root is not null);
            if (p.Root is not null)
                WriteRoot(ref w, p.Root);
            w.WriteOptionalString(p.Target);
        }

        w.WriteCount(state.DerefLog.Count);
        foreach (DerefEntry d in state.DerefLog)
        {
            w.WriteVarUInt(d.Index);
            w.WriteObjectId(d.Object);
        }

        w.WriteCount(state.BackupProgress.Count);
        foreach (BackupProgressRecord p in state.BackupProgress.Values)
            WriteBackupProgress(ref w, p);

        w.WriteCount(state.KeyVersions.Count);
        foreach (uint v in state.KeyVersions)
            w.WriteVarUInt(v);
        w.WriteOptionalStoragePolicy(state.MigrationTarget);

        w.WriteBool(state.Restore is not null);
        if (state.Restore is { } r)
        {
            w.WriteString(r.Source);
            w.WriteBool(r.Complete);
            WriteRoot(ref w, r.Staged);
            w.WriteVarUInt(r.Staging);
            w.WriteVarUInt(r.CompletedIndex);
        }

        w.WriteCount(state.Restores.Count);
        foreach (RestoreIntentRecord i in state.Restores.Values)
        {
            w.WriteOperationId(i.Operation);
            w.WriteUInt128(i.PayloadHash);
            w.WriteString(i.Principal);
            w.WriteString(i.Snapshot);
            w.WriteSnapshotId(i.SnapshotId);
            w.WriteVarUInt(i.BoundaryIndex);
            w.WriteString(i.NewVolumeName);
            w.WriteOptionalString(i.StoragePool);
            WriteRoot(ref w, i.Root);
            w.WriteVarUInt(i.CreatedIndex);
            w.WriteDateTimeOffset(i.CreatedUtc);
        }

        w.WriteNoExtensions();
    }

    /// <summary>Reads a checkpoint written by <see cref="WriteCheckpoint"/>.</summary>
    /// <exception cref="DfsIntegrityException">The data is malformed.</exception>
    public static VolumeState ReadCheckpoint(ReadOnlySpan<byte> data)
    {
        try
        {
            var r = new WireReader(data, CheckpointLimits);
            byte format = r.ReadByte();
            if (format != CheckpointFormat)
                throw new DfsIntegrityException($"Unsupported volume checkpoint format {format}.");
            VolumeConfig config = ReadConfig(ref r);
            ulong applied = r.ReadVarUInt();
            DateTimeOffset leaderTime = r.ReadDateTimeOffset();

            int versionCount = r.ReadCount(10);
            var versions = new Dictionary<(ulong, ulong), InodeRecord>(versionCount);
            for (int i = 0; i < versionCount; i++)
            {
                InodeRecord n = ReadInode(ref r);
                if (!versions.TryAdd((n.Id, n.Revision), n))
                    throw new DfsIntegrityException($"Duplicate inode version ({n.Id}, {n.Revision}) in the checkpoint.");
            }

            ImmutableSortedDictionary<ulong, InodeRecord> inodes = ReadRoot(ref r, versions);
            ImmutableSortedSet<ulong> detached = ReadUInt64Set(ref r);

            int genCount = r.ReadCount(3);
            var generations = ImmutableSortedDictionary.CreateBuilder<StreamKey, ulong>(StreamKeyComparer.Instance);
            for (int i = 0; i < genCount; i++)
                generations[new StreamKey(r.ReadVarUInt(), r.ReadString())] = r.ReadVarUInt();

            ulong nextInode = r.ReadVarUInt();
            ulong nextHandle = r.ReadVarUInt();
            ulong nextStaging = r.ReadVarUInt();
            ulong nextSession = r.ReadVarUInt();
            ulong nextPin = r.ReadVarUInt();

            int handleCount = r.ReadCount(10);
            var handles = ImmutableSortedDictionary.CreateBuilder<ulong, HandleRecord>();
            for (int i = 0; i < handleCount; i++)
            {
                HandleRecord h = ReadHandle(ref r, versions);
                handles[h.Id] = h;
            }

            int sessionCount = r.ReadCount(10);
            var sessions = ImmutableSortedDictionary.CreateBuilder<ulong, SessionRecord>();
            for (int i = 0; i < sessionCount; i++)
            {
                SessionRecord s = ReadSession(ref r);
                sessions[s.Id] = s;
            }

            ulong retiredBelow = r.ReadVarUInt();

            int txCount = r.ReadCount(10);
            var txs = ImmutableSortedDictionary.CreateBuilder<DfsOperationId, TxRecord>(OperationIdComparer.Instance);
            for (int i = 0; i < txCount; i++)
            {
                TxRecord t = ReadTx(ref r);
                txs[t.Id] = t;
            }

            int stagingCount = r.ReadCount(8);
            var staging = ImmutableSortedDictionary.CreateBuilder<ulong, StagingRecord>();
            for (int i = 0; i < stagingCount; i++)
            {
                var s = new StagingRecord(r.ReadVarUInt(), r.ReadVarUInt(), r.ReadVarUInt(), r.ReadOperationId(), r.ReadString(), (StagingState)r.ReadByte(),
                    r.ReadVarUInt(), r.ReadVarUInt(), r.ReadDateTimeOffset());
                staging[s.Id] = s;
            }

            int snapshotCount = r.ReadCount(20);
            var snapshots = ImmutableSortedDictionary.CreateBuilder<string, SnapshotRecord>(StringComparer.Ordinal);
            for (int i = 0; i < snapshotCount; i++)
            {
                SnapshotId id = r.ReadSnapshotId();
                string name = r.ReadString();
                ulong boundary = r.ReadVarUInt();
                var label = (DfsConsistencyLabel)r.ReadByte();
                int adapterCount = r.ReadCount(3);
                var adapters = ImmutableList.CreateBuilder<AdapterRecord>();
                for (int j = 0; j < adapterCount; j++)
                    adapters.Add(ReadAdapter(ref r));
                DateTimeOffset created = r.ReadDateTimeOffset();
                DateTimeOffset? expires = r.ReadOptionalDateTimeOffset();
                bool hold = r.ReadBool();
                ImmutableSortedDictionary<ulong, InodeRecord> root = ReadRoot(ref r, versions);
                snapshots[name] = new SnapshotRecord(id, name, boundary, label, adapters.ToImmutable(), created, expires, hold, root);
            }

            int pinCount = r.ReadCount(12);
            var pins = ImmutableSortedDictionary.CreateBuilder<ulong, PinRecord>();
            for (int i = 0; i < pinCount; i++)
            {
                ulong id = r.ReadVarUInt();
                var kind = (PinKind)r.ReadByte();
                ulong session = r.ReadVarUInt();
                string principal = r.ReadString();
                ulong created = r.ReadVarUInt();
                ulong pinIndex = r.ReadVarUInt();
                ulong handle = r.ReadVarUInt();
                string? snapshot = r.ReadOptionalString();
                ulong directory = r.ReadVarUInt();
                string binding = r.ReadString();
                ImmutableSortedDictionary<ulong, InodeRecord>? root = r.ReadBool() ? ReadRoot(ref r, versions) : null;
                string? target = r.ReadOptionalString();
                pins[id] = new PinRecord(id, kind, session, principal, created, pinIndex, handle, snapshot, directory, binding, root, target);
            }

            int derefCount = r.ReadCount(17);
            var deref = ImmutableList.CreateBuilder<DerefEntry>();
            for (int i = 0; i < derefCount; i++)
                deref.Add(new DerefEntry(r.ReadVarUInt(), r.ReadObjectId()));

            int progressCount = r.ReadCount(10);
            var progress = ImmutableSortedDictionary.CreateBuilder<string, BackupProgressRecord>(StringComparer.Ordinal);
            for (int i = 0; i < progressCount; i++)
            {
                BackupProgressRecord p = ReadBackupProgress(ref r);
                progress[p.Target] = p;
            }

            int keyCount = r.ReadCount(1);
            var keys = ImmutableSortedSet.CreateBuilder<uint>();
            for (int i = 0; i < keyCount; i++)
                keys.Add(r.ReadVarUInt32());
            DfsStoragePolicy? migration = r.ReadOptionalStoragePolicy();

            RestoreRecord? restore = null;
            if (r.ReadBool())
                restore = new RestoreRecord(r.ReadString(), r.ReadBool(), ReadRoot(ref r, versions), r.ReadVarUInt(), r.ReadVarUInt());

            int intentCount = r.ReadCount(30);
            var intents = ImmutableSortedDictionary.CreateBuilder<DfsOperationId, RestoreIntentRecord>(OperationIdComparer.Instance);
            for (int i = 0; i < intentCount; i++)
            {
                var intent = new RestoreIntentRecord(r.ReadOperationId(), r.ReadUInt128(), r.ReadString(), r.ReadString(), r.ReadSnapshotId(), r.ReadVarUInt(),
                    r.ReadString(), r.ReadOptionalString(), ReadRoot(ref r, versions), r.ReadVarUInt(), r.ReadDateTimeOffset());
                if (!intents.TryAdd(intent.Operation, intent))
                    throw new DfsIntegrityException($"Duplicate restore intent {intent.Operation} in the checkpoint.");
            }

            r.ReadExtensions();
            r.EnsureEnd();
            return new VolumeState
            {
                Config = config,
                AppliedIndex = applied,
                LeaderTime = leaderTime,
                Inodes = inodes,
                Detached = detached,
                WriterGenerations = generations.ToImmutable(),
                NextInodeId = nextInode,
                NextHandleId = nextHandle,
                NextStagingId = nextStaging,
                NextSessionId = nextSession,
                NextPinId = nextPin,
                Handles = handles.ToImmutable(),
                Sessions = sessions.ToImmutable(),
                RetiredSessionsBelow = retiredBelow,
                Transactions = txs.ToImmutable(),
                Staging = staging.ToImmutable(),
                Snapshots = snapshots.ToImmutable(),
                Pins = pins.ToImmutable(),
                DerefLog = deref.ToImmutable(),
                BackupProgress = progress.ToImmutable(),
                KeyVersions = keys.ToImmutable(),
                MigrationTarget = migration,
                Restore = restore,
                Restores = intents.ToImmutable(),
            };
        }
        catch (DfsProtocolException ex)
        {
            throw new DfsIntegrityException("Malformed volume checkpoint: " + ex.Message, ex);
        }
        catch (ArgumentException ex)
        {
            throw new DfsIntegrityException("Malformed volume checkpoint: " + ex.Message, ex);
        }
    }

    private static void WriteConfig(ref WireWriter w, VolumeConfig c)
    {
        w.WriteVolumeId(c.Id);
        w.WriteString(c.Name);
        w.WriteVarUInt(c.Epoch);
        w.WriteGuid(c.ClusterId);
        w.WriteString(c.Creator);
        w.WriteDateTimeOffset(c.CreatedUtc);
        w.WriteVarInt(c.InlineThreshold);
        w.WriteVolumeLimits(c.Limits);
        w.WriteString(c.StoragePool);
        w.WriteBool(c.EnforceTraverseChecking);
        w.WriteStoragePolicy(c.DefaultPolicy);
        w.WritePolicyMap(c.NamedPolicies);
    }

    private static VolumeConfig ReadConfig(ref WireReader r)
        => new(r.ReadVolumeId(), r.ReadString(), r.ReadVarUInt(), r.ReadGuid(), r.ReadString(), r.ReadDateTimeOffset(), r.ReadVarInt32(),
            r.ReadVolumeLimits(), r.ReadString(), r.ReadBool(), r.ReadStoragePolicy(),
            ImmutableSortedDictionary.CreateRange(StringComparer.Ordinal, r.ReadPolicyMap()));

    private static void WriteRoot(ref WireWriter w, ImmutableSortedDictionary<ulong, InodeRecord> root)
    {
        w.WriteCount(root.Count);
        foreach (InodeRecord n in root.Values)
        {
            w.WriteVarUInt(n.Id);
            w.WriteVarUInt(n.Revision);
        }
    }

    private static ImmutableSortedDictionary<ulong, InodeRecord> ReadRoot(ref WireReader r, Dictionary<(ulong, ulong), InodeRecord> versions)
    {
        int count = r.ReadCount(2);
        var builder = ImmutableSortedDictionary.CreateBuilder<ulong, InodeRecord>();
        for (int i = 0; i < count; i++)
        {
            ulong id = r.ReadVarUInt();
            ulong revision = r.ReadVarUInt();
            if (!versions.TryGetValue((id, revision), out InodeRecord? n))
                throw new DfsIntegrityException($"The checkpoint references an unknown inode version ({id}, {revision}).");
            builder[id] = n;
        }

        return builder.ToImmutable();
    }

    private static void WriteUInt64Set(ref WireWriter w, ImmutableSortedSet<ulong> set)
    {
        w.WriteCount(set.Count);
        foreach (ulong v in set)
            w.WriteVarUInt(v);
    }

    private static ImmutableSortedSet<ulong> ReadUInt64Set(ref WireReader r)
    {
        int count = r.ReadCount(1);
        var builder = ImmutableSortedSet.CreateBuilder<ulong>();
        for (int i = 0; i < count; i++)
            builder.Add(r.ReadVarUInt());
        return builder.ToImmutable();
    }

    private static void WriteHandle(ref WireWriter w, HandleRecord h)
    {
        w.WriteVarUInt(h.Id);
        w.WriteVarUInt(h.Inode);
        w.WriteString(h.Stream);
        w.WriteVarUInt(h.Session);
        w.WriteString(h.Principal);
        w.WriteVarInt((int)h.Mode);
        w.WriteVarInt((int)h.Access);
        w.WriteVarInt((int)h.Share);
        w.WriteBool(h.IsWriter);
        w.WriteVarUInt(h.WriterGeneration);
        w.WriteByte((byte)h.View);
        w.WriteBool(h.PinnedInode is not null);
        if (h.PinnedInode is { } pinned)
        {
            w.WriteVarUInt(pinned.Id);
            w.WriteVarUInt(pinned.Revision);
        }

        w.WriteVarUInt(h.PinId);
        w.WriteVarUInt(h.OpenedIndex);
        w.WriteVarUInt(unchecked((uint)h.GrantedRights));
        w.WriteBool(h.IsAppend);
    }

    private static HandleRecord ReadHandle(ref WireReader r, Dictionary<(ulong, ulong), InodeRecord> versions)
    {
        ulong id = r.ReadVarUInt();
        ulong inode = r.ReadVarUInt();
        string stream = r.ReadString();
        ulong session = r.ReadVarUInt();
        string principal = r.ReadString();
        var mode = (FileMode)r.ReadVarInt32();
        var access = (FileAccess)r.ReadVarInt32();
        var share = (FileShare)r.ReadVarInt32();
        bool writer = r.ReadBool();
        ulong generation = r.ReadVarUInt();
        var view = (HandleView)r.ReadByte();
        InodeRecord? pinned = null;
        if (r.ReadBool())
        {
            ulong pid = r.ReadVarUInt();
            ulong prev = r.ReadVarUInt();
            if (!versions.TryGetValue((pid, prev), out pinned))
                throw new DfsIntegrityException($"Handle {id} references an unknown inode version ({pid}, {prev}).");
        }

        ulong pin = r.ReadVarUInt();
        ulong opened = r.ReadVarUInt();
        var rights = unchecked((DfsFileSystemRights)r.ReadVarUInt32());
        bool append = r.ReadBool();
        return new HandleRecord(id, inode, stream, session, principal, mode, access, share, writer, generation, view, pinned, pin, opened, rights, append);
    }

    private static void WriteSession(ref WireWriter w, SessionRecord s)
    {
        w.WriteVarUInt(s.Id);
        w.WriteString(s.Principal);
        w.WriteString(s.ClientName);
        w.WriteUInt128(s.Nonce);
        w.WriteByte((byte)s.State);
        w.WriteVarUInt(s.CreatedIndex);
        w.WriteDateTimeOffset(s.LastActivityUtc);
        w.WriteOptionalDateTimeOffset(s.ExpiredUtc);
        w.WriteVarUInt(s.RetiredWatermark);
        w.WriteVarUInt(s.HighestSequence);
        w.WriteCount(s.Reserved.Count);
        foreach (ReservedRange range in s.Reserved)
        {
            w.WriteVarUInt(range.First);
            w.WriteVarUInt(range.Count);
            w.WriteVarUInt(range.Used);
        }

        w.WriteCount(s.Outcomes.Count);
        foreach (OutcomeRecord o in s.Outcomes.Values)
        {
            w.WriteVarUInt(o.Sequence);
            w.WriteVarInt((int)o.Operation);
            w.WriteUInt128(o.PayloadHash);
            w.WriteString(o.Principal);
            w.WriteVarInt((int)o.Code);
            w.WriteString(o.Message);
            w.WriteBytes(o.Reply.Span);
            w.WriteVarUInt(o.Index);
            w.WriteBool(o.Fenced);
        }

        w.WriteVarInt(s.MaxOutcomes);
        w.WriteTimeSpan(s.OutcomeRetention);
        w.WriteTimeSpan(s.LeaseDuration);
        w.WriteTimeSpan(s.SessionTimeout);
    }

    private static SessionRecord ReadSession(ref WireReader r)
    {
        ulong id = r.ReadVarUInt();
        string principal = r.ReadString();
        string client = r.ReadString();
        UInt128 nonce = r.ReadUInt128();
        var state = (SessionState)r.ReadByte();
        ulong created = r.ReadVarUInt();
        DateTimeOffset activity = r.ReadDateTimeOffset();
        DateTimeOffset? expired = r.ReadOptionalDateTimeOffset();
        ulong retired = r.ReadVarUInt();
        ulong highest = r.ReadVarUInt();
        int rangeCount = r.ReadCount(3);
        var ranges = ImmutableList.CreateBuilder<ReservedRange>();
        for (int i = 0; i < rangeCount; i++)
            ranges.Add(new ReservedRange(r.ReadVarUInt(), r.ReadVarUInt(), r.ReadVarUInt()));
        int outcomeCount = r.ReadCount(24);
        var outcomes = ImmutableSortedDictionary.CreateBuilder<ulong, OutcomeRecord>();
        for (int i = 0; i < outcomeCount; i++)
        {
            var o = new OutcomeRecord(r.ReadVarUInt(), (DfsOperationKind)r.ReadVarInt32(), r.ReadUInt128(), r.ReadString(), (DfsErrorCode)r.ReadVarInt32(),
                r.ReadString(), r.ReadBytesCopy(), r.ReadVarUInt(), r.ReadBool());
            outcomes[o.Sequence] = o;
        }

        return new SessionRecord(id, principal, client, nonce, state, created, activity, expired, retired, highest, ranges.ToImmutable(), outcomes.ToImmutable(),
            r.ReadVarInt32(), r.ReadTimeSpan(), r.ReadTimeSpan(), r.ReadTimeSpan());
    }

    private static void WriteTx(ref WireWriter w, TxRecord t)
    {
        w.WriteOperationId(t.Id);
        w.WriteVarUInt(t.Session);
        w.WriteString(t.Principal);
        w.WriteByte((byte)t.State);
        w.WriteVarUInt(t.ReadIndex);
        w.WriteVarUInt(t.Staging);
        w.WriteVarUInt(t.PinId);
        w.WriteDateTimeOffset(t.DeadlineUtc);
        w.WriteCount(t.Reservations.Count);
        foreach (StreamKey k in t.Reservations)
        {
            w.WriteVarUInt(k.Inode);
            w.WriteString(k.Stream);
        }

        w.WriteVarUInt(t.FinalIndex);
    }

    private static TxRecord ReadTx(ref WireReader r)
    {
        DfsOperationId id = r.ReadOperationId();
        ulong session = r.ReadVarUInt();
        string principal = r.ReadString();
        var state = (TxState)r.ReadByte();
        ulong readIndex = r.ReadVarUInt();
        ulong staging = r.ReadVarUInt();
        ulong pin = r.ReadVarUInt();
        DateTimeOffset deadline = r.ReadDateTimeOffset();
        int count = r.ReadCount(2);
        var keys = ImmutableSortedSet.CreateBuilder(StreamKeyComparer.Instance);
        for (int i = 0; i < count; i++)
            keys.Add(new StreamKey(r.ReadVarUInt(), r.ReadString()));
        return new TxRecord(id, session, principal, state, readIndex, staging, pin, deadline, keys.ToImmutable(), r.ReadVarUInt());
    }
}
