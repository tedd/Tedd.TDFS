using System.Collections.Immutable;
using Tedd.Tdfs.Server.Volumes.State;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Volumes.Apply;

internal sealed partial class VolumeStateMachine
{
    /// <inheritdoc />
    public VolumeStateInspection Inspect() => InspectState(_state);

    /// <inheritdoc />
    /// <remarks>
    /// The replicated state records no upload sizes (they live on storage nodes until published): always 0. A node's stored
    /// staged bytes are <see cref="LocalStagedBytes"/>.
    /// </remarks>
    public long StagedBytes => 0;

    /// <summary>
    /// Bytes of the given stored copies of this volume that are unpublished uploads at the applied state: not referenced by
    /// any root and bound to an open staging session (GC-015: a node's space held by uncommitted uploads).
    /// </summary>
    public long LocalStagedBytes(IEnumerable<ObjectRecordInfo> records)
    {
        ArgumentNullException.ThrowIfNull(records);
        VolumeState s = _state;
        HashSet<ObjectId>? reachable = null;
        long total = 0;
        foreach (ObjectRecordInfo record in records)
        {
            if (record.Volume != s.Config.Id || !s.Staging.TryGetValue(record.Staging.Value, out StagingRecord? staging) || !staging.IsOpen)
                continue;
            reachable ??= Reachable(s);
            if (!reachable.Contains(record.Id))
                total += record.Length;
        }

        return total;
    }

    /// <inheritdoc />
    public long PinnedBytes => ComputePinnedBytes(_state);

    /// <summary>An inspection view of <paramref name="s"/> (immutable; safe to use after the call returns).</summary>
    public static VolumeStateInspection InspectState(VolumeState s)
    {
        var handles = s.Handles.Values
            .Select(h => new HandleInspection(new HandleId(h.Id), new InodeId(h.Inode), h.Session, h.Access, h.Share, h.IsWriter, h.WriterGeneration, false))
            .ToArray();
        var snapshots = s.Snapshots.Values
            .OrderBy(x => x.BoundaryIndex)
            .Select(x => new SnapshotInspection(x.Id, x.Name, x.BoundaryIndex, x.Label, x.Root.Count))
            .ToArray();
        var sessions = s.Sessions.Values
            .Select(x => new SessionInspection(x.Id, x.Principal, x.State != SessionState.Active, x.RetiredWatermark, x.Outcomes.Count))
            .ToArray();
        var pins = s.Pins.Values.Select(p => new PinInspection(p.Id, p.Kind.ToString(), p.CreatedIndex, p.Snapshot)).ToArray();
        var staging = s.Staging.Values.Select(x => new StagingInspection(new StagingId(x.Id), x.IsOpen, x.CreatedIndex)).ToArray();
        int activeTx = s.Transactions.Values.Count(t => t.State == TxState.Active);
        var backups = s.BackupProgress.Values
            .Select(b => new BackupProgressInspection(b.Target, b.LastSnapshot, b.CoveredIndex, b.ReseedRequired, b.Lineage))
            .ToArray();
        return new VolumeStateInspection(s.Config.Id, s.Config.Epoch, s.AppliedIndex, s.Inodes.Count, handles, snapshots, sessions, pins, staging, activeTx, backups)
        {
            FileResolver = path => InspectFile(s, path),
            ReachableResolver = () => Reachable(s),
            PolicyResolver = () => ObjectPolicies(s),
        };
    }

    /// <summary>The name under which the volume default storage policy is reported (an inode with no named policy).</summary>
    public const string DefaultPolicyName = "default";

    /// <summary>The policy names of the inodes referencing each object of the current root, snapshots and pinned versions.</summary>
    private static IReadOnlyDictionary<ObjectId, IReadOnlySet<string>> ObjectPolicies(VolumeState s)
    {
        var result = new Dictionary<ObjectId, HashSet<string>>();
        void Add(InodeRecord n)
        {
            foreach (ObjectId o in n.Objects())
            {
                if (!result.TryGetValue(o, out HashSet<string>? names))
                    result[o] = names = new HashSet<string>(StringComparer.Ordinal);
                names.Add(n.PolicyName);
            }
        }

        foreach (InodeRecord n in s.Inodes.Values)
            Add(n);
        foreach (SnapshotRecord snapshot in s.Snapshots.Values)
        {
            foreach (InodeRecord n in snapshot.Root.Values)
                Add(n);
        }

        foreach (PinRecord pin in s.Pins.Values)
        {
            if (pin.Root is { } root)
            {
                foreach (InodeRecord n in root.Values)
                    Add(n);
            }
        }

        foreach (HandleRecord h in s.Handles.Values)
        {
            if (h.PinnedInode is { } pinned)
                Add(pinned);
        }

        return result.ToDictionary(p => p.Key, p => (IReadOnlySet<string>)p.Value);
    }

    /// <summary>The applied metadata of the file at <paramref name="path"/> (a stream path inspects its file), or null.</summary>
    public static FileInspection? InspectFile(VolumeState s, string path)
    {
        if (!DfsPath.TryNormalize(path, out string normalized, out _))
            return null;
        string filePath = DfsStreamPath.GetFilePath(normalized);
        Resolution res = new NamespaceView(s.Inodes).Resolve(filePath);
        if (!res.Found || res.Target!.IsDirectory)
            return null;
        InodeRecord inode = res.Target;
        StreamRecord main = inode.DefaultStream ?? StreamRecord.Empty(inode.CreatedIndex);
        var streams = inode.Streams
            .Where(p => p.Key.Length > 0)
            .Select(p => new StreamInspection(p.Key, p.Value.Length, p.Value.Revision, Extents(p.Value)))
            .ToArray();
        return new FileInspection(filePath, new InodeId(inode.Id), inode.ReportedRevision, main.Length, main.IsInline,
            s.WriterGenerationOf(new StreamKey(inode.Id, DfsStreamPath.DefaultStreamName)), inode.PolicyName.Length == 0 ? DefaultPolicyName : inode.PolicyName,
            Extents(main), streams);
    }

    private static IReadOnlyList<ExtentInspection> Extents(StreamRecord stream)
        => stream.IsInline ? [] : stream.Extents.Select(e => new ExtentInspection(e.Offset, e.Length, e.Object, e.ObjectOffset, e.ObjectLength)).ToArray();

    /// <summary>
    /// Bytes of objects the current namespace no longer references that are retained only for history: snapshot retention
    /// references, pinned roots (reader, enumeration and transfer pins) and version-pinned handles (GC-015, OPS-005: the
    /// space a volume holds beyond its visible files).
    /// </summary>
    internal static long ComputePinnedBytes(VolumeState s)
    {
        var live = new HashSet<ObjectId>();
        foreach (InodeRecord n in s.Inodes.Values)
            live.UnionWith(n.Objects());

        var pinned = new Dictionary<ObjectId, long>();
        void Add(InodeRecord n)
        {
            foreach (StreamRecord stream in n.Streams.Values)
            {
                if (stream.IsInline)
                    continue;
                foreach (ExtentRef e in stream.Extents)
                {
                    if (!live.Contains(e.Object))
                        pinned[e.Object] = e.ObjectLength;
                }
            }
        }

        foreach (SnapshotRecord snap in s.Snapshots.Values)
        {
            foreach (InodeRecord n in snap.Root.Values)
                Add(n);
        }

        foreach (PinRecord p in s.Pins.Values)
        {
            if (p.Root is null)
                continue;
            foreach (InodeRecord n in p.Root.Values)
                Add(n);
        }

        foreach (HandleRecord h in s.Handles.Values)
        {
            if (h.PinnedInode is not null)
                Add(h.PinnedInode);
        }

        long total = 0;
        foreach (long length in pinned.Values)
            total += length;
        _ = ImmutableArray<int>.Empty;
        return total;
    }
}
