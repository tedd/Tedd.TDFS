using System.Collections.Immutable;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

internal sealed partial class VolumeStateMachine
{
    /// <inheritdoc />
    public VolumeStateInspection Inspect() => InspectState(_state);

    /// <inheritdoc />
    /// <remarks>The replicated state records no upload sizes (they live on storage nodes until published): always 0.</remarks>
    public long StagedBytes => 0;

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
        };
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
        return new FileInspection(filePath, new InodeId(inode.Id), inode.Revision, main.Length, main.IsInline,
            s.WriterGenerationOf(new StreamKey(inode.Id, DfsStreamPath.DefaultStreamName)), inode.PolicyName, Extents(main), streams);
    }

    private static IReadOnlyList<ExtentInspection> Extents(StreamRecord stream)
        => stream.IsInline ? [] : stream.Extents.Select(e => new ExtentInspection(e.Offset, e.Length, e.Object, e.ObjectOffset, e.ObjectLength)).ToArray();

    /// <summary>Bytes of objects referenced only through pins (pinned roots and version-pinned handles).</summary>
    internal static long ComputePinnedBytes(VolumeState s)
    {
        var live = new HashSet<ObjectId>();
        foreach (InodeRecord n in s.Inodes.Values)
            live.UnionWith(n.Objects());
        foreach (SnapshotRecord snap in s.Snapshots.Values)
        {
            foreach (InodeRecord n in snap.Root.Values)
                live.UnionWith(n.Objects());
        }

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
