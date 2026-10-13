using System.Collections.Immutable;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>Classification of a stored copy by the GC rules (ARCHITECTURE §8).</summary>
internal enum ObjectClass : byte
{
    /// <summary>Referenced by a root, protected by the dereference log, or its staging session is still open: keep.</summary>
    Keep = 0,

    /// <summary>Unreachable, unprotected and its staging session is closed: garbage at the decision index.</summary>
    Deletable = 1,

    /// <summary>The staging session is newer than the applied state: cannot be classified yet.</summary>
    Unknown = 2,
}

internal sealed partial class VolumeStateMachine
{
    /// <summary>
    /// GC watermark W (ARCHITECTURE §8): the lowest protection index of any live-read handle pin and active transaction read
    /// index; dereference entries at or above it stay protected. Version-pinned handles, snapshot readers, transfers and
    /// enumerations keep their versions reachable through their pinned roots instead. Without pins: applied index + 1.
    /// </summary>
    internal static ulong Watermark(VolumeState s, bool ignorePins = false)
    {
        ulong w = s.AppliedIndex + 1;
        if (ignorePins)
            return w;
        foreach (PinRecord pin in s.Pins.Values)
        {
            if (pin.Kind == PinKind.Handle && s.Handles.TryGetValue(pin.Handle, out HandleRecord? h) && h.View == HandleView.Live)
                w = Math.Min(w, pin.PinIndex);
        }

        foreach (TxRecord tx in s.Transactions.Values)
        {
            if (tx.State == TxState.Active)
                w = Math.Min(w, tx.ReadIndex);
        }

        return w;
    }

    /// <summary>
    /// Objects reachable at the applied index: the current root (detached inodes included), snapshot roots, pinned roots
    /// (snapshot readers, transfers, enumerations), version-pinned handle inodes, a staged restore and the snapshot roots of
    /// accepted restores into new volumes. <paramref name="ignorePins"/> (the IgnoreSnapshotPins negative control, MUT-009)
    /// marks the current root only.
    /// </summary>
    internal static HashSet<ObjectId> Reachable(VolumeState s, bool ignorePins = false)
    {
        var result = new HashSet<ObjectId>();
        var seen = new HashSet<(ulong, ulong)>();
        void AddRoot(ImmutableSortedDictionary<ulong, InodeRecord> root)
        {
            foreach (InodeRecord n in root.Values)
                AddInode(n);
        }

        void AddInode(InodeRecord n)
        {
            if (!seen.Add((n.Id, n.Revision)))
                return;
            foreach (ObjectId o in n.Objects())
                result.Add(o);
        }

        AddRoot(s.Inodes);
        if (ignorePins)
            return result;
        foreach (SnapshotRecord snap in s.Snapshots.Values)
            AddRoot(snap.Root);
        foreach (PinRecord pin in s.Pins.Values)
        {
            if (pin.Root is not null)
                AddRoot(pin.Root);
        }

        foreach (HandleRecord h in s.Handles.Values)
        {
            if (h.PinnedInode is not null)
                AddInode(h.PinnedInode);
        }

        if (s.Restore is { } restore)
            AddRoot(restore.Staged);
        foreach (RestoreIntentRecord intent in s.Restores.Values)
            AddRoot(intent.Root);
        return result;
    }

    /// <summary>Objects protected by the dereference log at watermark <paramref name="watermark"/>.</summary>
    internal static HashSet<ObjectId> Protected(VolumeState s, ulong watermark)
    {
        var result = new HashSet<ObjectId>();
        for (int i = s.DerefLog.Count - 1; i >= 0; i--)
        {
            DerefEntry e = s.DerefLog[i];
            if (e.Index < watermark)
                break;
            result.Add(e.Object);
        }

        return result;
    }

    /// <summary>
    /// Classifies one stored copy <c>(object, staging)</c> at the applied index (ARCHITECTURE §8): deletable iff unreachable,
    /// not protected and its staging session is closed or fully retired. Open staging sessions protect their objects
    /// regardless of age (GC-005).
    /// </summary>
    internal static ObjectClass Classify(VolumeState s, ObjectId obj, ulong staging, HashSet<ObjectId> reachable, HashSet<ObjectId> protectedObjects)
    {
        if (reachable.Contains(obj) || protectedObjects.Contains(obj))
            return ObjectClass.Keep;
        if (staging >= s.NextStagingId)
            return ObjectClass.Unknown;
        if (s.Staging.TryGetValue(staging, out StagingRecord? record) && record.IsOpen)
            return ObjectClass.Keep;
        return ObjectClass.Deletable;
    }

    private static (VolumeState, VolumeApplyResult) ApplyPrune(VolumeState s, ApplyContext ctx, PruneDerefLogCommand c)
    {
        ulong below = Math.Min(c.Below, Watermark(s));
        int keep = 0;
        while (keep < s.DerefLog.Count && s.DerefLog[keep].Index < below)
            keep++;
        ImmutableList<DerefEntry> log = keep == 0 ? s.DerefLog : s.DerefLog.RemoveRange(0, keep);
        ImmutableSortedDictionary<ulong, StagingRecord> staging = s.Staging;
        foreach (StagingRecord record in s.Staging.Values)
        {
            if (!record.IsOpen && record.ClosedIndex < below)
                staging = staging.Remove(record.Id);
        }

        s = s with { DerefLog = log, Staging = staging };
        s = RetireSessions(s, ctx.Now);
        return (s, VolumeApplyResult.Ok(null, below));
    }

    private static (VolumeState, VolumeApplyResult) ApplyBackupProgress(VolumeState s, ApplyContext ctx, RecordBackupProgressCommand c)
    {
        BackupProgressRecord p = c.Progress with { UpdatedUtc = ctx.Now };
        if (s.BackupProgress.TryGetValue(p.Target, out BackupProgressRecord? old) && old.Lineage == p.Lineage && old.CoveredIndex > p.CoveredIndex)
            p = old with { UpdatedUtc = ctx.Now };
        s = s with { BackupProgress = s.BackupProgress.SetItem(p.Target, p) };
        return (s, VolumeApplyResult.Ok(null, p));
    }

    private static (VolumeState, VolumeApplyResult) ApplyAbandonLineage(VolumeState s, ApplyContext ctx, AbandonTransferLineageCommand c)
    {
        foreach (PinRecord pin in s.Pins.Values.Where(p => p.Kind == PinKind.Transfer && string.Equals(p.Target, c.Target, StringComparison.Ordinal)).ToArray())
            s = s with { Pins = s.Pins.Remove(pin.Id) };
        BackupProgressRecord progress = s.BackupProgress.TryGetValue(c.Target, out BackupProgressRecord? old)
            ? old with { ReseedRequired = true, Lineage = c.NewLineage, UpdatedUtc = ctx.Now }
            : new BackupProgressRecord(c.Target, null, default, 0, null, false, true, c.NewLineage, ctx.Now);
        s = s with { BackupProgress = s.BackupProgress.SetItem(c.Target, progress) };
        return (s, VolumeApplyResult.Ok(null, progress));
    }

    private static (VolumeState, VolumeApplyResult) ApplyRotateKey(VolumeState s, ApplyContext ctx, RotateDataKeyCommand c)
    {
        if (c.Version == 0)
            throw Fail(DfsErrorCode.InvalidArgument, "Data-key versions start at 1.");
        s = s with { KeyVersions = s.KeyVersions.Add(c.Version) };
        _ = ctx;
        return (s, VolumeApplyResult.Ok(null, c.Version));
    }

    private static (VolumeState, VolumeApplyResult) ApplyMigratePolicy(VolumeState s, ApplyContext ctx, MigratePolicyCommand c)
    {
        if (c.Target?.GetValidationError() is { } error)
            throw Fail(DfsErrorCode.InvalidPolicy, error);
        s = s with { MigrationTarget = c.Target };
        _ = ctx;
        return (s, VolumeApplyResult.Ok(null));
    }
}
