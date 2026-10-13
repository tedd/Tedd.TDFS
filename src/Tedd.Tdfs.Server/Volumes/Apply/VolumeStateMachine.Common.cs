using System.Collections.Immutable;
using System.Security.Cryptography;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>Snapshot name rules: 1..255 UTF-8 bytes, no control characters, no '/', '\' or ':'.</summary>
internal static class SnapshotNames
{
    public static string? GetError(string? name)
    {
        if (string.IsNullOrEmpty(name))
            return "A snapshot name must not be empty.";
        if (System.Text.Encoding.UTF8.GetByteCount(name) > 255)
            return "A snapshot name exceeds 255 UTF-8 bytes.";
        foreach (char ch in name)
        {
            if (char.IsControl(ch) || ch is '/' or '\\' or ':')
                return $"The snapshot name '{name}' contains an invalid character.";
        }

        return null;
    }
}

internal sealed partial class VolumeStateMachine
{
    // ------------------------------------------------------------------------------------------------- authorization

    /// <summary>The caller's rights on the first element of a chain (from the identity set carried in the command).</summary>
    internal static DfsFileSystemRights RightsOf(ApplyContext ctx, IReadOnlyList<InodeRecord> chain)
    {
        OperationHeader op = ctx.Op ?? throw Fail(DfsErrorCode.AccessDenied, "The command carries no caller identity.");
        return VolumeAccess.Rights(chain, op.IdentitySet, op.IsAdmin);
    }

    /// <summary>Requires <paramref name="required"/> on the first element of the chain.</summary>
    internal static void Require(ApplyContext ctx, IReadOnlyList<InodeRecord> chain, DfsFileSystemRights required, string what)
    {
        if (!VolumeAccess.Has(RightsOf(ctx, chain), required))
            throw Denied(ctx, what, required);
    }

    /// <summary>Requires one of two rights alternatives (ACL-05 "X OR Y").</summary>
    internal static void RequireEither(ApplyContext ctx, IReadOnlyList<InodeRecord> first, DfsFileSystemRights a, IReadOnlyList<InodeRecord>? second,
        DfsFileSystemRights b, string what)
    {
        if (VolumeAccess.Has(RightsOf(ctx, first), a))
            return;
        if (second is { Count: > 0 } && VolumeAccess.Has(RightsOf(ctx, second), b))
            return;
        throw Denied(ctx, what, a);
    }

    /// <summary>Delete rights on an attached inode: Delete on it, or DeleteSubdirectoriesAndFiles on its parent (ACL-016).</summary>
    internal static void RequireDelete(ApplyContext ctx, IReadOnlyList<InodeRecord> chain, string what)
        => RequireEither(ctx, chain, DfsFileSystemRights.Delete, chain.Count > 1 ? chain.Skip(1).ToArray() : null,
            DfsFileSystemRights.DeleteSubdirectoriesAndFiles, what);

    /// <summary>Traverse enforcement of a resolution (ACL-025).</summary>
    internal static void RequireTraverse(VolumeState s, ApplyContext ctx, Resolution resolution)
    {
        OperationHeader? op = ctx.Op;
        if (op is null || !s.Config.EnforceTraverseChecking)
            return;
        if (!VolumeAccess.TraverseAllowed(true, resolution, op.IdentitySet, op.IsAdmin))
            throw Fail(DfsErrorCode.AccessDenied, $"Traverse is denied on an ancestor of '{resolution.Path}'.");
    }

    private static ApplyFailure Denied(ApplyContext ctx, string what, DfsFileSystemRights required)
        => Fail(DfsErrorCode.AccessDenied, $"Access denied: {ctx.Op?.Principal ?? "caller"} lacks {DfsAccessRule.FormatRights(required)} ({what}).");

    // ------------------------------------------------------------------------------------------------- sharing

    /// <summary>
    /// Bidirectional share compatibility of a new open with one active handle plus writer exclusivity (LOCK-01/02).
    /// <see cref="FileShare.Delete"/> governs delete/rename only.
    /// </summary>
    internal static bool Compatible(HandleRecord active, FileAccess access, FileShare share)
    {
        bool newReads = (access & FileAccess.Read) != 0;
        bool newWrites = (access & FileAccess.Write) != 0;
        bool oldReads = (active.Access & FileAccess.Read) != 0;
        bool oldWrites = (active.Access & FileAccess.Write) != 0;
        if (newReads && (active.Share & FileShare.Read) == 0)
            return false;
        if (newWrites && (active.Share & FileShare.Write) == 0)
            return false;
        if (oldReads && (share & FileShare.Read) == 0)
            return false;
        if (oldWrites && (share & FileShare.Write) == 0)
            return false;
        return !(newWrites && oldWrites);
    }

    /// <summary>Active handles of an inode (all streams).</summary>
    internal static IEnumerable<HandleRecord> HandlesOf(VolumeState s, ulong inode) => s.Handles.Values.Where(h => h.Inode == inode);

    /// <summary>Active handles of one stream.</summary>
    internal static IEnumerable<HandleRecord> HandlesOf(VolumeState s, StreamKey key)
        => s.Handles.Values.Where(h => h.Inode == key.Inode && string.Equals(h.Stream, key.Stream, StringComparison.Ordinal));

    /// <summary>Requires every handle of the inode (any stream) to grant <see cref="FileShare.Delete"/> (NS-06, ADS-015).</summary>
    internal static void RequireDeleteSharing(VolumeState s, ulong inode, string what)
    {
        if (HandlesOf(s, inode).FirstOrDefault(h => (h.Share & FileShare.Delete) == 0) is { } blocking)
            throw Fail(DfsErrorCode.SharingViolation, $"{what}: handle {blocking.Id} does not grant FileShare.Delete.");
    }

    /// <summary>
    /// The implicit, instantaneous writer reservation of WriteAll/Append/Copy by path (ARCHITECTURE §9.4): no writer may hold
    /// the stream and every reader must grant <see cref="FileShare.Write"/>.
    /// </summary>
    internal static void RequireImplicitWriter(VolumeState s, ApplyContext ctx, StreamKey key, string what)
    {
        foreach (HandleRecord h in HandlesOf(s, key))
        {
            if (h.IsWriter)
                throw Fail(DfsErrorCode.WriterConflict, $"{what}: handle {h.Id} holds the writer reservation.");
            if ((h.Share & FileShare.Write) == 0)
                throw Fail(DfsErrorCode.SharingViolation, $"{what}: handle {h.Id} does not grant FileShare.Write.");
        }

        if (ReservationHolder(s, ctx, key) is { } holder)
            throw Fail(DfsErrorCode.SharingViolation, $"{what}: transaction {holder.Id} holds the writer reservation.");
    }

    /// <summary>
    /// The active transaction, other than the command's own (<see cref="ApplyContext.Transaction"/>), that holds the writer
    /// reservation of <paramref name="key"/> (LOCK-01, LOCK-019), or null.
    /// </summary>
    internal static TxRecord? ReservationHolder(VolumeState s, ApplyContext ctx, StreamKey key)
    {
        foreach (TxRecord tx in s.Transactions.Values)
        {
            if (tx.State == TxState.Active && tx.Id != ctx.Transaction && tx.Reservations.Contains(key))
                return tx;
        }

        return null;
    }

    // ------------------------------------------------------------------------------------------------- inodes

    /// <summary>A new inode in <paramref name="parent"/> owned by the caller, inheriting its ACL dynamically.</summary>
    internal static InodeRecord NewInode(VolumeState s, ApplyContext ctx, ulong id, InodeKind kind, InodeRecord parent, string name, string policy)
    {
        DfsIdentity owner = ctx.Op?.User ?? DfsIdentity.Administrators;
        var security = new DfsSecurityDescriptor(owner, [], false);
        ImmutableSortedDictionary<string, StreamRecord> streams = kind == InodeKind.File
            ? InodeRecord.NoStreams.Add(DfsStreamPath.DefaultStreamName, StreamRecord.Empty(ctx.Index))
            : InodeRecord.NoStreams;
        _ = s;
        return new InodeRecord(id, kind, ctx.Index, ctx.Index, ctx.Now, ctx.Now, security, policy, parent.Id, name, streams, InodeRecord.NoChildren)
        {
            MetadataRevision = ctx.Index,
        };
    }

    /// <summary>Adds a child entry to a directory (bumping its revision) and stores both records.</summary>
    internal static VolumeState Link(VolumeState s, ApplyContext ctx, InodeRecord parent, InodeRecord child)
    {
        InodeRecord updatedParent = Touch(parent with { Children = parent.Children.SetItem(child.Name, child.Id) }, ctx);
        return s with { Inodes = s.Inodes.SetItem(updatedParent.Id, updatedParent).SetItem(child.Id, child) };
    }

    /// <summary>A new version of an inode modified by the current entry.</summary>
    internal static InodeRecord Touch(InodeRecord inode, ApplyContext ctx) => inode with { Revision = ctx.Index, ModifiedUtc = ctx.Now };

    /// <summary>A new version of an inode whose own metadata (owner, ACL, policy, name or parent) the current entry changed.</summary>
    internal static InodeRecord TouchMetadata(InodeRecord inode, ApplyContext ctx) => Touch(inode, ctx) with { MetadataRevision = ctx.Index };

    /// <summary>Stores an inode record.</summary>
    internal static VolumeState Put(VolumeState s, InodeRecord inode) => s with { Inodes = s.Inodes.SetItem(inode.Id, inode) };

    /// <summary>Allocates an inode id.</summary>
    internal static (VolumeState State, ulong Id) AllocateInode(VolumeState s) => (s with { NextInodeId = s.NextInodeId + 1 }, s.NextInodeId);

    // ------------------------------------------------------------------------------------------------- dereference / reclaim

    /// <summary>Records objects that the current root stopped referencing (ARCHITECTURE §8).</summary>
    internal static VolumeState Dereference(VolumeState s, IEnumerable<ObjectId> objects, ulong index)
    {
        var distinct = new SortedSet<ObjectId>(objects, ObjectIdComparer.Instance);
        if (distinct.Count == 0)
            return s;
        ImmutableList<DerefEntry>.Builder log = s.DerefLog.ToBuilder();
        foreach (ObjectId o in distinct)
            log.Add(new DerefEntry(index, o));
        return s with { DerefLog = log.ToImmutable() };
    }

    /// <summary>Objects referenced by <paramref name="before"/> but not by <paramref name="after"/>.</summary>
    internal static IEnumerable<ObjectId> Dropped(StreamRecord? before, StreamRecord? after)
    {
        if (before is null || before.IsInline)
            return [];
        HashSet<ObjectId> kept = after is null ? [] : [.. after.Objects()];
        return before.Objects().Where(o => !kept.Contains(o));
    }

    /// <summary>Removes an inode that is unreachable from the namespace and has no handles; its objects enter the deref log.</summary>
    internal static VolumeState Reclaim(VolumeState s, ulong inodeId, ulong index)
    {
        if (s.Find(inodeId) is not { } inode || inodeId == InodeRecord.RootId)
            return s;
        s = Dereference(s, inode.Objects(), index);
        ImmutableSortedDictionary<StreamKey, ulong> generations = s.WriterGenerations;
        foreach (string stream in inode.Streams.Keys)
            generations = generations.Remove(new StreamKey(inodeId, stream));
        return s with { Inodes = s.Inodes.Remove(inodeId), Detached = s.Detached.Remove(inodeId), WriterGenerations = generations };
    }

    /// <summary>
    /// Removes a subtree from the namespace (already unlinked from its parent): inodes with open handles stay as detached
    /// inodes (readable/writable through those handles, never re-linked); the others are reclaimed now (NS-07, OPEN-017).
    /// </summary>
    internal static VolumeState DetachSubtree(VolumeState s, IReadOnlyList<InodeRecord> subtree, ulong index)
    {
        var withHandles = new HashSet<ulong>(s.Handles.Values.Select(h => h.Inode));
        foreach (InodeRecord n in subtree)
        {
            if (withHandles.Contains(n.Id))
                s = s with { Detached = s.Detached.Add(n.Id) };
            else
                s = Reclaim(s, n.Id, index);
        }

        return s;
    }

    // ------------------------------------------------------------------------------------------------- handles, pins, staging

    /// <summary>
    /// Releases a handle: sharing reservation, writer authority, pin and bound staging sessions; a detached inode whose last
    /// handle closes is reclaimed (ARCHITECTURE §5.3).
    /// </summary>
    internal static VolumeState ReleaseHandle(VolumeState s, HandleRecord h, ulong index)
    {
        s = s with { Handles = s.Handles.Remove(h.Id), Pins = s.Pins.Remove(h.PinId) };
        foreach (StagingRecord staging in s.Staging.Values.Where(x => x.Handle == h.Id && x.IsOpen).ToArray())
            s = CloseStaging(s, staging, index);
        if (s.Detached.Contains(h.Inode) && !s.Handles.Values.Any(x => x.Inode == h.Inode))
            s = Reclaim(s, h.Inode, index);
        return s;
    }

    /// <summary>Closes a staging session (its unreferenced objects become garbage).</summary>
    internal static VolumeState CloseStaging(VolumeState s, StagingRecord staging, ulong index)
        => staging.IsOpen ? s with { Staging = s.Staging.SetItem(staging.Id, staging with { State = StagingState.Closed, ClosedIndex = index }) } : s;

    /// <summary>Releases everything a session owns (expiry).</summary>
    internal static VolumeState ReleaseSession(VolumeState s, ulong session, ulong index)
    {
        foreach (HandleRecord h in s.Handles.Values.Where(x => x.Session == session).ToArray())
            s = ReleaseHandle(s, h, index);
        foreach (PinRecord p in s.Pins.Values.Where(x => x.Session == session && x.Kind != PinKind.Handle).ToArray())
            s = s with { Pins = s.Pins.Remove(p.Id) };
        foreach (StagingRecord staging in s.Staging.Values.Where(x => x.Session == session && x.IsOpen).ToArray())
            s = CloseStaging(s, staging, index);
        foreach (TxRecord tx in s.Transactions.Values.Where(x => x.Session == session && x.State == TxState.Active).ToArray())
        {
            s = s with { Transactions = s.Transactions.SetItem(tx.Id, tx with { State = TxState.Aborted, FinalIndex = index }), Pins = s.Pins.Remove(tx.PinId) };
            if (s.Staging.TryGetValue(tx.Staging, out StagingRecord? txStaging))
                s = CloseStaging(s, txStaging, index);
        }

        return s;
    }

    /// <summary>Allocates a pin id and stores the pin.</summary>
    internal static (VolumeState State, PinRecord Pin) AddPin(VolumeState s, PinKind kind, ulong session, string principal, ulong index, ulong pinIndex,
        ulong handle, string? snapshot, ulong directory, string binding, ImmutableSortedDictionary<ulong, InodeRecord>? root, string? target)
    {
        var pin = new PinRecord(s.NextPinId, kind, session, principal, index, pinIndex, handle, snapshot, directory, binding, root, target);
        return (s with { NextPinId = s.NextPinId + 1, Pins = s.Pins.Add(pin.Id, pin) }, pin);
    }

    // ------------------------------------------------------------------------------------------------- sessions

    /// <summary>The caller's active session, or a failure.</summary>
    internal static SessionRecord RequireSession(VolumeState s, ApplyContext ctx, ulong session)
    {
        string principal = ctx.Op?.Principal ?? string.Empty;
        if (session == 0)
            throw Fail(DfsErrorCode.InvalidArgument, "The request needs a session (OpenSession first).");
        if (!s.Sessions.TryGetValue(session, out SessionRecord? record))
        {
            throw session < s.NextSessionId
                ? Fail(DfsErrorCode.LeaseLost, $"Session {session} expired.")
                : Fail(DfsErrorCode.InvalidArgument, $"Session {session} was never issued by this volume.");
        }

        if (!string.Equals(record.Principal, principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.AccessDenied, $"Session {session} belongs to another principal.");
        if (record.State != SessionState.Active)
            throw Fail(DfsErrorCode.LeaseLost, $"Session {session} expired.");
        return record;
    }

    // ------------------------------------------------------------------------------------------------- descriptions

    /// <summary>Owner text reported in <see cref="FileInfoData.Owner"/> (principal name for user owners).</summary>
    internal static string OwnerText(DfsIdentity owner) => owner.Kind == DfsIdentityKind.User ? owner.Name : owner.ToString();

    /// <summary>
    /// Metadata of an inode (or one of its streams) as reported to clients (FileInfoData, NS-04, ADS-024). The writer
    /// generation comes from <paramref name="state"/> (null for snapshot views: 0).
    /// </summary>
    internal static FileInfoData Describe(VolumeState? state, InodeRecord inode, string path, string? stream)
    {
        DfsSecurityDescriptor security = inode.Security;
        bool ownerOnly = Matches(security, DfsSecurityDescriptor.FromAccessFlags(security.Owner, true, false));
        bool readOnly = !ownerOnly && Matches(security, DfsSecurityDescriptor.FromAccessFlags(security.Owner, false, true));
        string streamName = stream ?? DfsStreamPath.DefaultStreamName;
        StreamRecord? s = inode.GetStream(streamName);
        bool isStream = streamName.Length > 0;
        long length = s?.Length ?? 0;
        ulong revision = isStream ? s?.Revision ?? inode.Revision : inode.ReportedRevision;
        ulong generation = state?.WriterGenerationOf(new StreamKey(inode.Id, streamName)) ?? 0;
        return new FileInfoData(path, inode.IsDirectory ? WireEntryKind.Directory : WireEntryKind.File, new InodeId(inode.Id), revision,
            inode.IsDirectory && !isStream ? 0 : length, inode.CreatedUtc, inode.ModifiedUtc, OwnerText(security.Owner), ownerOnly, readOnly,
            inode.PolicyName, s?.IsInline ?? false, s?.IsSparse ?? false, s is null || s.IsInline ? 0 : s.Extents.Count,
            inode.IsDirectory ? inode.Children.Count : 0, inode.IsDirectory && !isStream ? 0 : generation)
        {
            IsStream = isStream,
            StreamName = isStream ? streamName : null,
            StreamCount = inode.NamedStreamCount,
        };
    }

    private static bool Matches(DfsSecurityDescriptor actual, DfsSecurityDescriptor shortcut)
        => actual.AreAccessRulesProtected == shortcut.AreAccessRulesProtected && actual.Rules.Count == shortcut.Rules.Count
            && actual.Rules.Zip(shortcut.Rules).All(p => p.First == p.Second) && shortcut.Rules.Count > 0;

    /// <summary>Deterministic snapshot id of the snapshot created at <paramref name="index"/>.</summary>
    internal static SnapshotId SnapshotIdAt(VolumeConfig config, ulong index)
    {
        Span<byte> input = stackalloc byte[24];
        config.Id.Value.TryWriteBytes(input);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(input[16..], index);
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(input, hash);
        return new SnapshotId(new Guid(hash[..16]));
    }

    /// <summary>Wire description of a snapshot.</summary>
    internal static SnapshotInfoData DescribeSnapshot(VolumeState s, SnapshotRecord snapshot)
        => new(snapshot.Id, snapshot.Name, s.Config.Name, s.TokenAt(snapshot.BoundaryIndex), snapshot.Label,
            snapshot.Adapters.Select(a => new AdapterRecordData(a.Id, a.Version, a.RestoreRequirements)).ToArray(), snapshot.CreatedUtc, snapshot.Retention);

    /// <summary>
    /// Commit-and-snapshot (FSD §11.2): when <paramref name="name"/> is set, creates the snapshot of the state after the
    /// mutation in the same entry (Snapshot grant required; a name collision fails the whole command).
    /// </summary>
    internal static (VolumeState State, SnapshotInfoData? Info) SnapshotInSameEntry(VolumeState s, ApplyContext ctx, string? name)
    {
        if (SnapshotNameOf(name) is not { } snapshotName)
            return (s, null);
        RequireGrant(ctx, DfsPermissions.Snapshot, "commit-and-snapshot");
        if (s.Snapshots.ContainsKey(snapshotName))
            throw Fail(DfsErrorCode.AlreadyExists, $"Snapshot '{snapshotName}' already exists.");
        var record = new SnapshotRecord(SnapshotIdAt(s.Config, ctx.Index), snapshotName, ctx.Index, DfsConsistencyLabel.FilesystemConsistent,
            ImmutableList<AdapterRecord>.Empty, ctx.Now, null, false, s.Inodes);
        s = s with { Snapshots = s.Snapshots.Add(snapshotName, record) };
        return (s, DescribeSnapshot(s, record));
    }
}

/// <summary>Deterministic ordering of object ids.</summary>
internal sealed class ObjectIdComparer : IComparer<ObjectId>
{
    public static readonly ObjectIdComparer Instance = new();

    public int Compare(ObjectId x, ObjectId y) => x.Value.CompareTo(y.Value);
}
