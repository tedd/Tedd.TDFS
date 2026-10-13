using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Commands;

// ---------------------------------------------------------------------------------------------------------------------
// Sessions and outcomes (1-99)
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>Opens a client session (ARCHITECTURE §7). Retries with the same (principal, nonce) return the same session.</summary>
internal sealed record OpenSessionCommand(string ClientName, UInt128 Nonce, TimeSpan LeaseDuration, TimeSpan SessionTimeout, int MaxOutcomes,
    TimeSpan OutcomeRetention) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.OpenSession;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(ClientName);
        w.WriteUInt128(Nonce);
        w.WriteTimeSpan(LeaseDuration);
        w.WriteTimeSpan(SessionTimeout);
        w.WriteVarInt(MaxOutcomes);
        w.WriteTimeSpan(OutcomeRetention);
    }

    internal static OpenSessionCommand ReadBody(ref WireReader r)
        => new(r.ReadString(), r.ReadUInt128(), r.ReadTimeSpan(), r.ReadTimeSpan(), r.ReadVarInt32(), r.ReadTimeSpan());
}

/// <summary>One live-read handle's pin advance.</summary>
internal readonly record struct PinAdvance(ulong Handle, ulong PinIndex);

/// <summary>Renews a session, its listed handles and reader pins, and records live-read pin advances (PROTOCOL.md §6).</summary>
internal sealed record KeepAliveCommand(ulong Session, ImmutableArray<ulong> Handles, ImmutableArray<PinAdvance> PinUpdates, ImmutableArray<ulong> ReaderPins)
    : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.KeepAlive;

    public override DfsOperationKind OperationKind => DfsOperationKind.KeepAlive;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteVarUInt(Session);
        CommandCodec.WriteUInt64s(ref w, Handles);
        w.WriteCount(PinUpdates.Length);
        foreach (PinAdvance p in PinUpdates)
        {
            w.WriteVarUInt(p.Handle);
            w.WriteVarUInt(p.PinIndex);
        }

        CommandCodec.WriteUInt64s(ref w, ReaderPins);
    }

    internal static KeepAliveCommand ReadBody(ref WireReader r)
    {
        ulong session = r.ReadVarUInt();
        ImmutableArray<ulong> handles = CommandCodec.ReadUInt64s(ref r);
        int count = r.ReadCount(2);
        var updates = ImmutableArray.CreateBuilder<PinAdvance>(count);
        for (int i = 0; i < count; i++)
            updates.Add(new PinAdvance(r.ReadVarUInt(), r.ReadVarUInt()));
        return new KeepAliveCommand(session, handles, updates.MoveToImmutable(), CommandCodec.ReadUInt64s(ref r));
    }
}

/// <summary>
/// Expires sessions the leader saw no keep-alive from within their timeout (ARCHITECTURE §7): releases their handles, pins
/// and staging sessions, aborts their transactions; and fully retires expired sessions whose outcome retention elapsed by
/// <see cref="VolumeCommand.LeaderTime"/>.
/// </summary>
internal sealed record ExpireSessionsCommand(ImmutableArray<ulong> Sessions) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.ExpireSessions;

    public override DfsOperationKind OperationKind => DfsOperationKind.KeepAlive;

    internal override void WriteBody(ref WireWriter w) => CommandCodec.WriteUInt64s(ref w, Sessions);

    internal static ExpireSessionsCommand ReadBody(ref WireReader r) => new(CommandCodec.ReadUInt64s(ref r));
}

/// <summary>Expires handles whose lease the leader saw not renewed within the lease duration (ARCHITECTURE §5.3).</summary>
internal sealed record ExpireHandlesCommand(ImmutableArray<ulong> Handles) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.ExpireHandles;

    public override DfsOperationKind OperationKind => DfsOperationKind.KeepAlive;

    internal override void WriteBody(ref WireWriter w) => CommandCodec.WriteUInt64s(ref w, Handles);

    internal static ExpireHandlesCommand ReadBody(ref WireReader r) => new(CommandCodec.ReadUInt64s(ref r));
}

/// <summary>Records a definite aborted outcome for an operation id that has none yet (OUT-013).</summary>
internal sealed record FenceOperationCommand(DfsOperationId Target, bool IsTransaction) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.FenceOperation;

    public override DfsOperationKind OperationKind => DfsOperationKind.QueryOperation;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteOperationId(Target);
        w.WriteBool(IsTransaction);
    }

    internal static FenceOperationCommand ReadBody(ref WireReader r) => new(r.ReadOperationId(), r.ReadBool());
}

/// <summary>Reserves a block of operation sequences in a session (PROTOCOL.md §9.3).</summary>
internal sealed record ReserveOperationIdsCommand(ulong Session, int Count) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.ReserveOperationIds;

    public override DfsOperationKind OperationKind => DfsOperationKind.OpenSession;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteVarUInt(Session);
        w.WriteVarInt(Count);
    }

    internal static ReserveOperationIdsCommand ReadBody(ref WireReader r) => new(r.ReadVarUInt(), r.ReadVarInt32());
}

// ---------------------------------------------------------------------------------------------------------------------
// Namespace (100-199)
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>Creates a directory (recursive and idempotent options, PROTOCOL.md §9.4).</summary>
internal sealed record CreateDirectoryCommand(string DirectoryPath, bool Recursive, string? SnapshotName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.CreateDirectory;

    public override string? Path => DirectoryPath;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(DirectoryPath);
        w.WriteBool(Recursive);
        w.WriteOptionalString(SnapshotName);
    }

    internal static CreateDirectoryCommand ReadBody(ref WireReader r) => new(r.ReadString(), r.ReadBool(), r.ReadOptionalString());
}

/// <summary>Atomically renames a file or directory (NS-05/06).</summary>
internal sealed record MoveCommand(string Source, string Destination, bool Overwrite, bool ExpectDirectory, string? SnapshotName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.Move;

    public override string? Path => Source;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(Source);
        w.WriteString(Destination);
        w.WriteBool(Overwrite);
        w.WriteBool(ExpectDirectory);
        w.WriteOptionalString(SnapshotName);
    }

    internal static MoveCommand ReadBody(ref WireReader r) => new(r.ReadString(), r.ReadString(), r.ReadBool(), r.ReadBool(), r.ReadOptionalString());
}

/// <summary>Deletes a file, a named stream or a directory (recursively: the subtree is detached atomically, NS-07).</summary>
internal sealed record DeleteCommand(string TargetPath, bool Recursive, bool ExpectDirectory, string? SnapshotName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.Delete;

    public override string? Path => TargetPath;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(TargetPath);
        w.WriteBool(Recursive);
        w.WriteBool(ExpectDirectory);
        w.WriteOptionalString(SnapshotName);
    }

    internal static DeleteCommand ReadBody(ref WireReader r) => new(r.ReadString(), r.ReadBool(), r.ReadBool(), r.ReadOptionalString());
}

// ---------------------------------------------------------------------------------------------------------------------
// Handles and publication (200-299)
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>
/// Opens a handle (ARCHITECTURE §5.3) in one entry: path resolution, permission check, mode semantics (create/truncate),
/// sharing in both directions, writer exclusivity, writer generation, lease and pin.
/// </summary>
internal sealed record OpenHandleCommand(string FilePath, FileMode Mode, FileAccess Access, FileShare Share, FileOptions Options, bool VersionPinned,
    string? StoragePolicy, ulong Session, TimeSpan LeaseDuration) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.OpenHandle;

    public override string? Path => FilePath;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(FilePath);
        w.WriteVarInt((int)Mode);
        w.WriteVarInt((int)Access);
        w.WriteVarInt((int)Share);
        w.WriteVarInt((int)Options);
        w.WriteBool(VersionPinned);
        w.WriteOptionalString(StoragePolicy);
        w.WriteVarUInt(Session);
        w.WriteTimeSpan(LeaseDuration);
    }

    internal static OpenHandleCommand ReadBody(ref WireReader r)
        => new(r.ReadString(), (FileMode)r.ReadVarInt32(), (FileAccess)r.ReadVarInt32(), (FileShare)r.ReadVarInt32(), (FileOptions)r.ReadVarInt32(),
            r.ReadBool(), r.ReadOptionalString(), r.ReadVarUInt(), r.ReadTimeSpan());
}

/// <summary>Closes a handle: releases its sharing reservation, writer authority, pin and bound staging session (idempotent).</summary>
internal sealed record CloseHandleCommand(ulong Handle, ulong Generation) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.CloseHandle;

    public override DfsOperationKind OperationKind => DfsOperationKind.CloseHandle;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteVarUInt(Handle);
        w.WriteVarUInt(Generation);
    }

    internal static CloseHandleCommand ReadBody(ref WireReader r) => new(r.ReadVarUInt(), r.ReadVarUInt());
}

/// <summary>Opens a staging session bound to a handle, a transaction or the caller's session (ARCHITECTURE §5.4).</summary>
internal sealed record CreateStagingCommand(ulong Session, ulong Handle, DfsOperationId Transaction) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.CreateStaging;

    public override DfsOperationKind OperationKind => DfsOperationKind.CreateStaging;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteVarUInt(Session);
        w.WriteVarUInt(Handle);
        w.WriteOperationId(Transaction);
    }

    internal static CreateStagingCommand ReadBody(ref WireReader r) => new(r.ReadVarUInt(), r.ReadVarUInt(), r.ReadOperationId());
}

/// <summary>Closes staging sessions (leader housekeeping): their unreferenced objects become garbage.</summary>
internal sealed record CloseStagingCommand(ImmutableArray<ulong> Staging) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.CloseStaging;

    public override DfsOperationKind OperationKind => DfsOperationKind.Gc;

    internal override void WriteBody(ref WireWriter w) => CommandCodec.WriteUInt64s(ref w, Staging);

    internal static CloseStagingCommand ReadBody(ref WireReader r) => new(CommandCodec.ReadUInt64s(ref r));
}

/// <summary>
/// One change of a publication in its validated, apply-ready form: <see cref="RangeChangeKind.Data"/> maps a range to an
/// object (<see cref="Extent"/> carries the object binding, placement epoch and the receipt holders), <see cref="RangeChangeKind.Zero"/>
/// punches a hole, <see cref="RangeChangeKind.Inline"/> carries bytes.
/// </summary>
internal sealed record StreamChange(long Offset, long Length, RangeChangeKind ChangeKind, ExtentRef? Extent, ReadOnlyMemory<byte> Inline)
{
    internal void Write(ref WireWriter w)
    {
        w.WriteVarInt(Offset);
        w.WriteVarInt(Length);
        w.WriteByte((byte)ChangeKind);
        w.WriteBool(Extent is not null);
        if (Extent is not null)
            StateCodec.WriteExtent(ref w, Extent);
        w.WriteBytes(Inline.Span);
    }

    internal static StreamChange Read(ref WireReader r)
    {
        long offset = r.ReadVarInt();
        long length = r.ReadVarInt();
        var kind = (RangeChangeKind)r.ReadByte();
        ExtentRef? extent = r.ReadBool() ? StateCodec.ReadExtent(ref r) : null;
        return new StreamChange(offset, length, kind, extent, r.ReadBytesCopy());
    }
}

/// <summary>
/// The validated content description shared by <c>CommitFile</c>, <c>WriteAll</c> and <c>Append</c>: changes in order,
/// truncation watermark, final length, the representation chosen by the leader (inline per LAY-002) and, when the leader
/// materialized content from the base revision, that revision (apply refuses when the base changed meanwhile).
/// </summary>
/// <param name="TruncateTo">Clip the base at this length before applying the changes (-1 = no truncation).</param>
/// <param name="NewLength">Length after publication (for relative appends: the appended length).</param>
/// <param name="Changes">Changes in order (later ones override earlier ones).</param>
/// <param name="FinalInline">True when the result is stored inline.</param>
/// <param name="DerivedFromRevision">Stream revision the leader derived materialized content from (0 = none).</param>
/// <param name="Staging">Staging session the referenced objects were uploaded under (must be open at apply).</param>
/// <param name="LeaderStaging">Leader-created staging session closed by this entry (0 = none).</param>
internal sealed record PublishSpec(long TruncateTo, long NewLength, ImmutableArray<StreamChange> Changes, bool FinalInline, ulong DerivedFromRevision,
    ulong Staging, ulong LeaderStaging)
{
    /// <summary>
    /// Name of the storage policy the leader validated the receipts and chose the representation against (LAY-002); apply
    /// refuses the publication (WriterConflict, retryable) when the target inode is under another policy by then. Null =
    /// not checked.
    /// </summary>
    public string? Policy { get; init; }

    internal void Write(ref WireWriter w)
    {
        w.WriteVarInt(TruncateTo);
        w.WriteVarInt(NewLength);
        w.WriteCount(Changes.Length);
        foreach (StreamChange c in Changes)
            c.Write(ref w);
        w.WriteBool(FinalInline);
        w.WriteVarUInt(DerivedFromRevision);
        w.WriteVarUInt(Staging);
        w.WriteVarUInt(LeaderStaging);
        w.WriteOptionalString(Policy);
    }

    internal static PublishSpec Read(ref WireReader r)
    {
        long truncate = r.ReadVarInt();
        long length = r.ReadVarInt();
        int count = r.ReadCount(4);
        var changes = ImmutableArray.CreateBuilder<StreamChange>(count);
        for (int i = 0; i < count; i++)
            changes.Add(StreamChange.Read(ref r));
        return new PublishSpec(truncate, length, changes.MoveToImmutable(), r.ReadBool(), r.ReadVarUInt(), r.ReadVarUInt(), r.ReadVarUInt())
        {
            Policy = r.ReadOptionalString(),
        };
    }

    /// <summary>Objects referenced by data changes.</summary>
    public IEnumerable<ObjectId> Objects() => Changes.Where(c => c.Extent is not null).Select(c => c.Extent!.Object).Distinct();
}

/// <summary>Publishes one complete revision of a stream through a writable handle (FSD §10.3, LOCK-04).</summary>
internal sealed record CommitFileCommand(ulong Handle, ulong Generation, ulong BaseRevision, PublishSpec Spec, string? SnapshotName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.CommitFile;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteVarUInt(Handle);
        w.WriteVarUInt(Generation);
        w.WriteVarUInt(BaseRevision);
        Spec.Write(ref w);
        w.WriteOptionalString(SnapshotName);
    }

    internal static CommitFileCommand ReadBody(ref WireReader r)
        => new(r.ReadVarUInt(), r.ReadVarUInt(), r.ReadVarUInt(), PublishSpec.Read(ref r), r.ReadOptionalString());
}

/// <summary>Atomically replaces a stream's contents by path, creating the file if absent and preserving the inode (FSD §5.4).</summary>
internal sealed record WriteAllCommand(string TargetPath, PublishSpec Spec, string? StoragePolicy, string? SnapshotName, bool CreateParents,
    ulong ExpectedRevision) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.WriteAll;

    public override string? Path => TargetPath;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(TargetPath);
        Spec.Write(ref w);
        w.WriteOptionalString(StoragePolicy);
        w.WriteOptionalString(SnapshotName);
        w.WriteBool(CreateParents);
        w.WriteVarUInt(ExpectedRevision);
    }

    internal static WriteAllCommand ReadBody(ref WireReader r)
        => new(r.ReadString(), PublishSpec.Read(ref r), r.ReadOptionalString(), r.ReadOptionalString(), r.ReadBool(), r.ReadVarUInt());
}

/// <summary>
/// Atomically appends a suffix (FSD §5.4, OUT-006). With <see cref="Absolute"/> false the changes are relative to the end of
/// file at apply time; with true the leader materialized the whole result (offsets absolute, base revision checked).
/// </summary>
internal sealed record AppendCommand(string TargetPath, ulong Handle, ulong Generation, PublishSpec Spec, bool Absolute, bool CreateIfMissing,
    string? StoragePolicy, string? SnapshotName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.Append;

    public override string? Path => TargetPath;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(TargetPath);
        w.WriteVarUInt(Handle);
        w.WriteVarUInt(Generation);
        Spec.Write(ref w);
        w.WriteBool(Absolute);
        w.WriteBool(CreateIfMissing);
        w.WriteOptionalString(StoragePolicy);
        w.WriteOptionalString(SnapshotName);
    }

    internal static AppendCommand ReadBody(ref WireReader r)
        => new(r.ReadString(), r.ReadVarUInt(), r.ReadVarUInt(), PublishSpec.Read(ref r), r.ReadBool(), r.ReadBool(), r.ReadOptionalString(),
            r.ReadOptionalString());
}

/// <summary>Metadata-only copy of a file's current revision (all streams; objects shared) into a destination (FSD §5.4, ADS-04).</summary>
internal sealed record CopyCommand(string Source, string Destination, bool Overwrite, ulong ExpectedSourceRevision, string? StoragePolicy,
    string? SnapshotName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.Copy;

    public override string? Path => Source;

    /// <summary>
    /// Inline source streams the leader stored as objects because the destination's policy may not be satisfied by inline
    /// metadata (LAY-002); applied only while the source stream is still at the materialized revision.
    /// </summary>
    public ImmutableArray<StreamMaterialization> Materialized { get; init; } = [];

    /// <summary>Leader-created staging session of <see cref="Materialized"/> (closed by this entry); 0 = none.</summary>
    public ulong LeaderStaging { get; init; }

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(Source);
        w.WriteString(Destination);
        w.WriteBool(Overwrite);
        w.WriteVarUInt(ExpectedSourceRevision);
        w.WriteOptionalString(StoragePolicy);
        w.WriteOptionalString(SnapshotName);
        StreamMaterialization.WriteAll(ref w, Materialized);
        w.WriteVarUInt(LeaderStaging);
    }

    internal static CopyCommand ReadBody(ref WireReader r)
        => new CopyCommand(r.ReadString(), r.ReadString(), r.ReadBool(), r.ReadVarUInt(), r.ReadOptionalString(), r.ReadOptionalString())
        {
            Materialized = StreamMaterialization.ReadAll(ref r),
            LeaderStaging = r.ReadVarUInt(),
        };
}

/// <summary>
/// An inline stream the leader stored as objects because the policy that will govern it is stronger than what inline
/// metadata guarantees (LAY-002): the source of a copy, or a stream whose inode gets a new policy.
/// </summary>
/// <param name="Stream">Stream name.</param>
/// <param name="SourceRevision">Revision of the inline stream the objects were made from.</param>
/// <param name="Extents">Extents covering the whole stream (leader staging session).</param>
internal sealed record StreamMaterialization(string Stream, ulong SourceRevision, ImmutableList<ExtentRef> Extents)
{
    internal static void WriteAll(ref WireWriter w, ImmutableArray<StreamMaterialization> materialized)
    {
        ImmutableArray<StreamMaterialization> items = materialized.IsDefault ? [] : materialized;
        w.WriteCount(items.Length);
        foreach (StreamMaterialization m in items)
        {
            w.WriteString(m.Stream);
            w.WriteVarUInt(m.SourceRevision);
            w.WriteCount(m.Extents.Count);
            foreach (ExtentRef e in m.Extents)
                StateCodec.WriteExtent(ref w, e);
        }
    }

    internal static ImmutableArray<StreamMaterialization> ReadAll(ref WireReader r)
    {
        int count = r.ReadCount(3);
        var materialized = ImmutableArray.CreateBuilder<StreamMaterialization>(count);
        for (int i = 0; i < count; i++)
        {
            string stream = r.ReadString();
            ulong revision = r.ReadVarUInt();
            int extentCount = r.ReadCount(20);
            var extents = ImmutableList.CreateBuilder<ExtentRef>();
            for (int j = 0; j < extentCount; j++)
                extents.Add(StateCodec.ReadExtent(ref r));
            materialized.Add(new StreamMaterialization(stream, revision, extents.ToImmutable()));
        }

        return materialized.MoveToImmutable();
    }
}

/// <summary>Removes detached inodes that no handle references any more (ARCHITECTURE §8).</summary>
internal sealed record ReclaimDetachedCommand(ImmutableArray<ulong> Inodes) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.ReclaimDetached;

    public override DfsOperationKind OperationKind => DfsOperationKind.Gc;

    internal override void WriteBody(ref WireWriter w) => CommandCodec.WriteUInt64s(ref w, Inodes);

    internal static ReclaimDetachedCommand ReadBody(ref WireReader r) => new(CommandCodec.ReadUInt64s(ref r));
}

// ---------------------------------------------------------------------------------------------------------------------
// Transactions (300-399) — next phase: commands and state are defined; apply answers Unsupported.
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>Begins a transaction (ARCHITECTURE §5.6). Next phase.</summary>
internal sealed record TxBeginCommand(ulong Session, DateTimeOffset DeadlineUtc) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.TxBegin;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteVarUInt(Session);
        w.WriteDateTimeOffset(DeadlineUtc);
    }

    internal static TxBeginCommand ReadBody(ref WireReader r) => new(r.ReadVarUInt(), r.ReadDateTimeOffset());
}

/// <summary>Commits a transaction with its read set and ordered writes (ARCHITECTURE §5.6). Next phase: the descriptor is opaque.</summary>
internal sealed record TxCommitCommand(DfsOperationId Transaction, ReadOnlyMemory<byte> Descriptor, string? SnapshotName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.TxCommit;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteOperationId(Transaction);
        w.WriteBytes(Descriptor.Span);
        w.WriteOptionalString(SnapshotName);
    }

    internal static TxCommitCommand ReadBody(ref WireReader r) => new(r.ReadOperationId(), r.ReadBytesCopy(), r.ReadOptionalString());
}

/// <summary>Aborts a transaction (never reverses a commit). Next phase.</summary>
internal sealed record TxAbortCommand(DfsOperationId Transaction) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.TxAbort;

    internal override void WriteBody(ref WireWriter w) => w.WriteOperationId(Transaction);

    internal static TxAbortCommand ReadBody(ref WireReader r) => new(r.ReadOperationId());
}

/// <summary>Takes writer reservations for transaction writes (LOCK-019). Next phase.</summary>
internal sealed record TxAcquireReservationCommand(DfsOperationId Transaction, ImmutableArray<StreamKey> Streams) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.TxAcquireReservation;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteOperationId(Transaction);
        w.WriteCount(Streams.Length);
        foreach (StreamKey key in Streams)
        {
            w.WriteVarUInt(key.Inode);
            w.WriteString(key.Stream);
        }
    }

    internal static TxAcquireReservationCommand ReadBody(ref WireReader r)
    {
        DfsOperationId tx = r.ReadOperationId();
        int count = r.ReadCount(2);
        var keys = ImmutableArray.CreateBuilder<StreamKey>(count);
        for (int i = 0; i < count; i++)
            keys.Add(new StreamKey(r.ReadVarUInt(), r.ReadString()));
        return new TxAcquireReservationCommand(tx, keys.MoveToImmutable());
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Snapshots and pins (400-499)
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>Creates a named snapshot capturing the current root (O(1), structural sharing; FSD §11).</summary>
internal sealed record CreateSnapshotCommand(string Name, DfsConsistencyLabel Label, ImmutableArray<AdapterRecord> Adapters, DateTimeOffset? ExpiresUtc,
    bool Hold) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.CreateSnapshot;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(Name);
        w.WriteByte((byte)Label);
        w.WriteCount(Adapters.Length);
        foreach (AdapterRecord a in Adapters)
            StateCodec.WriteAdapter(ref w, a);
        w.WriteOptionalDateTimeOffset(ExpiresUtc);
        w.WriteBool(Hold);
    }

    internal static CreateSnapshotCommand ReadBody(ref WireReader r)
    {
        string name = r.ReadString();
        var label = (DfsConsistencyLabel)r.ReadByte();
        int count = r.ReadCount(3);
        var adapters = ImmutableArray.CreateBuilder<AdapterRecord>(count);
        for (int i = 0; i < count; i++)
            adapters.Add(StateCodec.ReadAdapter(ref r));
        return new CreateSnapshotCommand(name, label, adapters.MoveToImmutable(), r.ReadOptionalDateTimeOffset(), r.ReadBool());
    }
}

/// <summary>Removes a snapshot's retention reference (FSD §11.1).</summary>
internal sealed record DeleteSnapshotCommand(string Name) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.DeleteSnapshot;

    internal override void WriteBody(ref WireWriter w) => w.WriteString(Name);

    internal static DeleteSnapshotCommand ReadBody(ref WireReader r) => new(r.ReadString());
}

/// <summary>Changes a snapshot's retention (shortening requires RetentionAdmin).</summary>
internal sealed record SetSnapshotRetentionCommand(string Name, DateTimeOffset? ExpiresUtc, bool Hold) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.SetSnapshotRetention;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(Name);
        w.WriteOptionalDateTimeOffset(ExpiresUtc);
        w.WriteBool(Hold);
    }

    internal static SetSnapshotRetentionCommand ReadBody(ref WireReader r) => new(r.ReadString(), r.ReadOptionalDateTimeOffset(), r.ReadBool());
}

/// <summary>Acquires a reader, enumeration or transfer pin (ARCHITECTURE §5.1).</summary>
internal sealed record AcquirePinCommand(PinKind PinKind, ulong Session, string? Snapshot, ulong Directory, string Binding, string? Target) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.AcquirePin;

    public override DfsOperationKind OperationKind => PinKind == PinKind.Enumeration ? DfsOperationKind.Enumerate : DfsOperationKind.OpenSnapshot;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteByte((byte)PinKind);
        w.WriteVarUInt(Session);
        w.WriteOptionalString(Snapshot);
        w.WriteVarUInt(Directory);
        w.WriteString(Binding);
        w.WriteOptionalString(Target);
    }

    internal static AcquirePinCommand ReadBody(ref WireReader r)
        => new((PinKind)r.ReadByte(), r.ReadVarUInt(), r.ReadOptionalString(), r.ReadVarUInt(), r.ReadString(), r.ReadOptionalString());
}

/// <summary>Releases a reader, enumeration or transfer pin owned by the caller's session (idempotent).</summary>
internal sealed record ReleasePinCommand(ulong Pin) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.ReleasePin;

    public override DfsOperationKind OperationKind => DfsOperationKind.OpenSnapshot;

    internal override void WriteBody(ref WireWriter w) => w.WriteVarUInt(Pin);

    internal static ReleasePinCommand ReadBody(ref WireReader r) => new(r.ReadVarUInt());
}

/// <summary>Expires reader and enumeration pins the leader saw not renewed within their lifetime.</summary>
internal sealed record ExpirePinsCommand(ImmutableArray<ulong> Pins) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.ExpirePins;

    public override DfsOperationKind OperationKind => DfsOperationKind.Gc;

    internal override void WriteBody(ref WireWriter w) => CommandCodec.WriteUInt64s(ref w, Pins);

    internal static ExpirePinsCommand ReadBody(ref WireReader r) => new(CommandCodec.ReadUInt64s(ref r));
}

// ---------------------------------------------------------------------------------------------------------------------
// GC and restore (500-599)
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>
/// Drops dereference-log entries and closed staging records below the GC watermark (ARCHITECTURE §8). Apply recomputes the
/// watermark and never prunes above it.
/// </summary>
internal sealed record PruneDerefLogCommand(ulong Below) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.PruneDerefLog;

    public override DfsOperationKind OperationKind => DfsOperationKind.Gc;

    internal override void WriteBody(ref WireWriter w) => w.WriteVarUInt(Below);

    internal static PruneDerefLogCommand ReadBody(ref WireReader r) => new(r.ReadVarUInt());
}

/// <summary>
/// Stages inodes of a snapshot being restored into this new volume (installed atomically by <see cref="RestoreCompleteCommand"/>).
/// The first chunk of a restore (<paramref name="Staging"/> = 0) opens it and allocates the restore staging session.
/// </summary>
internal sealed record RestoreChunkCommand(string Source, ulong Staging, ImmutableArray<InodeRecord> Inodes) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.RestoreChunk;

    public override DfsOperationKind OperationKind => DfsOperationKind.RestoreSnapshot;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(Source);
        w.WriteVarUInt(Staging);
        w.WriteCount(Inodes.Length);
        foreach (InodeRecord inode in Inodes)
            StateCodec.WriteInode(ref w, inode);
    }

    internal static RestoreChunkCommand ReadBody(ref WireReader r)
    {
        string source = r.ReadString();
        ulong staging = r.ReadVarUInt();
        int count = r.ReadCount(8);
        var inodes = ImmutableArray.CreateBuilder<InodeRecord>(count);
        for (int i = 0; i < count; i++)
            inodes.Add(StateCodec.ReadInode(ref r));
        return new RestoreChunkCommand(source, staging, inodes.MoveToImmutable());
    }
}

/// <summary>Installs the staged restore root as the volume's namespace in one entry.</summary>
internal sealed record RestoreCompleteCommand(string Source, int InodeCount) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.RestoreComplete;

    public override DfsOperationKind OperationKind => DfsOperationKind.RestoreSnapshot;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(Source);
        w.WriteVarInt(InodeCount);
    }

    internal static RestoreCompleteCommand ReadBody(ref WireReader r) => new(r.ReadString(), r.ReadVarInt32());
}

/// <summary>
/// Accepts a <c>RestoreSnapshot</c> of this volume (source side): records a <see cref="RestoreIntentRecord"/> for
/// <paramref name="Operation"/> holding the snapshot root. The command's own header carries no operation id (the outcome is
/// recorded later by <see cref="RestoreFinishCommand"/>), but apply runs the operation gate for <paramref name="Operation"/>
/// so an id with a stored outcome is never accepted again. Requires the VolumeAdmin grant and cluster administration.
/// </summary>
internal sealed record RestoreBeginCommand(DfsOperationId Operation, UInt128 PayloadHash, string Snapshot, string NewVolumeName, string? StoragePool)
    : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.RestoreBegin;

    public override DfsOperationKind OperationKind => DfsOperationKind.RestoreSnapshot;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteOperationId(Operation);
        w.WriteUInt128(PayloadHash);
        w.WriteString(Snapshot);
        w.WriteString(NewVolumeName);
        w.WriteOptionalString(StoragePool);
    }

    internal static RestoreBeginCommand ReadBody(ref WireReader r)
        => new(r.ReadOperationId(), r.ReadUInt128(), r.ReadString(), r.ReadString(), r.ReadOptionalString());
}

/// <summary>
/// Records the outcome of an accepted restore under its operation id (through the operation gate) and removes its intent:
/// on success <paramref name="Reply"/> is the encoded <c>RestoreSnapshotResult</c>; otherwise <paramref name="Code"/> and
/// <paramref name="Message"/> describe the definite failure.
/// </summary>
internal sealed record RestoreFinishCommand(DfsOperationId Operation, DfsErrorCode Code, string Message, ReadOnlyMemory<byte> Reply) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.RestoreFinish;

    public override DfsOperationKind OperationKind => DfsOperationKind.RestoreSnapshot;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteOperationId(Operation);
        w.WriteEnum(Code);
        w.WriteString(Message);
        w.WriteBytes(Reply.Span);
    }

    internal static RestoreFinishCommand ReadBody(ref WireReader r)
        => new(r.ReadOperationId(), r.ReadEnumRaw<DfsErrorCode>(), r.ReadString(), r.ReadBytesCopy());
}

// ---------------------------------------------------------------------------------------------------------------------
// Backup (600-699)
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>Records a target's acknowledged backup progress (ARCHITECTURE §12).</summary>
internal sealed record RecordBackupProgressCommand(BackupProgressRecord Progress) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.RecordBackupProgress;

    public override DfsOperationKind OperationKind => DfsOperationKind.BackupPublish;

    internal override void WriteBody(ref WireWriter w) => StateCodec.WriteBackupProgress(ref w, Progress);

    internal static RecordBackupProgressCommand ReadBody(ref WireReader r) => new(StateCodec.ReadBackupProgress(ref r));
}

/// <summary>Abandons a target's incremental lineage: releases its transfer pins and marks it for reseeding (FSD §12.4).</summary>
internal sealed record AbandonTransferLineageCommand(string Target, Guid NewLineage) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.AbandonTransferLineage;

    public override DfsOperationKind OperationKind => DfsOperationKind.BackupTransfer;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(Target);
        w.WriteGuid(NewLineage);
    }

    internal static AbandonTransferLineageCommand ReadBody(ref WireReader r) => new(r.ReadString(), r.ReadGuid());
}

// ---------------------------------------------------------------------------------------------------------------------
// Policy, security and keys (700-799)
// ---------------------------------------------------------------------------------------------------------------------

/// <summary>Selects a named storage policy for an inode (FSD §9.1; empty name = the volume default).</summary>
internal sealed record SetPolicyCommand(string TargetPath, string PolicyName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.SetPolicy;

    public override string? Path => TargetPath;

    /// <summary>Inline streams of the inode the leader stored as objects for the new policy (LAY-002).</summary>
    public ImmutableArray<StreamMaterialization> Materialized { get; init; } = [];

    /// <summary>Leader-created staging session of <see cref="Materialized"/> (closed by this entry); 0 = none.</summary>
    public ulong LeaderStaging { get; init; }

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(TargetPath);
        w.WriteString(PolicyName);
        StreamMaterialization.WriteAll(ref w, Materialized);
        w.WriteVarUInt(LeaderStaging);
    }

    internal static SetPolicyCommand ReadBody(ref WireReader r)
        => new SetPolicyCommand(r.ReadString(), r.ReadString()) { Materialized = StreamMaterialization.ReadAll(ref r), LeaderStaging = r.ReadVarUInt() };
}

/// <summary>How a <see cref="SetAclCommand"/> describes the new explicit entries.</summary>
internal enum AclChangeKind : byte
{
    /// <summary>The supplied explicit entries and protection flag (SetAccessControl).</summary>
    Descriptor = 0,

    /// <summary>The <c>SetAccess</c> shortcut (DfsSecurityDescriptor.FromAccessFlags with the inode's owner).</summary>
    AccessFlags = 1,
}

/// <summary>Replaces an inode's explicit entries and protection flag (ACL-07); bumps the inode revision.</summary>
internal sealed record SetAclCommand(string TargetPath, AclChangeKind Change, DfsSecurityDescriptor? Descriptor, bool OwnerOnly, bool ReadOnlyForOthers)
    : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.SetAcl;

    public override string? Path => TargetPath;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(TargetPath);
        w.WriteByte((byte)Change);
        w.WriteOptionalSecurityDescriptor(Descriptor);
        w.WriteBool(OwnerOnly);
        w.WriteBool(ReadOnlyForOthers);
    }

    internal static SetAclCommand ReadBody(ref WireReader r)
        => new(r.ReadString(), (AclChangeKind)r.ReadByte(), r.ReadOptionalSecurityDescriptor(), r.ReadBool(), r.ReadBool());
}

/// <summary>Changes an inode's owner (ACL-05, ACL-014); bumps the inode revision.</summary>
internal sealed record SetOwnerCommand(string TargetPath, DfsIdentity Owner) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.SetOwner;

    public override string? Path => TargetPath;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(TargetPath);
        w.WriteIdentity(Owner);
    }

    internal static SetOwnerCommand ReadBody(ref WireReader r) => new(r.ReadString(), r.ReadIdentity());
}

/// <summary>Records a new data-key version of the volume (key rotation, ARCHITECTURE §11).</summary>
internal sealed record RotateDataKeyCommand(uint Version) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.RotateDataKey;

    public override DfsOperationKind OperationKind => DfsOperationKind.KeyRotation;

    internal override void WriteBody(ref WireWriter w) => w.WriteVarUInt(Version);

    internal static RotateDataKeyCommand ReadBody(ref WireReader r) => new(r.ReadVarUInt32());
}

/// <summary>Records the default policy being migrated to (null = migration complete).</summary>
internal sealed record MigratePolicyCommand(DfsStoragePolicy? Target) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.MigratePolicy;

    public override DfsOperationKind OperationKind => DfsOperationKind.PolicyChange;

    internal override void WriteBody(ref WireWriter w) => w.WriteOptionalStoragePolicy(Target);

    internal static MigratePolicyCommand ReadBody(ref WireReader r) => new(r.ReadOptionalStoragePolicy());
}
