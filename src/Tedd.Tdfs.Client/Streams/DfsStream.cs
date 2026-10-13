using System.Buffers;
using System.Runtime.ExceptionServices;
using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Data;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Client.Internal.Overlay;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>
/// A handle on a distributed file (FSD §6). Writes are staged privately and published atomically by
/// <see cref="Flush"/>/<see cref="FlushAsync(CancellationToken)"/>/<see cref="CommitAsync(CancellationToken)"/>/disposal
/// (<see cref="DfsWriteMode.OnFlush"/>) or by every call (<see cref="DfsWriteMode.WriteThrough"/>). Reads observe one
/// revision per call (live handles may observe newer revisions on later calls; version-pinned handles always read the
/// revision current at open). A stream does not support overlapping calls: a call made while another is in progress
/// fails with <see cref="InvalidOperationException"/> and does not disturb the call in progress.
/// </summary>
public sealed class DfsStream : Stream, ILeaseHolder
{
    private const int MaxReadRequest = 4 << 20;

    private readonly AsyncGate _gate = new(nameof(DfsStream));
    private readonly VolumeContext _volume;
    private readonly ClientRuntime _runtime;
    private readonly DfsTransaction? _transaction;
    private readonly HandleId _handle;
    private readonly bool _canRead;
    private readonly bool _canWrite;
    private readonly int _bufferSize;
    private readonly StagedOverlay? _overlay;
    private readonly string _policyName;
    private readonly ulong _snapshotPin;
    private readonly VolumeBinding _binding;
    private StagingLease? _staging;
    private long _position;
    private long _knownLength;
    private CommitToken _lastToken;
    private int _disposed;
    private int _released;
    private Exception? _fault;

    internal DfsStream(VolumeContext volume, DfsTransaction? transaction, HandleId handle, FileInfoData info, string path, DfsReadView readView,
        DfsWriteMode writeMode, FileAccess access, FileShare share, bool isAppend, ulong writerGeneration, int bufferSize, CommitToken openToken,
        ulong snapshotPin, VolumeBinding binding)
    {
        ArgumentNullException.ThrowIfNull(volume);
        ArgumentNullException.ThrowIfNull(info);
        ArgumentNullException.ThrowIfNull(path);
        _volume = volume;
        _runtime = volume.Runtime;
        _transaction = transaction;
        _handle = handle;
        _binding = binding;
        Path = path;
        InodeId = info.Inode;
        OpenRevision = info.Revision;
        ReadView = readView;
        WriteMode = writeMode;
        Access = access;
        Share = share;
        IsAppend = isAppend;
        IsTransactionBound = transaction is not null;
        WriterGeneration = writerGeneration;
        _canRead = (access & FileAccess.Read) != 0;
        _canWrite = (access & FileAccess.Write) != 0;
        _bufferSize = bufferSize;
        _policyName = info.StoragePolicy;
        _knownLength = info.Length;
        _lastToken = openToken;
        _snapshotPin = snapshotPin;
        if (snapshotPin != 0)
            _released = 1;
        if (_canWrite)
            _overlay = new StagedOverlay(info.Length);
        if (isAppend)
            _position = info.Length;
    }

    /// <summary>Path at open time (informational; the handle follows the inode across renames).</summary>
    public string Path { get; }

    /// <summary>Inode the handle is bound to.</summary>
    public InodeId InodeId { get; }

    /// <summary>Inode revision observed when the handle was opened.</summary>
    public ulong OpenRevision { get; }

    /// <summary>Read view of the handle.</summary>
    public DfsReadView ReadView { get; }

    /// <summary>Publication mode of the handle.</summary>
    public DfsWriteMode WriteMode { get; }

    /// <summary>Granted access.</summary>
    public FileAccess Access { get; }

    /// <summary>Sharing granted to other handles.</summary>
    public FileShare Share { get; }

    /// <summary>True for append handles (writes go to the logical end; not seekable).</summary>
    public bool IsAppend { get; }

    /// <summary>True when the handle belongs to a transaction (changes publish at transaction commit).</summary>
    public bool IsTransactionBound { get; }

    /// <summary>Fencing generation of the writer reservation; 0 for read-only handles.</summary>
    public ulong WriterGeneration { get; }

    /// <summary>True when the stream is faulted (lease lost, unknown publication outcome, expired pin).</summary>
    public bool IsFaulted => Volatile.Read(ref _fault) is not null;

    /// <summary>The fault that disabled the stream, if any; further I/O rethrows it.</summary>
    public Exception? Fault => Volatile.Read(ref _fault);

    /// <summary>Staged, unpublished bytes (buffered plus uploaded).</summary>
    public long PendingBytes => _overlay?.PendingBytes ?? 0;

    /// <inheritdoc />
    public override bool CanRead => !IsDisposed && _canRead;

    /// <inheritdoc />
    public override bool CanWrite => !IsDisposed && _canWrite;

    /// <inheritdoc />
    public override bool CanSeek => !IsDisposed && !IsAppend;

    /// <summary>Always false: deadlines come from <see cref="DfsClientOptions.OperationTimeout"/>.</summary>
    public override bool CanTimeout => false;

    /// <summary>Not supported; throws <see cref="InvalidOperationException"/>.</summary>
    public override int ReadTimeout
    {
        get => throw new InvalidOperationException("DfsStream does not support timeouts; use DfsClientOptions.OperationTimeout or a CancellationToken.");
        set => throw new InvalidOperationException("DfsStream does not support timeouts; use DfsClientOptions.OperationTimeout or a CancellationToken.");
    }

    /// <summary>Not supported; throws <see cref="InvalidOperationException"/>.</summary>
    public override int WriteTimeout
    {
        get => throw new InvalidOperationException("DfsStream does not support timeouts; use DfsClientOptions.OperationTimeout or a CancellationToken.");
        set => throw new InvalidOperationException("DfsStream does not support timeouts; use DfsClientOptions.OperationTimeout or a CancellationToken.");
    }

    /// <summary>Length of the handle's view (live, pinned or staged); may perform network I/O.</summary>
    public override long Length
    {
        get
        {
            ThrowIfDisposed();
            return _overlay is not null ? _overlay.Length : GetLength();
        }
    }

    /// <summary>The handle's local position.</summary>
    public override long Position
    {
        get
        {
            ThrowIfDisposed();
            return Volatile.Read(ref _position);
        }
        set
        {
            using AsyncGate.Scope gate = _gate.Enter("Position");
            if (IsAppend)
                throw new NotSupportedException("An append stream is not seekable: writes always target the logical end of the file.");
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _position = value;
        }
    }

    private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    // ------------------------------------------------------------------------------------------------ reads

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        return SyncBridge.Run(() => ReadAsync(buffer, offset, count, CancellationToken.None));
    }

    /// <inheritdoc />
    public override int Read(Span<byte> buffer)
    {
        int length = buffer.Length;
        byte[] temp = ArrayPool<byte>.Shared.Rent(Math.Max(1, length));
        try
        {
            int read = SyncBridge.Run(() => ReadAsync(temp.AsMemory(0, length), CancellationToken.None).AsTask());
            temp.AsSpan(0, read).CopyTo(buffer);
            return read;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(temp);
        }
    }

    /// <inheritdoc />
    public override int ReadByte()
    {
        var one = new byte[1];
        return Read(one, 0, 1) == 1 ? one[0] : -1;
    }

    /// <inheritdoc />
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    /// <inheritdoc />
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        AsyncGate.Scope gate = _gate.Enter("ReadAsync");
        try
        {
            if (!_canRead)
                throw new NotSupportedException("The stream was opened without read access.");
            ThrowIfFaulted();
            cancellationToken.ThrowIfCancellationRequested();
            if (buffer.IsEmpty)
            {
                gate.Dispose();
                return ValueTask.FromResult(0);
            }
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return new ValueTask<int>(ReadGatedAsync(gate, buffer, cancellationToken));
    }

    /// <inheritdoc />
    public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state) =>
        TaskToAsyncResult.Begin(ReadAsync(buffer, offset, count, CancellationToken.None), callback, state);

    /// <inheritdoc />
    public override int EndRead(IAsyncResult asyncResult) => TaskToAsyncResult.End<int>(asyncResult);

    private async Task<int> ReadGatedAsync(AsyncGate.Scope gate, Memory<byte> destination, CancellationToken ct)
    {
        using (gate)
        {
            using DfsTransaction.TransactionOperation? transaction = _transaction?.Enter();
            using OperationScope scope = Begin(DfsOperationKind.Read, ct);
            await scope.HitAsync(FaultStage.F00, FaultSites.ClientOperationStart).ConfigureAwait(false);
            await scope.HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
            scope.ThrowIfCancelled();
            return await ReadCoreAsync(destination, scope).ConfigureAwait(false);
        }
    }

    /// <summary>Reads at the current position into <paramref name="destination"/> (written only on success) and advances.</summary>
    private async Task<int> ReadCoreAsync(Memory<byte> destination, OperationScope scope)
    {
        int want = Math.Min(destination.Length, MaxReadRequest);
        long position = _position;

        // No stream reaches long.MaxValue: a range that would end beyond it is clipped, never wrapped (PATH-013).
        if (position > long.MaxValue - want)
            want = (int)(long.MaxValue - position);
        if (want == 0)
            return 0;
        byte[] temp;
        int got;
        try
        {
            if (_overlay is not null)
            {
                (temp, got) = await ReadStagedAsync(position, want, scope).ConfigureAwait(false);
            }
            else
            {
                temp = new byte[want];
                got = await ReadRevisionAsync(position, temp, scope).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw Classify(ex, publication: false);
        }

        temp.AsSpan(0, got).CopyTo(destination.Span);
        _position = position + got;
        return got;
    }

    private async Task<(byte[] Buffer, int Count)> ReadStagedAsync(long position, int want, OperationScope scope)
    {
        StagedOverlay overlay = _overlay!;
        if (position >= overlay.Length)
            return ([], 0);
        List<StagedPiece> pieces = overlay.Map(position, want);
        int total = 0;
        foreach (StagedPiece piece in pieces)
            total += piece.Length;
        var temp = new byte[total];
        int filled = 0;
        StagedReadGrant? grant = null;
        for (int index = 0; index < pieces.Count; index++)
        {
            StagedPiece piece = pieces[index];
            Memory<byte> target = temp.AsMemory(filled, piece.Length);
            switch (piece.Source)
            {
                case StagedSource.Buffered:
                {
                    StagedSegment segment = piece.Segment!;
                    segment.Bytes.Slice((int)(piece.Offset - segment.Offset), piece.Length).CopyTo(target);
                    break;
                }

                case StagedSource.Uploaded:
                    grant = await ReadUploadedAsync(pieces, index, target, grant, scope).ConfigureAwait(false);
                    break;

                case StagedSource.Base:
                {
                    int done = 0;
                    while (done < piece.Length)
                    {
                        int n = await ReadRevisionAsync(piece.Offset + done, target[done..], scope).ConfigureAwait(false);
                        if (n <= 0)
                            break;
                        done += n;
                    }

                    if (done < piece.Length)
                    {
                        // A short base result: return the verified prefix (a short read is valid, STR-004).
                        return (temp, filled + done);
                    }

                    break;
                }

                case StagedSource.Zero:
                    break;
            }

            filled += piece.Length;
        }

        return (temp, filled);
    }

    /// <summary>A read capability covering some of this writer's uploaded, unpublished objects (one read call).</summary>
    private sealed record StagedReadGrant(ReadOnlyMemory<byte> Capability, HashSet<ObjectId> Objects);

    /// <summary>
    /// Reads the bytes of <paramref name="pieces"/>[<paramref name="index"/>], a range of an object this writer uploaded early
    /// and has not published (bounded memory, FSD §6.3/§15). Staging capabilities never authorize <c>GetObject</c>: the leader
    /// grants read access through <c>ReadRange</c> with the staged-objects extension after verifying the receipts name
    /// uploads of this handle's open staging session (PROTOCOL.md §8.1). Returns the grant to reuse for later pieces.
    /// </summary>
    private async Task<StagedReadGrant> ReadUploadedAsync(List<StagedPiece> pieces, int index, Memory<byte> target, StagedReadGrant? grant,
        OperationScope scope)
    {
        StagedPiece piece = pieces[index];
        StagedSegment segment = piece.Segment!;
        UploadedObject uploaded = segment.Object ?? throw new InvalidOperationException("An uploaded staged range has no object.");
        long objectOffset = segment.ObjectOffset + (piece.Offset - segment.Offset);
        NodeId[] locations = uploaded.Receipts.Select(r => r.Node).Distinct().ToArray();
        ulong placementEpoch = uploaded.Receipts.Count == 0 ? 0 : uploaded.Receipts.Max(r => r.PlacementEpoch);
        for (int attempt = 0; ; attempt++)
        {
            if (grant is null || !grant.Objects.Contains(uploaded.Id))
                grant = await GrantStagedReadAsync(pieces, index, scope).ConfigureAwait(false);
            try
            {
                await _runtime.Data.ReadObjectAsync(scope, _volume, grant.Capability, uploaded.Id, uploaded.Length, uploaded.Checksum, ObjectEncodings.Plain,
                    placementEpoch, locations, objectOffset, target).ConfigureAwait(false);
                return grant;
            }
            catch (DataCapabilityRejectedException) when (attempt == 0)
            {
                // The capability expired between the grant and the fetch: ask again.
                grant = null;
            }
            catch (DataCapabilityRejectedException ex)
            {
                throw new UnauthorizedAccessException("Storage nodes rejected the read capability issued for this stream's staged data.", ex);
            }
        }
    }

    /// <summary>
    /// Asks the leader for read access to the uploaded objects among <paramref name="pieces"/> from <paramref name="first"/> on
    /// (one receipt per object proves the upload; at most <see cref="ReadRangeMessage.MaxStagedObjects"/> per request).
    /// </summary>
    private async Task<StagedReadGrant> GrantStagedReadAsync(List<StagedPiece> pieces, int first, OperationScope scope)
    {
        var receipts = new List<ObjectReceipt>();
        var objects = new HashSet<ObjectId>();
        for (int i = first; i < pieces.Count && receipts.Count < ReadRangeMessage.MaxStagedObjects; i++)
        {
            if (pieces[i] is { Source: StagedSource.Uploaded, Segment.Object: { } uploaded } && uploaded.Receipts.Count > 0 && objects.Add(uploaded.Id))
                receipts.Add(uploaded.Receipts[0]);
        }

        if (receipts.Count == 0)
            throw new InvalidOperationException("An uploaded staged range has no receipt.");
        ReadRangeResultMessage reply = await _volume.ReadAsync<ReadRangeResultMessage>(scope, "ReadRange(staged)",
            header => new ReadRangeMessage(header, _handle, 0, default, 0, 0).WithStagedReceipts(receipts), Path, _transaction?.Id,
            requireSession: true, binding: _binding).ConfigureAwait(false);
        if (reply.ReadCapability.IsEmpty)
            throw new DfsProtocolException("The leader granted no read capability for this stream's staged data.");
        return new StagedReadGrant(reply.ReadCapability, objects);
    }

    /// <summary>
    /// Reads published bytes through the handle (one <c>ReadRange</c>, one revision) into the start of
    /// <paramref name="destination"/>; returns the bytes produced (0 = end of file).
    /// </summary>
    private async Task<int> ReadRevisionAsync(long position, Memory<byte> destination, OperationScope scope)
    {
        for (int attempt = 0; ; attempt++)
        {
            int requested = destination.Length;
            ReadRangeResultMessage range = await _volume.ReadAsync<ReadRangeResultMessage>(scope, "ReadRange",
                header => new ReadRangeMessage(header, _handle, _snapshotPin, _snapshotPin != 0 ? InodeId : default, position, requested), Path,
                _transaction?.Id,
                requireSession: true, binding: _binding).ConfigureAwait(false);
            if (_overlay is null)
            {
                if (ReadView == DfsReadView.Live)
                    _knownLength = range.FileLength;
                if (ReadView == DfsReadView.Live && !IsTransactionBound)
                    _volume.ReportPinAdvance(_handle, range.Revision);
            }

            if (range.Length <= 0)
            {
                if (position < range.FileLength)
                    throw new DfsProtocolException($"ReadRange returned no bytes at {position} of a {range.FileLength}-byte file.");
                return 0;
            }

            if (range.Offset != position || range.Length > requested)
                throw new DfsProtocolException($"ReadRange covered [{range.Offset}, +{range.Length}) for a request at {position} of {requested} bytes.");
            int count = (int)range.Length;
            Memory<byte> target = destination[..count];
            target.Span.Clear();
            try
            {
                await FillAsync(range, position, target, scope).ConfigureAwait(false);
                return count;
            }
            catch (DataCapabilityRejectedException) when (attempt == 0)
            {
                // The read capability expired between the reply and the object fetch: ask again.
            }
            catch (DataCapabilityRejectedException ex)
            {
                throw new UnauthorizedAccessException("Storage nodes rejected the read capability issued for this file.", ex);
            }
        }
    }

    private async Task FillAsync(ReadRangeResultMessage range, long position, Memory<byte> target, OperationScope scope)
    {
        if (range.IsInline)
        {
            if (range.InlineData.Length < target.Length)
                throw new DfsProtocolException($"An inline ReadRange result carries {range.InlineData.Length} bytes for a {target.Length}-byte range.");
            range.InlineData[..target.Length].CopyTo(target);
            return;
        }

        // One fetch per object per call: a fragmented map (small overwrites splitting an older object) maps several ranges of
        // the call to the same object, and storage nodes answer whole verification units, so fetching each piece on its own
        // would move the same units again for every fragment (read amplification, PERF-002/PERF-009).
        long end = position + target.Length;
        var groups = new List<ObjectReadGroup>();
        foreach (ExtentData extent in range.Extents)
        {
            long start = Math.Max(position, extent.Offset);
            long stop = Math.Min(end, extent.Offset + extent.Length);
            if (stop <= start)
                continue;
            var piece = new ObjectReadPiece(extent.ObjectOffset + (start - extent.Offset), (int)(start - position), (int)(stop - start));
            ObjectReadGroup? group = groups.Find(g => g.Extent.Object == extent.Object);
            if (group is null)
                groups.Add(group = new ObjectReadGroup(extent));
            group.Pieces.Add(piece);
        }

        foreach (ObjectReadGroup group in groups)
        {
            ExtentData extent = group.Extent;
            long from = group.Pieces.Min(p => p.ObjectOffset);
            long to = group.Pieces.Max(p => p.ObjectOffset + p.Length);
            long useful = group.Pieces.Sum(p => (long)p.Length);
            if (group.Pieces.Count == 1 || to - from > useful + MaxCoalescedReadGap)
            {
                foreach (ObjectReadPiece piece in group.Pieces)
                {
                    await _runtime.Data.ReadObjectAsync(scope, _volume, range.ReadCapability, extent.Object, extent.ObjectLength, extent.Checksum,
                        extent.Encoding, extent.PlacementEpoch, extent.Locations, piece.ObjectOffset, target.Slice(piece.TargetOffset, piece.Length))
                        .ConfigureAwait(false);
                }

                continue;
            }

            var covering = new byte[to - from];
            await _runtime.Data.ReadObjectAsync(scope, _volume, range.ReadCapability, extent.Object, extent.ObjectLength, extent.Checksum, extent.Encoding,
                extent.PlacementEpoch, extent.Locations, from, covering).ConfigureAwait(false);
            foreach (ObjectReadPiece piece in group.Pieces)
                covering.AsMemory((int)(piece.ObjectOffset - from), piece.Length).CopyTo(target.Slice(piece.TargetOffset, piece.Length));
        }
    }

    /// <summary>Most bytes between the pieces of one object fetched along with them instead of fetching the pieces separately.</summary>
    private const long MaxCoalescedReadGap = 128 * 1024;

    /// <summary>A range of one object needed by a read call (object offset, offset in the call's destination, length).</summary>
    private readonly record struct ObjectReadPiece(long ObjectOffset, int TargetOffset, int Length);

    /// <summary>The pieces of one object needed by a read call (fetched once when they lie close together).</summary>
    private sealed record ObjectReadGroup(ExtentData Extent)
    {
        public List<ObjectReadPiece> Pieces { get; } = [];
    }

    // ------------------------------------------------------------------------------------------------ writes

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        byte[] owned = buffer.AsSpan(offset, count).ToArray();
        SyncBridge.Run(() => WriteOwnedAsync(owned, "Write", CancellationToken.None));
    }

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        byte[] owned = buffer.ToArray();
        SyncBridge.Run(() => WriteOwnedAsync(owned, "Write", CancellationToken.None));
    }

    /// <inheritdoc />
    public override void WriteByte(byte value)
    {
        byte[] owned = [value];
        SyncBridge.Run(() => WriteOwnedAsync(owned, "WriteByte", CancellationToken.None));
    }

    /// <inheritdoc />
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ValidateBufferArguments(buffer, offset, count);
        byte[] owned = buffer.AsSpan(offset, count).ToArray();
        return WriteOwnedAsync(owned, "WriteAsync", cancellationToken);
    }

    /// <inheritdoc />
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        // The caller's buffer is copied before anything else happens (STR-027): later changes to it never reach the file.
        byte[] owned = buffer.ToArray();
        return new ValueTask(WriteOwnedAsync(owned, "WriteAsync", cancellationToken));
    }

    /// <inheritdoc />
    public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state) =>
        TaskToAsyncResult.Begin(WriteAsync(buffer, offset, count, CancellationToken.None), callback, state);

    /// <inheritdoc />
    public override void EndWrite(IAsyncResult asyncResult) => TaskToAsyncResult.End(asyncResult);

    private Task WriteOwnedAsync(byte[] owned, string operation, CancellationToken ct)
    {
        AsyncGate.Scope gate = _gate.Enter(operation);
        try
        {
            if (!_canWrite)
                throw new NotSupportedException("The stream was opened without write access.");
            ThrowIfFaulted();
            ct.ThrowIfCancellationRequested();
            if (owned.Length == 0)
            {
                gate.Dispose();
                return Task.CompletedTask;
            }

            long offset = IsAppend ? _overlay!.Length : _position;
            long maximum = _volume.MaxFileLength;
            if (offset > maximum - owned.Length)
                throw new DfsLimitExceededException(nameof(DfsVolumeLimits.MaxFileLength), maximum, offset + owned.Length);
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return WriteGatedAsync(gate, owned, ct);
    }

    private async Task WriteGatedAsync(AsyncGate.Scope gate, byte[] owned, CancellationToken ct)
    {
        using (gate)
        {
            // Transaction-bound writes cannot overlap the transaction's commit (TX-017).
            using DfsTransaction.TransactionOperation? transaction = _transaction?.Enter();
            StagedOverlay overlay = _overlay!;
            long offset = IsAppend ? overlay.Length : _position;
            StagedOverlay.Snapshot before = overlay.Capture();
            long positionBefore = _position;

            // Staged synchronously (before any await): the bytes now belong to the stream.
            overlay.WriteOwned(offset, owned);
            _position = offset + owned.Length;
            try
            {
                if (WriteMode == DfsWriteMode.WriteThrough)
                {
                    using OperationScope scope = Begin(DfsOperationKind.CommitFile, ct);
                    await scope.HitAsync(FaultStage.F00, FaultSites.ClientOperationStart).ConfigureAwait(false);
                    await scope.HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
                    scope.ThrowIfCancelled();
                    await PublishAsync(scope, null).ConfigureAwait(false);
                }
                else if (NeedsSpill())
                {
                    using OperationScope scope = Begin(DfsOperationKind.PutObject, ct);
                    await SpillAsync(scope).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Exception classified = Classify(ex, publication: WriteMode == DfsWriteMode.WriteThrough);
                if (!IsFaulted)
                {
                    // The failed call leaves no partial effect (PUB-022): the staged view is exactly as before it.
                    overlay.Restore(before);
                    _position = positionBefore;
                }

                throw classified;
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ seek / length

    /// <summary>Moves the local position; seeking beyond the end does not extend the file. Not supported on append handles.</summary>
    public override long Seek(long offset, SeekOrigin origin)
    {
        using AsyncGate.Scope gate = _gate.Enter("Seek");
        if (IsAppend)
            throw new NotSupportedException("An append stream is not seekable: writes always target the logical end of the file.");
        long basis = origin switch
        {
            SeekOrigin.Begin => 0,
            SeekOrigin.Current => _position,
            SeekOrigin.End => _overlay?.Length ?? CurrentLengthForSeek(),
            _ => throw new ArgumentException($"Invalid seek origin {origin}.", nameof(origin)),
        };
        long target;
        try
        {
            target = checked(basis + offset);
        }
        catch (OverflowException)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "The resulting position overflows.");
        }

        if (target < 0)
            throw new IOException("An attempt was made to move the position before the beginning of the stream.");
        _position = target;
        return target;
    }

    private long CurrentLengthForSeek()
    {
        if (ReadView == DfsReadView.VersionPinned)
            return _knownLength;
        ThrowIfFaulted();
        return SyncBridge.Run(async () =>
        {
            using OperationScope scope = Begin(DfsOperationKind.GetInfo, CancellationToken.None);
            return await CurrentLengthAsync(scope).ConfigureAwait(false);
        });
    }

    /// <summary>Changes the staged length (published at the next flush, or immediately with write-through). Does not move <see cref="Position"/>.</summary>
    public override void SetLength(long value) => SyncBridge.Run(() => SetLengthAsync(value, CancellationToken.None));

    /// <summary>Asynchronous counterpart of <see cref="SetLength"/>.</summary>
    public Task SetLengthAsync(long length, CancellationToken ct = default)
    {
        AsyncGate.Scope gate = _gate.Enter("SetLength");
        try
        {
            if (IsAppend)
                throw new NotSupportedException("An append stream cannot change its length.");
            if (!_canWrite)
                throw new NotSupportedException("The stream was opened without write access.");
            ArgumentOutOfRangeException.ThrowIfNegative(length);
            if (length > _volume.MaxFileLength)
                throw new DfsLimitExceededException(nameof(DfsVolumeLimits.MaxFileLength), _volume.MaxFileLength, length);
            ThrowIfFaulted();
            ct.ThrowIfCancellationRequested();
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return SetLengthGatedAsync(gate, length, ct);
    }

    private async Task SetLengthGatedAsync(AsyncGate.Scope gate, long length, CancellationToken ct)
    {
        using (gate)
        {
            using DfsTransaction.TransactionOperation? transaction = _transaction?.Enter();
            StagedOverlay overlay = _overlay!;
            StagedOverlay.Snapshot before = overlay.Capture();
            overlay.SetLength(length);
            if (WriteMode != DfsWriteMode.WriteThrough)
                return;
            try
            {
                using OperationScope scope = Begin(DfsOperationKind.CommitFile, ct);
                await scope.HitAsync(FaultStage.F00, FaultSites.ClientOperationStart).ConfigureAwait(false);
                await scope.HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
                scope.ThrowIfCancelled();
                await PublishAsync(scope, null).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Exception classified = Classify(ex, publication: true);
                if (!IsFaulted)
                    overlay.Restore(before);
                throw classified;
            }
        }
    }

    /// <summary>Returns the length of the handle's view (live: current; pinned: at open; writer: staged).</summary>
    public ValueTask<long> GetLengthAsync(CancellationToken ct = default)
    {
        AsyncGate.Scope gate = _gate.Enter("GetLengthAsync");
        try
        {
            ThrowIfFaulted();
            ct.ThrowIfCancellationRequested();
            _transaction?.ThrowIfNotActive();
            if (_overlay is not null || ReadView == DfsReadView.VersionPinned)
            {
                long length = _overlay?.Length ?? _knownLength;
                gate.Dispose();
                return ValueTask.FromResult(length);
            }
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return new ValueTask<long>(GetLengthGatedAsync(gate, ct));
    }

    private async Task<long> GetLengthGatedAsync(AsyncGate.Scope gate, CancellationToken ct)
    {
        using (gate)
        {
            using DfsTransaction.TransactionOperation? transaction = _transaction?.Enter();
            using OperationScope scope = Begin(DfsOperationKind.GetInfo, ct);
            return await CurrentLengthAsync(scope).ConfigureAwait(false);
        }
    }

    private async Task<long> CurrentLengthAsync(OperationScope scope)
    {
        GetInfoResultMessage result = await GetHandleInfoAsync(scope).ConfigureAwait(false);
        return result.Info!.Length;
    }

    /// <summary>Synchronous counterpart of <see cref="GetLengthAsync"/>.</summary>
    public long GetLength() => SyncBridge.RunValueTask(() => GetLengthAsync(CancellationToken.None));

    /// <summary>Metadata of the file as seen through this handle (a writer reports its staged length).</summary>
    public ValueTask<DfsFileInfo> GetInfoAsync(CancellationToken ct = default)
    {
        AsyncGate.Scope gate = _gate.Enter("GetInfoAsync");
        try
        {
            ThrowIfFaulted();
            ct.ThrowIfCancellationRequested();
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return new ValueTask<DfsFileInfo>(GetInfoGatedAsync(gate, ct));
    }

    private async Task<DfsFileInfo> GetInfoGatedAsync(AsyncGate.Scope gate, CancellationToken ct)
    {
        using (gate)
        {
            using DfsTransaction.TransactionOperation? transaction = _transaction?.Enter();
            using OperationScope scope = Begin(DfsOperationKind.GetInfo, ct);
            await scope.HitAsync(FaultStage.F00, FaultSites.ClientOperationStart).ConfigureAwait(false);
            await scope.HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
            scope.ThrowIfCancelled();
            GetInfoResultMessage result = await GetHandleInfoAsync(scope).ConfigureAwait(false);
            FileInfoData info = result.Info!;
            long? length = _overlay?.Length ?? (ReadView == DfsReadView.VersionPinned ? _knownLength : null);
            return WireConvert.ToFileInfo(info, result.ObservedAt, length);
        }
    }

    /// <summary>Synchronous counterpart of <see cref="GetInfoAsync"/>.</summary>
    public DfsFileInfo GetInfo() => SyncBridge.RunValueTask(() => GetInfoAsync(CancellationToken.None));

    private async Task<GetInfoResultMessage> GetHandleInfoAsync(OperationScope scope)
    {
        try
        {
            GetInfoResultMessage result = await _volume.ReadAsync<GetInfoResultMessage>(scope, "GetInfo",
                header => _snapshotPin != 0
                    ? new GetInfoMessage(header, Path, default, _snapshotPin)
                    : new GetInfoMessage(header, string.Empty, _handle, 0), Path, _transaction?.Id, requireSession: true, binding: _binding).ConfigureAwait(false);
            if (!result.Exists || result.Info is null)
                throw new DfsProtocolException("GetInfo through an open handle reported no file.");
            if (_overlay is null && ReadView == DfsReadView.Live)
                _knownLength = result.Info.Length;
            return result;
        }
        catch (Exception ex)
        {
            throw Classify(ex, publication: false);
        }
    }

    // ------------------------------------------------------------------------------------------------ publication

    /// <summary>Atomically publishes all pending changes as one durable revision (no-op for read-only handles).</summary>
    public override void Flush() => SyncBridge.Run(() => FlushAsync(CancellationToken.None));

    /// <summary>Atomically publishes all pending changes as one durable revision (no-op for read-only handles).</summary>
    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        if (!_canWrite && IsDisposed)
            return Task.CompletedTask;
        AsyncGate.Scope gate = _gate.Enter("FlushAsync");
        try
        {
            if (!_canWrite)
            {
                gate.Dispose();
                return Task.CompletedTask;
            }

            ThrowIfFaulted();
            cancellationToken.ThrowIfCancellationRequested();
            if (!_overlay!.IsDirty)
            {
                gate.Dispose();
                return Task.CompletedTask;
            }
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return CommitGatedAsync(gate, null, cancellationToken);
    }

    /// <summary>Publishes the staged changes as one revision and returns its commit token (FSD §6.2).</summary>
    /// <exception cref="InvalidOperationException">The stream belongs to a transaction (it publishes with the transaction).</exception>
    public Task<CommitToken> CommitAsync(CancellationToken ct = default) => CommitAsync(new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="CommitAsync(CancellationToken)"/>.</summary>
    public CommitToken Commit() => SyncBridge.Run(() => CommitAsync(CancellationToken.None));

    /// <summary>Publishes the staged changes with options (operation id, commit-and-snapshot).</summary>
    public Task<CommitToken> CommitAsync(DfsMutationOptions options, CancellationToken ct = default)
    {
        AsyncGate.Scope gate = _gate.Enter("CommitAsync");
        try
        {
            ArgumentNullException.ThrowIfNull(options);
            if (IsTransactionBound)
                throw new InvalidOperationException("A transaction-bound stream publishes with its transaction; commit the transaction instead.");
            if (!_canWrite)
                throw new NotSupportedException("The stream was opened without write access.");
            Validation.MutationOptions(options, nameof(options));
            ThrowIfFaulted();
            ct.ThrowIfCancellationRequested();
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return CommitGatedAsync(gate, options, ct);
    }

    /// <summary>Synchronous counterpart of <see cref="CommitAsync(DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Commit(DfsMutationOptions options) => SyncBridge.Run(() => CommitAsync(options, CancellationToken.None));

    private async Task<CommitToken> CommitGatedAsync(AsyncGate.Scope gate, DfsMutationOptions? options, CancellationToken ct)
    {
        using (gate)
        {
            using DfsTransaction.TransactionOperation? transaction = _transaction?.Enter();
            using OperationScope scope = Begin(DfsOperationKind.CommitFile, ct);
            await scope.HitAsync(FaultStage.F00, FaultSites.ClientOperationStart).ConfigureAwait(false);
            await scope.HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
            scope.ThrowIfCancelled();
            try
            {
                return await PublishAsync(scope, options).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw Classify(ex, publication: true);
            }
        }
    }

    /// <summary>Discards the staged changes and releases the handle (and its writer reservation); published revisions stay.</summary>
    public Task AbortAsync(CancellationToken ct = default)
    {
        AsyncGate.Scope gate = _gate.Enter("AbortAsync");
        if (ct.IsCancellationRequested)
        {
            gate.Dispose();
            return Task.FromCanceled(ct);
        }

        DfsTransaction.TransactionOperation? transaction;
        try
        {
            transaction = EnterForCleanup("AbortAsync");
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return AbortGatedAsync(gate, transaction, ct);
    }

    /// <summary>Synchronous counterpart of <see cref="AbortAsync"/>.</summary>
    public void Abort() => SyncBridge.Run(() => AbortAsync(CancellationToken.None));

    private async Task AbortGatedAsync(AsyncGate.Scope gate, DfsTransaction.TransactionOperation? transaction, CancellationToken ct)
    {
        try
        {
            _overlay?.Reset(_overlay.BaseLength);
            await ReleaseHandleAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            transaction?.Dispose();
            Volatile.Write(ref _disposed, 1);
            gate.ExitAndClose();
            _gate.Close();
        }
    }

    /// <summary>
    /// Registers a cleanup call (disposal, abort) of a transaction-bound stream with its transaction, so the transaction
    /// cannot start committing (and publishing this stream's staged view) while the call discards or publishes it. A call
    /// overlapping a commit in progress is rejected with <see cref="InvalidOperationException"/> and changes nothing; after
    /// the transaction ended the call proceeds without a registration.
    /// </summary>
    private DfsTransaction.TransactionOperation? EnterForCleanup(string operation)
    {
        if (_transaction is null)
            return null;
        DfsTransaction.TransactionOperation? registration = _transaction.TryEnter(out DfsTransactionState state);
        if (registration is null && state == DfsTransactionState.Committing)
        {
            throw new InvalidOperationException(
                $"{operation} cannot run while transaction {_transaction.Id} is committing; the commit publishes this stream's staged changes.");
        }

        return registration;
    }

    /// <summary>
    /// Publishes the staged view with one <c>CommitFile</c> (inline when the result fits the inline threshold and the
    /// policy allows it, otherwise after uploading every buffered range), then makes it the new base.
    /// </summary>
    private async Task<CommitToken> PublishAsync(OperationScope scope, DfsMutationOptions? options)
    {
        StagedOverlay overlay = _overlay!;
        if (!overlay.IsDirty && options?.CommitSnapshotName is null)
            return _lastToken;

        VolumeEntry entry = await EnsureEntryAsync(scope).ConfigureAwait(false);
        long publishing = overlay.PendingBytes;
        if (publishing > _volume.MaxPublishBytes)
        {
            // Nothing is uploaded or published: the limit applies to the whole publication (PUB-024).
            throw new DfsLimitExceededException(nameof(DfsVolumeLimits.MaxPublishBytes), _volume.MaxPublishBytes, publishing);
        }

        DfsStoragePolicy policy = ContentLayout.ResolvePolicy(entry, _policyName) ?? entry.DefaultPolicy;
        long newLength = overlay.Length;
        int threshold = ContentLayout.InlineThreshold(entry, _runtime.Capabilities);
        // Objects uploaded early (spilled to bound memory) stay objects: the leader materializes them if the result is inline.
        bool inline = newLength <= threshold && overlay.Segments.All(s => s.IsBuffered)
            && ContentLayout.InlinePermitted(entry, policy, id => _runtime.Map.Node(id)?.Labels);

        var changes = new List<RangeChange>();
        var receipts = new List<ObjectReceipt>();
        var seenObjects = new HashSet<ObjectId>();
        StagingLease? staging = null;

        // Uploading replaces buffered ranges with staged objects. If the publication fails, a readable writer's staged view
        // returns to what it was (buffered bytes stay buffered), so its reads do not depend on uploads of a failed publication.
        StagedOverlay.Snapshot? readable = _canRead ? overlay.Capture() : null;
        CommitFileResultMessage result;
        try
        {
            if (inline)
            {
                foreach (StagedSegment segment in overlay.Segments)
                    changes.Add(ContentLayout.InlineChange(segment.Offset, segment.Bytes));
            }
            else if (overlay.Segments.Count > 0)
            {
                // A pure length change (truncation, sparse extension) of an extent-based file references no new object.
                staging = await EnsureStagingAsync(scope).ConfigureAwait(false);
                await UploadBufferedAsync(scope, staging, entry, policy, all: true).ConfigureAwait(false);
                foreach (StagedSegment segment in overlay.Segments)
                {
                    UploadedObject uploaded = segment.Object ?? throw new InvalidOperationException("A buffered range was not uploaded.");
                    changes.Add(new RangeChange(segment.Offset, segment.Length, RangeChangeKind.Data, uploaded.Id, segment.ObjectOffset, uploaded.Length,
                        uploaded.Checksum, ObjectEncodings.Plain, ReadOnlyMemory<byte>.Empty));
                    if (seenObjects.Add(uploaded.Id))
                        receipts.AddRange(uploaded.Receipts);
                }
            }

            long truncateTo = overlay.TruncateTo;
            StagingId stagingId = receipts.Count > 0 ? staging!.Id : default;
            result = await _volume.MutateAsync<CommitFileResultMessage>(scope, "CommitFile", options?.OperationId,
                header => new CommitFileMessage(header, _handle, WriterGeneration, 0, truncateTo, newLength, changes, receipts, stagingId,
                    options?.CommitSnapshotName), Path, _transaction?.Id, rebindable: false, binding: _binding).ConfigureAwait(false);
        }
        catch
        {
            if (readable is not null)
                overlay.Restore(readable);
            throw;
        }

        overlay.Reset(result.NewLength);
        _knownLength = result.NewLength;
        _lastToken = result.Token;
        _runtime.Emit("stream.published", $"Published revision {result.Revision} ({result.NewLength} bytes).", options?.OperationId, _transaction?.Id,
            _volume.Name, result.Token);
        return result.Token;
    }

    private async Task<VolumeEntry> EnsureEntryAsync(OperationScope scope)
    {
        if (_volume.Entry is { } entry)
            return entry;
        await _volume.RefreshRouteAsync(scope, _runtime.Rpc.DefaultAttemptTimeout(scope)).ConfigureAwait(false);
        return _volume.Entry ?? throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"Volume '{_volume.Name}' could not be located.");
    }

    private async Task<StagingLease> EnsureStagingAsync(OperationScope scope)
    {
        if (_staging is { } existing)
            return existing;
        StagingLease lease = _transaction is not null
            ? _transaction.Staging
            : await _volume.CreateHandleStagingAsync(_handle, scope, _binding).ConfigureAwait(false);
        _staging = lease;
        return lease;
    }

    /// <summary>
    /// True when buffered staged data should be uploaded before the publication (bounded memory, FSD §6.3/§15). A readable
    /// writer reads uploaded ranges back through a leader-granted read capability (<see cref="ReadUploadedAsync"/>); a
    /// readable transaction-bound stream keeps its staged bytes buffered (its staging session is the transaction's, not
    /// bound to the handle, so the leader cannot grant reads of it through the handle).
    /// </summary>
    private bool NeedsSpill()
    {
        if (_canRead && _transaction is not null)
            return false;
        StagedOverlay overlay = _overlay!;
        long bound = Math.Max(_bufferSize, ContentLayout.ChunkSize(_runtime.Capabilities));
        int inline = ContentLayout.InlineThreshold(_volume.Entry, _runtime.Capabilities);
        return overlay.Length > inline && overlay.BufferedBytes > bound;
    }

    /// <summary>Uploads buffered staged data early (bounded memory, FSD §6.2): whole chunks first, the rest when far over the bound.</summary>
    private async Task SpillAsync(OperationScope scope)
    {
        VolumeEntry entry = await EnsureEntryAsync(scope).ConfigureAwait(false);
        DfsStoragePolicy policy = ContentLayout.ResolvePolicy(entry, _policyName) ?? entry.DefaultPolicy;
        StagingLease staging = await EnsureStagingAsync(scope).ConfigureAwait(false);
        await UploadBufferedAsync(scope, staging, entry, policy, all: false).ConfigureAwait(false);
        long bound = Math.Max(_bufferSize, ContentLayout.ChunkSize(_runtime.Capabilities));
        if (_overlay!.BufferedBytes > 2 * bound)
            await UploadBufferedAsync(scope, staging, entry, policy, all: true).ConfigureAwait(false);
    }

    private async Task UploadBufferedAsync(OperationScope scope, StagingLease staging, VolumeEntry entry, DfsStoragePolicy policy, bool all)
    {
        StagedOverlay overlay = _overlay!;
        int chunk = ContentLayout.ChunkSize(_runtime.Capabilities);
        string pool = ContentLayout.PoolOf(entry, policy);
        var pieces = new List<(long Offset, ReadOnlyMemory<byte> Bytes)>();
        foreach (StagedSegment segment in overlay.Segments)
        {
            if (!segment.IsBuffered)
                continue;
            foreach ((long offset, int length) in ContentLayout.Chunks(segment.Offset, segment.End, chunk))
            {
                if (!all && length != chunk)
                    continue;
                pieces.Add((offset, segment.Bytes.Slice((int)(offset - segment.Offset), length)));
            }
        }

        // Consecutive dirty ranges smaller than a chunk are packed into one object of at most a chunk (each range maps to its
        // own object offset): a fragmented overlay costs one upload per chunk of data, not one per range (LAY-017).
        int ordinal = 0;
        for (int first = 0; first < pieces.Count;)
        {
            int last = first + 1;
            long size = pieces[first].Bytes.Length;
            while (last < pieces.Count && size + pieces[last].Bytes.Length <= chunk)
                size += pieces[last++].Bytes.Length;

            ReadOnlyMemory<byte> bytes;
            if (last == first + 1)
            {
                bytes = pieces[first].Bytes;
            }
            else
            {
                var packed = new byte[size];
                int at = 0;
                for (int i = first; i < last; i++)
                {
                    pieces[i].Bytes.Span.CopyTo(packed.AsSpan(at));
                    at += pieces[i].Bytes.Length;
                }

                bytes = packed;
            }

            (ObjectId id, UInt128 checksum) = _runtime.Data.Describe(bytes.Span);
            IReadOnlyList<ObjectReceipt> receipts = await _runtime.Data.UploadAsync(scope, _volume, staging, id, bytes, checksum, policy, pool,
                scope.OperationId, ordinal++).ConfigureAwait(false);
            var uploaded = new UploadedObject(id, checksum, bytes.Length, receipts);
            long objectOffset = 0;
            for (int i = first; i < last; i++)
            {
                overlay.ReplaceWithUploaded(pieces[i].Offset, pieces[i].Bytes.Length, uploaded, objectOffset);
                objectOffset += pieces[i].Bytes.Length;
            }

            first = last;
        }
    }

    // ------------------------------------------------------------------------------------------------ copy

    /// <summary>Copies the rest of the stream (from the current position) to <paramref name="destination"/>; not atomic.</summary>
    public override void CopyTo(Stream destination, int bufferSize)
    {
        ValidateCopyToArguments(destination, bufferSize);
        SyncBridge.Run(() => CopyToAsync(destination, bufferSize, CancellationToken.None));
    }

    /// <summary>Copies the rest of the stream (from the current position) to <paramref name="destination"/>; not atomic.</summary>
    public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
    {
        ValidateCopyToArguments(destination, bufferSize);
        AsyncGate.Scope gate = _gate.Enter("CopyToAsync");
        try
        {
            if (!_canRead)
                throw new NotSupportedException("The stream was opened without read access.");
            if (!destination.CanWrite)
                throw new NotSupportedException("The destination stream is not writable.");
            ThrowIfFaulted();
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return CopyGatedAsync(gate, destination, bufferSize, cancellationToken);
    }

    private async Task CopyGatedAsync(AsyncGate.Scope gate, Stream destination, int bufferSize, CancellationToken ct)
    {
        using (gate)
        {
            using DfsTransaction.TransactionOperation? transaction = _transaction?.Enter();
            var buffer = new byte[Math.Min(bufferSize, MaxReadRequest)];
            while (true)
            {
                int read;
                using (OperationScope scope = Begin(DfsOperationKind.Read, ct))
                {
                    await scope.HitAsync(FaultStage.F00, FaultSites.ClientOperationStart).ConfigureAwait(false);
                    await scope.HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
                    scope.ThrowIfCancelled();
                    read = await ReadCoreAsync(buffer, scope).ConfigureAwait(false);
                }

                if (read == 0)
                    return;

                // Not atomic: the source position already advanced past bytes a failing destination did not take (STR-020).
                await destination.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ disposal

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing && !IsDisposed)
            SyncBridge.RunValueTask(DisposeAsync);
        base.Dispose(disposing);
    }

    /// <summary>
    /// Publishes staged changes (into the transaction when transaction-bound), releases the handle and closes the
    /// stream. A failed publication is reported (FSD §6.2, PUB-017) and the handle is released either way. Idempotent;
    /// a call overlapping another operation is rejected with <see cref="InvalidOperationException"/> and leaves the
    /// stream open.
    /// </summary>
    public override ValueTask DisposeAsync()
    {
        if (IsDisposed)
            return ValueTask.CompletedTask;
        AsyncGate.Scope gate;
        try
        {
            gate = _gate.Enter("DisposeAsync");
        }
        catch (ObjectDisposedException)
        {
            return ValueTask.CompletedTask;
        }

        DfsTransaction.TransactionOperation? transaction;
        try
        {
            // Disposal of a transaction-bound stream cannot overlap the transaction's commit (TX-017): the commit
            // publishes this stream's staged view.
            transaction = EnterForCleanup("DisposeAsync");
        }
        catch
        {
            gate.Dispose();
            throw;
        }

        return new ValueTask(DisposeGatedAsync(gate, transaction));
    }

    private async Task DisposeGatedAsync(AsyncGate.Scope gate, DfsTransaction.TransactionOperation? transaction)
    {
        Exception? failure = null;
        try
        {
            if (_overlay is { IsDirty: true } overlay)
            {
                if (Fault is { } fault)
                {
                    failure = fault;
                }
                else if (_transaction is { } ended && transaction is null)
                {
                    // The transaction ended. An abort (the application's decision) discarded its staged changes, these
                    // included: disposal is cleanup only (FSD §8.2). Otherwise the changes were never part of it.
                    if (ended.State != DfsTransactionState.Aborted)
                        failure = new InvalidOperationException($"Transaction {ended.Id} is {ended.State}; this stream's staged changes are not part of it.");
                }
                else if (_runtime.IsDisposed)
                {
                    failure = new ObjectDisposedException(nameof(DfsClient), "The DfsClient was disposed before the stream's staged changes were published.");
                }
                else
                {
                    try
                    {
                        // Transaction-bound staged changes join the transaction (the registration keeps its commit out).
                        using OperationScope scope = Begin(DfsOperationKind.CommitFile, CancellationToken.None);
                        await scope.HitAsync(FaultStage.F00, FaultSites.ClientOperationStart).ConfigureAwait(false);
                        await scope.HitAsync(FaultStage.F01, FaultSites.ClientValidated).ConfigureAwait(false);
                        await PublishAsync(scope, null).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        failure = Classify(ex, publication: true);
                    }
                }

                overlay.Reset(overlay.BaseLength);
            }

            await ReleaseHandleAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            transaction?.Dispose();
            Volatile.Write(ref _disposed, 1);
            gate.ExitAndClose();
            _gate.Close();
        }

        if (failure is not null)
            ExceptionDispatchInfo.Throw(failure);
    }

    /// <summary>Closes the handle on the server (best effort) and stops renewing its lease.</summary>
    private async Task ReleaseHandleAsync(CancellationToken ct)
    {
        _transaction?.Unregister(this);
        if (Interlocked.Exchange(ref _released, 1) != 0)
            return;
        _volume.UnregisterHandle(_handle);
        if (_runtime.IsDisposed)
            return;
        try
        {
            using OperationScope scope = Begin(DfsOperationKind.CloseHandle, ct);
            await _volume.CloseHandleAsync(_handle, WriterGeneration, _transaction?.Id, scope, _binding).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // The leader expires the handle after its lease when the close cannot be delivered (PUB-017).
            _runtime.Emit("stream.close-failed", $"Closing handle {_handle.Value} failed ({ex.GetType().Name}); it expires with its lease.",
                volume: _volume.Name, exception: ex);
        }
    }

    // ------------------------------------------------------------------------------------------------ helper access

    /// <summary>The length of the stream's fixed view (helpers read pinned, transaction or snapshot views).</summary>
    internal long KnownLength => _overlay?.Length ?? _knownLength;

    /// <summary>Reads within an operation owned by a helper (no gate, no fault-site hits of its own).</summary>
    internal Task<int> ReadInternalAsync(Memory<byte> destination, OperationScope scope)
    {
        ThrowIfFaulted();
        return destination.IsEmpty ? Task.FromResult(0) : ReadCoreAsync(destination, scope);
    }

    /// <summary>Closes a helper's private stream (releases the handle; nothing was staged).</summary>
    internal async Task ReleaseInternalAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _gate.Close();
        await ReleaseHandleAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes this transaction-bound stream's staged changes into its transaction as part of the transaction's commit
    /// (the transaction already rejects new stream operations, so the stream's gate is not needed).
    /// </summary>
    internal async Task PublishForTransactionCommitAsync(OperationScope scope)
    {
        if (IsDisposed || _overlay is not { IsDirty: true })
            return;
        ThrowIfFaulted();
        try
        {
            await PublishAsync(scope, null).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw Classify(ex, publication: true);
        }
    }

    /// <summary>Faults the stream when the snapshot pin it reads through expired.</summary>
    internal void OnPinExpired(Exception reason) => SetFault(reason);

    // ------------------------------------------------------------------------------------------------ helpers

    void ILeaseHolder.OnLeaseLost(Exception reason) => SetFault(reason);

    internal void SetFault(Exception fault)
    {
        ArgumentNullException.ThrowIfNull(fault);
        if (Interlocked.CompareExchange(ref _fault, fault, null) is null)
            _runtime.Emit("stream.faulted", $"Stream on handle {_handle.Value} faulted: {fault.GetType().Name}.", volume: _volume.Name, exception: fault);
    }

    private void ThrowIfFaulted()
    {
        if (Fault is { } fault)
            ExceptionDispatchInfo.Throw(fault);
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(IsDisposed, this);

    /// <summary>Starts an operation; transaction-bound streams perform <see cref="DfsOperationKind.TxOp"/> operations.</summary>
    private OperationScope Begin(DfsOperationKind kind, CancellationToken ct) =>
        _runtime.BeginOperation(IsTransactionBound && kind != DfsOperationKind.CloseHandle ? DfsOperationKind.TxOp : kind, _volume.Name, Path, ct);

    /// <summary>Maps a failure to what the caller sees and faults the stream when it can no longer be used safely.</summary>
    private Exception Classify(Exception ex, bool publication)
    {
        switch (ex)
        {
            case DfsCommitOutcomeUnknownException when publication:
                // The base revision is unknown now: the stream cannot publish relative to it any more.
                SetFault(ex);
                return ex;
            case DfsException d when d.ErrorCode is DfsErrorCode.LeaseLost or DfsErrorCode.InvalidHandle or DfsErrorCode.PinExpired
                                         or DfsErrorCode.EpochMismatch or DfsErrorCode.TransactionAborted:
                SetFault(ex);
                return ex;
            default:
                return ex;
        }
    }
}
