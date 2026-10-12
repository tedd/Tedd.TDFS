using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>
/// A handle on a distributed file (FSD §6). Writes are staged privately and published atomically by
/// <see cref="Flush"/>/<see cref="FlushAsync(CancellationToken)"/>/<see cref="CommitAsync(CancellationToken)"/>
/// (or per call with <see cref="DfsWriteMode.WriteThrough"/>). Overlapping operations are rejected with
/// <see cref="InvalidOperationException"/>. <see cref="Stream.CanTimeout"/> is false; deadlines come from the client
/// options.
/// </summary>
public sealed class DfsStream : Stream
{
    private Exception? _fault;

    internal DfsStream(
        string path,
        InodeId inodeId,
        ulong openRevision,
        DfsReadView readView,
        DfsWriteMode writeMode,
        FileAccess access,
        FileShare share,
        bool isAppend,
        bool isTransactionBound,
        ulong writerGeneration)
    {
        ArgumentNullException.ThrowIfNull(path);
        Path = path;
        InodeId = inodeId;
        OpenRevision = openRevision;
        ReadView = readView;
        WriteMode = writeMode;
        Access = access;
        Share = share;
        IsAppend = isAppend;
        IsTransactionBound = isTransactionBound;
        WriterGeneration = writerGeneration;
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

    /// <summary>True when the stream is faulted (lease lost, unknown publication outcome, failed write).</summary>
    public bool IsFaulted => Volatile.Read(ref _fault) is not null;

    /// <summary>The fault that disabled the stream, if any; further writes rethrow it.</summary>
    public Exception? Fault => Volatile.Read(ref _fault);

    /// <summary>Staged, unpublished bytes (buffered plus uploaded).</summary>
    public long PendingBytes => throw new NotImplementedException("DfsStream.PendingBytes: implemented in a later phase");

    /// <inheritdoc/>
    public override bool CanRead => throw new NotImplementedException("DfsStream.CanRead: implemented in a later phase");

    /// <inheritdoc/>
    public override bool CanWrite => throw new NotImplementedException("DfsStream.CanWrite: implemented in a later phase");

    /// <inheritdoc/>
    public override bool CanSeek => throw new NotImplementedException("DfsStream.CanSeek: implemented in a later phase");

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
    public override long Length => throw new NotImplementedException("DfsStream.Length: implemented in a later phase");

    /// <summary>The handle's local position.</summary>
    public override long Position
    {
        get => throw new NotImplementedException("DfsStream.Position: implemented in a later phase");
        set => throw new NotImplementedException("DfsStream.Position: implemented in a later phase");
    }

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count) =>
        throw new NotImplementedException("DfsStream.Read: implemented in a later phase");

    /// <inheritdoc/>
    public override int Read(Span<byte> buffer) =>
        throw new NotImplementedException("DfsStream.Read: implemented in a later phase");

    /// <inheritdoc/>
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        throw new NotImplementedException("DfsStream.ReadAsync: implemented in a later phase");

    /// <inheritdoc/>
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException("DfsStream.ReadAsync: implemented in a later phase");

    /// <inheritdoc/>
    public override int ReadByte() =>
        throw new NotImplementedException("DfsStream.ReadByte: implemented in a later phase");

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count) =>
        throw new NotImplementedException("DfsStream.Write: implemented in a later phase");

    /// <inheritdoc/>
    public override void Write(ReadOnlySpan<byte> buffer) =>
        throw new NotImplementedException("DfsStream.Write: implemented in a later phase");

    /// <inheritdoc/>
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        throw new NotImplementedException("DfsStream.WriteAsync: implemented in a later phase");

    /// <inheritdoc/>
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException("DfsStream.WriteAsync: implemented in a later phase");

    /// <inheritdoc/>
    public override void WriteByte(byte value) =>
        throw new NotImplementedException("DfsStream.WriteByte: implemented in a later phase");

    /// <summary>Moves the local position; seeking beyond the end does not extend the file. Not supported on append handles.</summary>
    public override long Seek(long offset, SeekOrigin origin) =>
        throw new NotImplementedException("DfsStream.Seek: implemented in a later phase");

    /// <summary>Changes the staged length (published at the next flush, or immediately with write-through). Does not move <see cref="Position"/>.</summary>
    public override void SetLength(long value) =>
        throw new NotImplementedException("DfsStream.SetLength: implemented in a later phase");

    /// <summary>Atomically publishes all pending changes as one durable revision (no-op for read-only handles).</summary>
    public override void Flush() =>
        throw new NotImplementedException("DfsStream.Flush: implemented in a later phase");

    /// <summary>Atomically publishes all pending changes as one durable revision (no-op for read-only handles).</summary>
    public override Task FlushAsync(CancellationToken cancellationToken) =>
        throw new NotImplementedException("DfsStream.FlushAsync: implemented in a later phase");

    /// <summary>Copies with base <see cref="Stream"/> semantics (a composition of reads and writes, not an atomic copy).</summary>
    public override void CopyTo(Stream destination, int bufferSize) => base.CopyTo(destination, bufferSize);

    /// <summary>Copies with base <see cref="Stream"/> semantics (a composition of reads and writes, not an atomic copy).</summary>
    public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken) =>
        base.CopyToAsync(destination, bufferSize, cancellationToken);

    /// <summary>Returns the length of the handle's view without synchronous property I/O.</summary>
    public ValueTask<long> GetLengthAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsStream.GetLengthAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetLengthAsync"/>.</summary>
    public long GetLength() =>
        SyncBridge.RunValueTask(() => GetLengthAsync(CancellationToken.None));

    /// <summary>Reads the metadata of the handle's inode in the handle's view.</summary>
    public ValueTask<DfsFileInfo> GetInfoAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsStream.GetInfoAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetInfoAsync"/>.</summary>
    public DfsFileInfo GetInfo() =>
        SyncBridge.RunValueTask(() => GetInfoAsync(CancellationToken.None));

    /// <summary>Asynchronous counterpart of <see cref="SetLength"/>.</summary>
    public Task SetLengthAsync(long length, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsStream.SetLengthAsync: implemented in a later phase");

    /// <summary>Publishes pending changes like <see cref="FlushAsync(CancellationToken)"/> and returns the commit token.</summary>
    /// <exception cref="InvalidOperationException">The stream is transaction-bound.</exception>
    public Task<CommitToken> CommitAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsStream.CommitAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CommitAsync(CancellationToken)"/>.</summary>
    public CommitToken Commit() =>
        SyncBridge.Run(() => CommitAsync(CancellationToken.None));

    /// <summary>Publishes pending changes with options (e.g. commit-and-snapshot in the same decision).</summary>
    /// <exception cref="InvalidOperationException">The stream is transaction-bound.</exception>
    public Task<CommitToken> CommitAsync(DfsMutationOptions options, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsStream.CommitAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CommitAsync(DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Commit(DfsMutationOptions options) =>
        SyncBridge.Run(() => CommitAsync(options, CancellationToken.None));

    /// <summary>Discards staged changes and releases the writer reservation; does not undo earlier flushes.</summary>
    public Task AbortAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsStream.AbortAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="AbortAsync"/>.</summary>
    public void Abort() =>
        SyncBridge.Run(() => AbortAsync(CancellationToken.None));

    /// <summary>Flushes pending changes (surfacing failures), then closes the handle. Never commits from a finalizer.</summary>
    protected override void Dispose(bool disposing) =>
        throw new NotImplementedException("DfsStream.Dispose: implemented in a later phase");

    /// <summary>Flushes pending changes (surfacing failures), then closes the handle.</summary>
    public override ValueTask DisposeAsync() =>
        throw new NotImplementedException("DfsStream.DisposeAsync: implemented in a later phase");

    /// <summary>Faults the stream; the first fault wins.</summary>
    internal void SetFault(Exception fault)
    {
        ArgumentNullException.ThrowIfNull(fault);
        Interlocked.CompareExchange(ref _fault, fault, null);
    }
}
