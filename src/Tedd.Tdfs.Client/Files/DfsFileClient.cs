using System.Runtime.CompilerServices;
using System.Text;
using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Data;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Client.Internal.Text;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>
/// File operations of a volume (FSD §5): streams, atomic whole-file helpers, namespace operations and metadata. Facades
/// are thread-safe. A transaction-bound facade (<see cref="DfsTransaction.Files"/>) reads and writes the transaction's
/// private view; a snapshot view's facade is read-only.
/// </summary>
public sealed partial class DfsFileClient
{
    internal DfsFileClient(DfsVolume volume, bool isReadOnly = false, DfsTransaction? transaction = null, DfsSnapshotView? view = null)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
        IsReadOnly = isReadOnly || view is not null;
        Transaction = transaction;
        View = view;
    }

    /// <summary>The owning volume facade.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>The transaction this facade is bound to, if any.</summary>
    internal DfsTransaction? Transaction { get; }

    /// <summary>The snapshot view this facade reads, if any.</summary>
    internal DfsSnapshotView? View { get; }

    /// <summary>True for snapshot views and backup hosts (mutations are rejected).</summary>
    public bool IsReadOnly { get; }

    /// <summary>True when the facade belongs to a transaction.</summary>
    public bool IsTransactionBound => Transaction is not null;

    private VolumeContext Context => Volume.Context;

    private ClientRuntime Runtime => Volume.Runtime;

    private DfsTransactionId? TxId => Transaction?.Id;

    private ulong Pin => View?.Pin ?? 0;

    // ---- Open family ------------------------------------------------------------------------------------------

    /// <summary>Opens a stream with FileStream-like semantics (FSD §6.1).</summary>
    public async Task<DfsStream> OpenAsync(string path, DfsOpenOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Open, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.FilePath(path);
        Validation.OpenOptions(options, IsTransactionBound);
        bool writable = (options.Access & FileAccess.Write) != 0;
        if (writable)
            EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        return View is not null
            ? await OpenSnapshotStreamAsync(p, options, scope).ConfigureAwait(false)
            : await OpenHandleAsync(p, options, scope).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="OpenAsync(string, DfsOpenOptions, CancellationToken)"/>.</summary>
    public DfsStream Open(string path, DfsOpenOptions options) =>
        SyncBridge.Run(() => OpenAsync(path, options, CancellationToken.None));

    /// <summary>Opens a stream with the given mode, access and sharing.</summary>
    public Task<DfsStream> OpenAsync(string path, FileMode mode, FileAccess access, FileShare share, CancellationToken ct = default) =>
        OpenAsync(path, new DfsOpenOptions { Mode = mode, Access = access, Share = share, BufferSize = Runtime.Options.DefaultBufferSize }, ct);

    /// <summary>Synchronous counterpart of <see cref="OpenAsync(string, FileMode, FileAccess, FileShare, CancellationToken)"/>.</summary>
    public DfsStream Open(string path, FileMode mode, FileAccess access, FileShare share) =>
        SyncBridge.Run(() => OpenAsync(path, mode, access, share, CancellationToken.None));

    /// <summary>Opens an existing file for live reading (FileMode.Open, FileAccess.Read, FileShare.Read).</summary>
    public Task<DfsStream> OpenReadAsync(string path, CancellationToken ct = default) =>
        OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.Read, BufferSize = Runtime.Options.DefaultBufferSize }, ct);

    /// <summary>Synchronous counterpart of <see cref="OpenReadAsync"/>.</summary>
    public DfsStream OpenRead(string path) =>
        SyncBridge.Run(() => OpenReadAsync(path, CancellationToken.None));

    /// <summary>Opens or creates a file for writing without truncating it (FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read).</summary>
    public Task<DfsStream> OpenWriteAsync(string path, CancellationToken ct = default) =>
        OpenAsync(path, new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Share = FileShare.Read, BufferSize = Runtime.Options.DefaultBufferSize }, ct);

    /// <summary>Synchronous counterpart of <see cref="OpenWriteAsync"/>.</summary>
    public DfsStream OpenWrite(string path) =>
        SyncBridge.Run(() => OpenWriteAsync(path, CancellationToken.None));

    /// <summary>Creates or truncates a file (FileMode.Create, FileAccess.ReadWrite, FileShare.Read).</summary>
    public Task<DfsStream> CreateAsync(string path, CancellationToken ct = default) =>
        OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Create, Access = FileAccess.ReadWrite, Share = FileShare.Read, BufferSize = Runtime.Options.DefaultBufferSize }, ct);

    /// <summary>Synchronous counterpart of <see cref="CreateAsync"/>.</summary>
    public DfsStream Create(string path) =>
        SyncBridge.Run(() => CreateAsync(path, CancellationToken.None));

    private async Task<DfsStream> OpenHandleAsync(string path, DfsOpenOptions options, OperationScope scope)
    {
        bool pinned = options.ReadView == DfsReadView.VersionPinned;
        bool writeThrough = options.WriteMode == DfsWriteMode.WriteThrough;
        OpenHandleResultMessage opened = await Context.MutateAsync<OpenHandleResultMessage>(scope, "OpenHandle", options.OperationId,
            header => new OpenHandleMessage(header, path, options.Mode, options.Access, options.Share, options.Options, pinned, writeThrough, options.StoragePolicy),
            path, TxId).ConfigureAwait(false);
        var stream = new DfsStream(Context, Transaction, opened.Handle, opened.Info, path, options.ReadView, options.WriteMode, options.Access, options.Share,
            options.Mode == FileMode.Append, opened.WriterGeneration, options.BufferSize, opened.Token, snapshotPin: 0);
        if (Context.CurrentSession is { } session)
            Context.RegisterHandle(opened.Handle, session, stream);
        Transaction?.Register(stream);
        return stream;
    }

    private async Task<DfsStream> OpenSnapshotStreamAsync(string path, DfsOpenOptions options, OperationScope scope)
    {
        if (options.Mode != FileMode.Open)
            throw new DfsReadOnlyException("A snapshot view is read-only: only FileMode.Open with read access is supported.");
        GetInfoResultMessage info = await InfoAsync(path, scope).ConfigureAwait(false);
        FileInfoData file = RequireFile(info, path);
        return new DfsStream(Context, null, default, file, path, DfsReadView.VersionPinned, DfsWriteMode.OnFlush, FileAccess.Read, options.Share,
            isAppend: false, 0, options.BufferSize, info.ObservedAt, Pin);
    }

    // ---- Whole-file reads -------------------------------------------------------------------------------------

    /// <summary>Reads a whole file from one pinned revision (bounded by <see cref="DfsClientOptions.MaxMaterializeBytes"/>).</summary>
    public async Task<byte[]> ReadAllBytesAsync(string path, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Read, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.FilePath(path);
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        return await ReadAllCoreAsync(p, scope).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="ReadAllBytesAsync"/>.</summary>
    public byte[] ReadAllBytes(string path) =>
        SyncBridge.Run(() => ReadAllBytesAsync(path, CancellationToken.None));

    /// <summary>Reads a whole text file (BOM detection; UTF-8 by default).</summary>
    public async Task<string> ReadAllTextAsync(string path, Encoding? encoding = null, CancellationToken ct = default)
    {
        byte[] bytes = await ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        return TextCodec.Decode(bytes, encoding);
    }

    /// <summary>Synchronous counterpart of <see cref="ReadAllTextAsync"/>.</summary>
    public string ReadAllText(string path, Encoding? encoding = null) =>
        SyncBridge.Run(() => ReadAllTextAsync(path, encoding, CancellationToken.None));

    /// <summary>Reads all lines of a text file.</summary>
    public async Task<string[]> ReadAllLinesAsync(string path, Encoding? encoding = null, CancellationToken ct = default)
    {
        string text = await ReadAllTextAsync(path, encoding, ct).ConfigureAwait(false);
        return TextCodec.SplitLines(text);
    }

    /// <summary>Synchronous counterpart of <see cref="ReadAllLinesAsync"/>.</summary>
    public string[] ReadAllLines(string path, Encoding? encoding = null) =>
        SyncBridge.Run(() => ReadAllLinesAsync(path, encoding, CancellationToken.None));

    /// <summary>
    /// Enumerates the lines of a text file lazily from one pinned revision (one handle for the enumeration's lifetime;
    /// disposing the enumerator early releases it).
    /// </summary>
    public IAsyncEnumerable<string> ReadLinesAsync(string path, Encoding? encoding = null, CancellationToken ct = default)
    {
        Runtime.ThrowIfDisposed();
        string p = Validation.FilePath(path);
        return ReadLinesCoreAsync(p, encoding, ct);
    }

    /// <summary>Synchronous counterpart of <see cref="ReadLinesAsync"/>.</summary>
    public IEnumerable<string> ReadLines(string path, Encoding? encoding = null) =>
        SyncBridge.Enumerate(() => ReadLinesAsync(path, encoding, CancellationToken.None));

    private async Task<byte[]> ReadAllCoreAsync(string path, OperationScope scope)
    {
        DfsStream stream = await OpenPinnedReaderAsync(path, scope).ConfigureAwait(false);
        try
        {
            long length = stream.KnownLength;
            long limit = Runtime.Options.MaxMaterializeBytes;
            if (length > limit || length > Array.MaxLength)
                throw new DfsLimitExceededException(nameof(DfsClientOptions.MaxMaterializeBytes), limit, length);
            var result = new byte[length];
            int filled = 0;
            while (filled < result.Length)
            {
                int read = await stream.ReadInternalAsync(result.AsMemory(filled), scope).ConfigureAwait(false);
                if (read == 0)
                    throw new DfsProtocolException($"The pinned revision of '{path}' ended at {filled} of {length} bytes.");
                filled += read;
            }

            return result;
        }
        finally
        {
            await stream.ReleaseInternalAsync().ConfigureAwait(false);
        }
    }

    private async IAsyncEnumerable<string> ReadLinesCoreAsync(string path, Encoding? encoding, [EnumeratorCancellation] CancellationToken ct)
    {
        DfsStream stream;
        using (OperationScope scope = Begin(DfsOperationKind.Read, ct))
        {
            await scope.StartedAsync().ConfigureAwait(false);
            using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
            scope.Path = path;
            await scope.ValidatedAsync().ConfigureAwait(false);
            stream = await OpenPinnedReaderAsync(path, scope).ConfigureAwait(false);
        }

        try
        {
            int chunk = Math.Max(TextCodec.MaxPreambleLength, Runtime.Options.DefaultBufferSize);
            var buffer = new byte[chunk];
            var lines = new List<string>();
            var splitter = new TextCodec.LineSplitter();
            Decoder? decoder = null;
            var pending = new List<byte>(TextCodec.MaxPreambleLength);
            char[] chars = new char[chunk + 4];
            bool finished = false;
            while (!finished)
            {
                int read;
                using (OperationScope scope = Begin(DfsOperationKind.Read, ct))
                    read = await stream.ReadInternalAsync(buffer, scope).ConfigureAwait(false);
                ReadOnlySpan<byte> data = buffer.AsSpan(0, read);
                if (decoder is null)
                {
                    // Byte order mark detection needs the first bytes of the file (or the whole file when shorter).
                    pending.AddRange(data.ToArray());
                    if (pending.Count < TextCodec.MaxPreambleLength && read > 0)
                        continue;
                    (Encoding detected, int skip) = TextCodec.Detect(pending.ToArray(), encoding);
                    decoder = detected.GetDecoder();
                    data = pending.ToArray().AsSpan(skip);
                    pending.Clear();
                }

                finished = read == 0;
                if (chars.Length < data.Length + 4)
                    chars = new char[data.Length + 4];
                int decoded = decoder.GetChars(data, chars, flush: finished);
                splitter.Append(chars.AsSpan(0, decoded), lines);
                if (finished)
                    splitter.Complete(lines);
                foreach (string line in lines)
                    yield return line;
                lines.Clear();
            }
        }
        finally
        {
            await stream.ReleaseInternalAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Opens a reader of one revision: a version-pinned handle (live views), the transaction view, or the snapshot.</summary>
    private async Task<DfsStream> OpenPinnedReaderAsync(string path, OperationScope scope)
    {
        if (View is not null)
            return await OpenSnapshotStreamAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read }, scope).ConfigureAwait(false);
        var options = new DfsOpenOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite | FileShare.Delete,
            ReadView = IsTransactionBound ? DfsReadView.Live : DfsReadView.VersionPinned,
            BufferSize = Runtime.Options.DefaultBufferSize,
        };
        try
        {
            return await OpenHandleAsync(path, options, scope).ConfigureAwait(false);
        }
        catch (DfsCommitOutcomeUnknownException unknown)
        {
            // A read-only open has no namespace effect: an unanswered one is an availability failure (its handle, if
            // any, expires with its lease).
            throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"Opening '{path}' for reading did not complete.", unknown.InnerException ?? unknown);
        }
    }

    // ---- Whole-file writes ------------------------------------------------------------------------------------

    /// <summary>Atomically replaces the contents of a file (creating it), preserving its inode.</summary>
    public Task<CommitToken> WriteAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, CancellationToken ct = default) =>
        WriteAllBytesAsync(path, bytes, new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="WriteAllBytesAsync(string, ReadOnlyMemory{byte}, CancellationToken)"/>.</summary>
    public CommitToken WriteAllBytes(string path, ReadOnlyMemory<byte> bytes) =>
        SyncBridge.Run(() => WriteAllBytesAsync(path, bytes, CancellationToken.None));

    /// <summary>Atomically replaces the contents of a file with options (operation id, storage policy, commit-and-snapshot).</summary>
    public async Task<CommitToken> WriteAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.WriteAll, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.FilePath(path);
        Validation.MutationOptions(options);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        if (bytes.Length > Context.MaxPublishBytes)
            throw new DfsLimitExceededException(nameof(DfsVolumeLimits.MaxPublishBytes), Context.MaxPublishBytes, bytes.Length);
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        return await WriteAllCoreAsync(p, bytes, options, scope).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="WriteAllBytesAsync(string, ReadOnlyMemory{byte}, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken WriteAllBytes(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options) =>
        SyncBridge.Run(() => WriteAllBytesAsync(path, bytes, options, CancellationToken.None));

    /// <summary>Atomically replaces a file with text (UTF-8 without BOM by default; an explicit encoding writes its preamble).</summary>
    public async Task<CommitToken> WriteAllTextAsync(string path, string contents, Encoding? encoding = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(contents);
        byte[] bytes = TextCodec.Encode(contents, encoding, includePreamble: encoding is not null);
        return await WriteAllBytesAsync(path, bytes, ct).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="WriteAllTextAsync"/>.</summary>
    public CommitToken WriteAllText(string path, string contents, Encoding? encoding = null) =>
        SyncBridge.Run(() => WriteAllTextAsync(path, contents, encoding, CancellationToken.None));

    /// <summary>Atomically replaces a file with lines (each followed by <c>\n</c>).</summary>
    public Task<CommitToken> WriteAllLinesAsync(string path, IEnumerable<string> lines, Encoding? encoding = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(lines);
        return WriteAllTextAsync(path, TextCodec.JoinLines(lines), encoding, ct);
    }

    /// <summary>Synchronous counterpart of <see cref="WriteAllLinesAsync"/>.</summary>
    public CommitToken WriteAllLines(string path, IEnumerable<string> lines, Encoding? encoding = null) =>
        SyncBridge.Run(() => WriteAllLinesAsync(path, lines, encoding, CancellationToken.None));

    /// <summary>Atomically appends bytes (creating the file); applied exactly once per operation id.</summary>
    public Task<CommitToken> AppendAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, CancellationToken ct = default) =>
        AppendAllBytesAsync(path, bytes, new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="AppendAllBytesAsync(string, ReadOnlyMemory{byte}, CancellationToken)"/>.</summary>
    public CommitToken AppendAllBytes(string path, ReadOnlyMemory<byte> bytes) =>
        SyncBridge.Run(() => AppendAllBytesAsync(path, bytes, CancellationToken.None));

    /// <summary>Atomically appends bytes with options.</summary>
    public async Task<CommitToken> AppendAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Append, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.FilePath(path);
        Validation.MutationOptions(options);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        if (bytes.Length > Context.MaxPublishBytes)
            throw new DfsLimitExceededException(nameof(DfsVolumeLimits.MaxPublishBytes), Context.MaxPublishBytes, bytes.Length);
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        return await AppendCoreAsync(p, _ => bytes, options, scope).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="AppendAllBytesAsync(string, ReadOnlyMemory{byte}, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken AppendAllBytes(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options) =>
        SyncBridge.Run(() => AppendAllBytesAsync(path, bytes, options, CancellationToken.None));

    /// <summary>Appends text (an explicit encoding's preamble is written only when the file is new or empty).</summary>
    public async Task<CommitToken> AppendAllTextAsync(string path, string contents, Encoding? encoding = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(contents);
        using OperationScope scope = Begin(DfsOperationKind.Append, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.FilePath(path);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        byte[] withPreamble = TextCodec.Encode(contents, encoding, includePreamble: encoding is not null);
        byte[] plain = encoding is null || encoding.Preamble.Length == 0 ? withPreamble : TextCodec.Encode(contents, encoding, includePreamble: false);
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        return await AppendCoreAsync(p, currentLength => currentLength == 0 ? withPreamble : plain, new DfsMutationOptions(), scope).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="AppendAllTextAsync"/>.</summary>
    public CommitToken AppendAllText(string path, string contents, Encoding? encoding = null) =>
        SyncBridge.Run(() => AppendAllTextAsync(path, contents, encoding, CancellationToken.None));

    /// <summary>Appends lines (each followed by <c>\n</c>).</summary>
    public Task<CommitToken> AppendAllLinesAsync(string path, IEnumerable<string> lines, Encoding? encoding = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(lines);
        return AppendAllTextAsync(path, TextCodec.JoinLines(lines), encoding, ct);
    }

    /// <summary>Synchronous counterpart of <see cref="AppendAllLinesAsync"/>.</summary>
    public CommitToken AppendAllLines(string path, IEnumerable<string> lines, Encoding? encoding = null) =>
        SyncBridge.Run(() => AppendAllLinesAsync(path, lines, encoding, CancellationToken.None));

    private async Task<CommitToken> WriteAllCoreAsync(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options, OperationScope scope)
    {
        VolumeEntry entry = await EntryAsync(scope).ConfigureAwait(false);
        DfsStoragePolicy policy = await PolicyForAsync(path, options.StoragePolicy, entry, null, scope).ConfigureAwait(false);
        int threshold = ContentLayout.InlineThreshold(entry, Runtime.Capabilities);
        bool inline = bytes.Length <= threshold && ContentLayout.InlinePermitted(entry, policy, LabelsOf);
        Payload payload = inline
            ? Payload.Inline(bytes)
            : await UploadAsync(bytes, 0, entry, policy, scope).ConfigureAwait(false);
        long length = bytes.Length;
        WriteAllResultMessage result = await Context.MutateAsync<WriteAllResultMessage>(scope, "WriteAll", options.OperationId,
            header => new WriteAllMessage(header, path, length, payload.Changes, payload.Receipts, payload.Staging, options.StoragePolicy,
                options.CommitSnapshotName, CreateParents: false, ExpectedRevision: 0), path, TxId).ConfigureAwait(false);
        return result.Token;
    }

    private async Task<CommitToken> AppendCoreAsync(string path, Func<long, ReadOnlyMemory<byte>> content, DfsMutationOptions options,
        OperationScope scope)
    {
        VolumeEntry entry = await EntryAsync(scope).ConfigureAwait(false);
        int threshold = ContentLayout.InlineThreshold(entry, Runtime.Capabilities);
        for (int attempt = 0; ; attempt++)
        {
            // The current length decides the representation (and whether a text preamble is due).
            GetInfoResultMessage info = await InfoAsync(path, scope).ConfigureAwait(false);
            long current = info is { Exists: true, Info: { Kind: WireEntryKind.File } file } ? file.Length : 0;
            ReadOnlyMemory<byte> bytes = content(current);
            DfsStoragePolicy policy = await PolicyForAsync(path, options.StoragePolicy, entry, info, scope).ConfigureAwait(false);
            bool inline = attempt == 0 && current + bytes.Length <= threshold && ContentLayout.InlinePermitted(entry, policy, LabelsOf);
            Payload payload = inline
                ? Payload.Inline(bytes)
                : await UploadAsync(bytes, 0, entry, policy, scope).ConfigureAwait(false);
            long appendLength = bytes.Length;
            try
            {
                AppendResultMessage result = await Context.MutateAsync<AppendResultMessage>(scope, "Append", options.OperationId,
                    header => new AppendMessage(header, path, default, 0, appendLength, payload.Changes, payload.Receipts, payload.Staging,
                        CreateIfMissing: true, options.StoragePolicy, options.CommitSnapshotName), path, TxId).ConfigureAwait(false);
                return result.Token;
            }
            catch (ArgumentException) when (inline && options.OperationId is null)
            {
                // The file grew past the inline threshold between the length check and the append (the inline change
                // was rejected before anything was applied): append again as objects under a new operation id.
            }
        }
    }

    /// <summary>The changes, receipts and staging session of a publication's content.</summary>
    private sealed record Payload(IReadOnlyList<RangeChange> Changes, IReadOnlyList<ObjectReceipt> Receipts, StagingId Staging)
    {
        public static Payload Inline(ReadOnlyMemory<byte> bytes) =>
            new(bytes.IsEmpty ? [] : [ContentLayout.InlineChange(0, bytes.ToArray())], [], default);
    }

    /// <summary>Uploads <paramref name="bytes"/> as objects split at chunk boundaries (offsets relative to <paramref name="baseOffset"/>).</summary>
    private async Task<Payload> UploadAsync(ReadOnlyMemory<byte> bytes, long baseOffset, VolumeEntry entry, DfsStoragePolicy policy, OperationScope scope)
    {
        if (bytes.IsEmpty)
            return new Payload([], [], default);
        int chunk = ContentLayout.ChunkSize(Runtime.Capabilities);
        string pool = ContentLayout.PoolOf(entry, policy);
        for (int attempt = 0; ; attempt++)
        {
            StagingLease staging = await StagingAsync(scope).ConfigureAwait(false);
            var changes = new List<RangeChange>();
            var receipts = new List<ObjectReceipt>();
            try
            {
                int ordinal = 0;
                foreach ((long offset, int length) in ContentLayout.Chunks(0, bytes.Length, chunk))
                {
                    // A private copy: the caller's buffer may change while the upload is in flight.
                    byte[] piece = bytes.Slice((int)offset, length).ToArray();
                    (ObjectId id, UInt128 checksum) = Runtime.Data.Describe(piece);
                    IReadOnlyList<ObjectReceipt> copies = await Runtime.Data.UploadAsync(scope, Context, staging, id, piece, checksum, policy, pool,
                        scope.OperationId, ordinal++).ConfigureAwait(false);
                    changes.Add(ContentLayout.DataChange(baseOffset + offset, id, length, checksum));
                    receipts.AddRange(copies);
                }

                return new Payload(changes, receipts, staging.Id);
            }
            catch (DfsException ex) when (attempt == 0 && Transaction is null && ex.ErrorCode is DfsErrorCode.LeaseLost)
            {
                // The shared helper staging session was closed (session expiry): open a new one and upload again.
                Context.DropHelperStaging(staging);
            }
        }
    }

    private Task<StagingLease> StagingAsync(OperationScope scope) =>
        Transaction is not null ? Task.FromResult(Transaction.Staging) : Context.GetHelperStagingAsync(scope);

    /// <summary>
    /// The policy governing a write's content: the requested one, else the existing file's, else the volume default
    /// (only looked up when the volume defines named policies).
    /// </summary>
    private async Task<DfsStoragePolicy> PolicyForAsync(string path, string? requested, VolumeEntry entry, GetInfoResultMessage? info, OperationScope scope)
    {
        if (requested is not null)
        {
            if (ContentLayout.ResolvePolicy(entry, requested) is { } named)
                return named;
            await Context.RefreshRouteAsync(scope, Runtime.Rpc.DefaultAttemptTimeout(scope)).ConfigureAwait(false);
            return ContentLayout.ResolvePolicy(Context.Entry, requested) ?? entry.DefaultPolicy;
        }

        if (entry.NamedPolicies.Count == 0)
            return entry.DefaultPolicy;
        info ??= await InfoAsync(path, scope).ConfigureAwait(false);
        if (info is { Exists: true, Info: { } existing } && ContentLayout.ResolvePolicy(entry, existing.StoragePolicy) is { } own)
            return own;
        return entry.DefaultPolicy;
    }

    private DfsTopologyLabels? LabelsOf(NodeId node) => Runtime.Map.Node(node)?.Labels;

    // ---- Namespace operations ---------------------------------------------------------------------------------

    /// <summary>Copies a file atomically (FSD §5.4).</summary>
    public Task<CommitToken> CopyAsync(string sourcePath, string destinationPath, bool overwrite = false, CancellationToken ct = default) =>
        CopyAsync(sourcePath, destinationPath, overwrite, new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="CopyAsync(string, string, bool, CancellationToken)"/>.</summary>
    public CommitToken Copy(string sourcePath, string destinationPath, bool overwrite = false) =>
        SyncBridge.Run(() => CopyAsync(sourcePath, destinationPath, overwrite, CancellationToken.None));

    /// <summary>Copies a file atomically with options.</summary>
    public async Task<CommitToken> CopyAsync(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Copy, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string source = Validation.FilePath(sourcePath, nameof(sourcePath));
        string destination = Validation.FilePath(destinationPath, nameof(destinationPath));
        Validation.MutationOptions(options);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = source;
        await scope.ValidatedAsync().ConfigureAwait(false);
        CopyResultMessage result = await Context.MutateAsync<CopyResultMessage>(scope, "Copy", options.OperationId,
            header => new CopyMessage(header, source, destination, overwrite, 0, options.StoragePolicy, options.CommitSnapshotName), source, TxId)
            .ConfigureAwait(false);
        return result.Token;
    }

    /// <summary>Synchronous counterpart of <see cref="CopyAsync(string, string, bool, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Copy(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options) =>
        SyncBridge.Run(() => CopyAsync(sourcePath, destinationPath, overwrite, options, CancellationToken.None));

    /// <summary>Renames a file atomically (the inode is preserved).</summary>
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, bool overwrite = false, CancellationToken ct = default) =>
        MoveAsync(sourcePath, destinationPath, overwrite, new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="MoveAsync(string, string, bool, CancellationToken)"/>.</summary>
    public CommitToken Move(string sourcePath, string destinationPath, bool overwrite = false) =>
        SyncBridge.Run(() => MoveAsync(sourcePath, destinationPath, overwrite, CancellationToken.None));

    /// <summary>Renames a file atomically with options.</summary>
    public async Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Move, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string source = Validation.FilePath(sourcePath, nameof(sourcePath));
        string destination = Validation.FilePath(destinationPath, nameof(destinationPath));
        Validation.MutationOptions(options);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = source;
        await scope.ValidatedAsync().ConfigureAwait(false);
        MoveResultMessage result = await Context.MutateAsync<MoveResultMessage>(scope, "Move", options.OperationId,
            header => new MoveMessage(header, source, destination, overwrite, ExpectDirectory: false, options.CommitSnapshotName), source, TxId)
            .ConfigureAwait(false);
        return result.Token;
    }

    /// <summary>Synchronous counterpart of <see cref="MoveAsync(string, string, bool, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Move(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options) =>
        SyncBridge.Run(() => MoveAsync(sourcePath, destinationPath, overwrite, options, CancellationToken.None));

    /// <summary>Deletes a file (or a named stream); an absent file is not an error.</summary>
    public Task<CommitToken> DeleteAsync(string path, CancellationToken ct = default) =>
        DeleteAsync(path, new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync(string, CancellationToken)"/>.</summary>
    public CommitToken Delete(string path) =>
        SyncBridge.Run(() => DeleteAsync(path, CancellationToken.None));

    /// <summary>Deletes a file with options.</summary>
    public async Task<CommitToken> DeleteAsync(string path, DfsMutationOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Delete, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.FilePath(path);
        Validation.MutationOptions(options);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        DeleteResultMessage result = await Context.MutateAsync<DeleteResultMessage>(scope, "Delete", options.OperationId,
            header => new DeleteMessage(header, p, Recursive: false, ExpectDirectory: false, options.CommitSnapshotName), p, TxId).ConfigureAwait(false);
        return result.Token;
    }

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync(string, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Delete(string path, DfsMutationOptions options) =>
        SyncBridge.Run(() => DeleteAsync(path, options, CancellationToken.None));

    /// <summary>True when a file exists at <paramref name="path"/> (false only for genuine absence).</summary>
    public async Task<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Exists, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.Path(path);
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        GetInfoResultMessage info = await InfoAsync(p, scope).ConfigureAwait(false);
        return info is { Exists: true, Info.Kind: WireEntryKind.File };
    }

    /// <summary>Synchronous counterpart of <see cref="ExistsAsync"/>.</summary>
    public bool Exists(string path) =>
        SyncBridge.Run(() => ExistsAsync(path, CancellationToken.None));

    /// <summary>Metadata of a file (<see cref="FileNotFoundException"/> when absent).</summary>
    public async Task<DfsFileInfo> GetInfoAsync(string path, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.GetInfo, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.FilePath(path);
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        GetInfoResultMessage info = await InfoAsync(p, scope).ConfigureAwait(false);
        return WireConvert.ToFileInfo(RequireFile(info, p), info.ObservedAt);
    }

    /// <summary>Synchronous counterpart of <see cref="GetInfoAsync"/>.</summary>
    public DfsFileInfo GetInfo(string path) =>
        SyncBridge.Run(() => GetInfoAsync(path, CancellationToken.None));

    /// <summary>Changes the storage policy of a file (existing data migrates in the background).</summary>
    public async Task<CommitToken> SetStoragePolicyAsync(string path, string policyName, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.PolicyChange, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.Path(path);
        Validation.Name(policyName, nameof(policyName));
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        SetPolicyResultMessage result = await Context.MutateAsync<SetPolicyResultMessage>(scope, "SetPolicy", null,
            header => new SetPolicyMessage(header, p, policyName), p, TxId).ConfigureAwait(false);
        return result.Token;
    }

    /// <summary>Synchronous counterpart of <see cref="SetStoragePolicyAsync"/>.</summary>
    public CommitToken SetStoragePolicy(string path, string policyName) =>
        SyncBridge.Run(() => SetStoragePolicyAsync(path, policyName, CancellationToken.None));

    /// <summary>Changes the owner-only / read-only-for-others access flags of a file.</summary>
    public async Task<CommitToken> SetAccessAsync(string path, DfsInodeAccess access, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Admin, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.Path(path);
        ArgumentNullException.ThrowIfNull(access);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        SetAccessResultMessage result = await Context.MutateAsync<SetAccessResultMessage>(scope, "SetAccess", null,
            header => new SetAccessMessage(header, p, access.OwnerOnly, access.ReadOnlyForOthers), p, TxId).ConfigureAwait(false);
        return result.Token;
    }

    /// <summary>Synchronous counterpart of <see cref="SetAccessAsync"/>.</summary>
    public CommitToken SetAccess(string path, DfsInodeAccess access) =>
        SyncBridge.Run(() => SetAccessAsync(path, access, CancellationToken.None));

    // ---- helpers ----------------------------------------------------------------------------------------------

    /// <summary>Starts an operation; transaction-bound operations are <see cref="DfsOperationKind.TxOp"/> operations.</summary>
    private OperationScope Begin(DfsOperationKind kind, CancellationToken ct) =>
        Runtime.BeginOperation(Transaction is not null ? DfsOperationKind.TxOp : kind, Volume.Name, null, ct);

    private void EnsureWritable()
    {
        if (IsReadOnly)
        {
            throw new DfsReadOnlyException(View is not null
                ? "A snapshot view is read-only."
                : "This volume is served read-only (backup host).");
        }
    }

    /// <summary>Registers a transaction-bound operation (null outside transactions); fails unless the transaction is active.</summary>
    private DfsTransaction.TransactionOperation? EnterTransaction() => Transaction?.Enter();

    /// <summary><c>GetInfo</c> of a path in this facade's view (live, transaction or snapshot).</summary>
    internal Task<GetInfoResultMessage> InfoAsync(string path, OperationScope scope) =>
        Context.ReadAsync<GetInfoResultMessage>(scope, "GetInfo", header => new GetInfoMessage(header, path, default, Pin), path, TxId,
            requireSession: Pin != 0 || TxId is not null);

    private async Task<VolumeEntry> EntryAsync(OperationScope scope)
    {
        if (Context.Entry is { } entry)
            return entry;
        await Context.RefreshRouteAsync(scope, Runtime.Rpc.DefaultAttemptTimeout(scope)).ConfigureAwait(false);
        return Context.Entry ?? throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"Volume '{Volume.Name}' could not be located.");
    }

    private static FileInfoData RequireFile(GetInfoResultMessage info, string path)
    {
        if (!info.Exists || info.Info is null)
            throw new FileNotFoundException($"File '{Validation.Printable(path)}' does not exist.", path);
        if (info.Info.Kind == WireEntryKind.Directory)
            throw DfsErrors.ToException(DfsErrorCode.IsADirectory, $"'{Validation.Printable(path)}' is a directory, not a file.", path);
        return info.Info;
    }
}
