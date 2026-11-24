using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Rpc;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>
/// Directory operations of a volume (FSD §5.5): recursive creation, metadata, atomic rename, (recursive) deletion and
/// enumeration of one pinned namespace version. Facades are thread-safe.
/// </summary>
public sealed partial class DfsDirectoryClient
{
    internal DfsDirectoryClient(DfsVolume volume, bool isReadOnly = false, DfsTransaction? transaction = null, DfsSnapshotView? view = null)
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

    /// <summary>The volume binding of the transaction or snapshot view this facade works in (null for the live volume).</summary>
    private VolumeBinding? Bound => Transaction?.Binding ?? View?.Binding;

    /// <summary>Creates a directory and any missing ancestors (idempotent: an existing directory is returned).</summary>
    public Task<DfsDirectoryInfo> CreateDirectoryAsync(string path, CancellationToken ct = default) =>
        CreateDirectoryAsync(path, new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="CreateDirectoryAsync(string, CancellationToken)"/>.</summary>
    public DfsDirectoryInfo CreateDirectory(string path) =>
        SyncBridge.Run(() => CreateDirectoryAsync(path, CancellationToken.None));

    /// <summary>Creates a directory (recursively) with options.</summary>
    public async Task<DfsDirectoryInfo> CreateDirectoryAsync(string path, DfsMutationOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.CreateDirectory, path, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.DirectoryPath(path);
        Validation.MutationOptions(options);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        CreateDirectoryResultMessage result = await Context.MutateAsync<CreateDirectoryResultMessage>(scope, "CreateDirectory", options.OperationId,
            header => new CreateDirectoryMessage(header, p, Recursive: true, options.CommitSnapshotName), p, TxId, binding: Bound).ConfigureAwait(false);
        return WireConvert.ToDirectoryInfo(result.Info, result.Token);
    }

    /// <summary>Synchronous counterpart of <see cref="CreateDirectoryAsync(string, DfsMutationOptions, CancellationToken)"/>.</summary>
    public DfsDirectoryInfo CreateDirectory(string path, DfsMutationOptions options) =>
        SyncBridge.Run(() => CreateDirectoryAsync(path, options, CancellationToken.None));

    /// <summary>True when a directory exists at <paramref name="path"/> (false only for genuine absence).</summary>
    public async Task<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Exists, path, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        // Stream syntax in a directory operation is an invalid path (Addendum ADS-06).
        string p = Validation.DirectoryPath(path);
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        GetInfoResultMessage info = await InfoAsync(p, scope).ConfigureAwait(false);
        return info is { Exists: true, Info.Kind: WireEntryKind.Directory };
    }

    /// <summary>Synchronous counterpart of <see cref="ExistsAsync"/>.</summary>
    public bool Exists(string path) =>
        SyncBridge.Run(() => ExistsAsync(path, CancellationToken.None));

    /// <summary>Metadata of a directory (<see cref="DirectoryNotFoundException"/> when absent).</summary>
    public async Task<DfsDirectoryInfo> GetInfoAsync(string path, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.GetInfo, path, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.DirectoryPath(path);
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        GetInfoResultMessage info = await InfoAsync(p, scope).ConfigureAwait(false);
        if (!info.Exists || info.Info is null)
            throw DfsErrors.ToException(DfsErrorCode.DirectoryNotFound, $"Directory '{Validation.Printable(p)}' does not exist.", p);
        if (info.Info.Kind != WireEntryKind.Directory)
            throw DfsErrors.ToException(DfsErrorCode.NotADirectory, $"'{Validation.Printable(p)}' is a file, not a directory.", p);
        return WireConvert.ToDirectoryInfo(info.Info, info.ObservedAt);
    }

    /// <summary>Synchronous counterpart of <see cref="GetInfoAsync"/>.</summary>
    public DfsDirectoryInfo GetInfo(string path) =>
        SyncBridge.Run(() => GetInfoAsync(path, CancellationToken.None));

    /// <summary>Renames a directory atomically (its subtree moves with it).</summary>
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, CancellationToken ct = default) =>
        MoveAsync(sourcePath, destinationPath, new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="MoveAsync(string, string, CancellationToken)"/>.</summary>
    public CommitToken Move(string sourcePath, string destinationPath) =>
        SyncBridge.Run(() => MoveAsync(sourcePath, destinationPath, CancellationToken.None));

    /// <summary>Renames a directory atomically with options.</summary>
    public async Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, DfsMutationOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Move, sourcePath, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string source = Validation.DirectoryPath(sourcePath, nameof(sourcePath));
        string destination = Validation.DirectoryPath(destinationPath, nameof(destinationPath));
        Validation.MutationOptions(options);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = source;
        await scope.ValidatedAsync().ConfigureAwait(false);
        MoveResultMessage result = await Context.MutateAsync<MoveResultMessage>(scope, "Move", options.OperationId,
            header => new MoveMessage(header, source, destination, Overwrite: false, ExpectDirectory: true, options.CommitSnapshotName), source, TxId, binding: Bound)
            .ConfigureAwait(false);
        return result.Token;
    }

    /// <summary>Synchronous counterpart of <see cref="MoveAsync(string, string, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Move(string sourcePath, string destinationPath, DfsMutationOptions options) =>
        SyncBridge.Run(() => MoveAsync(sourcePath, destinationPath, options, CancellationToken.None));

    /// <summary>Deletes a directory (recursively when asked; a non-empty directory otherwise fails).</summary>
    public Task<CommitToken> DeleteAsync(string path, bool recursive = false, CancellationToken ct = default) =>
        DeleteAsync(path, recursive, new DfsMutationOptions(), ct);

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync(string, bool, CancellationToken)"/>.</summary>
    public CommitToken Delete(string path, bool recursive = false) =>
        SyncBridge.Run(() => DeleteAsync(path, recursive, CancellationToken.None));

    /// <summary>Deletes a directory with options.</summary>
    public async Task<CommitToken> DeleteAsync(string path, bool recursive, DfsMutationOptions options, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Delete, path, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.DirectoryPath(path);
        Validation.MutationOptions(options);
        EnsureWritable();
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        DeleteResultMessage result = await Context.MutateAsync<DeleteResultMessage>(scope, "Delete", options.OperationId,
            header => new DeleteMessage(header, p, recursive, ExpectDirectory: true, options.CommitSnapshotName), p, TxId, binding: Bound).ConfigureAwait(false);
        if (!result.Existed)
            throw DfsErrors.ToException(DfsErrorCode.DirectoryNotFound, $"Directory '{Validation.Printable(p)}' does not exist.", p);
        return result.Token;
    }

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync(string, bool, DfsMutationOptions, CancellationToken)"/>.</summary>
    public CommitToken Delete(string path, bool recursive, DfsMutationOptions options) =>
        SyncBridge.Run(() => DeleteAsync(path, recursive, options, CancellationToken.None));

    /// <summary>
    /// Enumerates a directory lazily; every page comes from the namespace version pinned by the first page (the pin is
    /// renewed while the enumeration runs and released when it ends or is disposed).
    /// </summary>
    public IAsyncEnumerable<DfsEntryInfo> EnumerateAsync(string path, DfsEnumerationOptions? options = null, CancellationToken ct = default)
    {
        Runtime.ThrowIfDisposed();
        string p = Validation.DirectoryPath(path);
        DfsEnumerationOptions validated = Validation.EnumerationOptions(options);
        return EnumerateCoreAsync(p, validated, ct);
    }

    /// <summary>Synchronous counterpart of <see cref="EnumerateAsync"/>.</summary>
    public IEnumerable<DfsEntryInfo> Enumerate(string path, DfsEnumerationOptions? options = null) =>
        SyncBridge.Enumerate(() => EnumerateAsync(path, options, CancellationToken.None));

    /// <summary>
    /// Lists one page. Pass the previous page's <see cref="DfsDirectoryPage.ContinuationToken"/> to continue in the same
    /// pinned version; an expired continuation fails with <see cref="DfsPinExpiredException"/> (ContinuationExpired).
    /// </summary>
    public async Task<DfsDirectoryPage> ListPageAsync(string path, DfsEnumerationOptions? options = null, string? continuationToken = null,
        CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Enumerate, path, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        string p = Validation.DirectoryPath(path);
        DfsEnumerationOptions validated = Validation.EnumerationOptions(options);
        if (continuationToken is { Length: 0 })
            throw new ArgumentException("The continuation token must not be empty (use null for the first page).", nameof(continuationToken));
        using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
        scope.Path = p;
        await scope.ValidatedAsync().ConfigureAwait(false);
        ListDirectoryResultMessage page = await ListAsync(p, validated, continuationToken, Bound, scope).ConfigureAwait(false);
        return new DfsDirectoryPage(page.Entries.Select(e => WireConvert.ToEntry(p, e)).ToArray(), page.Continuation, page.PinnedAt);
    }

    /// <summary>Synchronous counterpart of <see cref="ListPageAsync"/>.</summary>
    public DfsDirectoryPage ListPage(string path, DfsEnumerationOptions? options = null, string? continuationToken = null) =>
        SyncBridge.Run(() => ListPageAsync(path, options, continuationToken, CancellationToken.None));

    private async IAsyncEnumerable<DfsEntryInfo> EnumerateCoreAsync(string path, DfsEnumerationOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var holder = new EnumerationPin();
        ulong pin = 0;
        VolumeBinding? binding = Bound;
        try
        {
            string? continuation = null;
            bool first = true;
            while (true)
            {
                ListDirectoryResultMessage page;
                using (OperationScope scope = Begin(DfsOperationKind.Enumerate, ct))
                {
                    if (first)
                        await scope.StartedAsync().ConfigureAwait(false);

                    // Every page is transaction-bound I/O: it cannot overlap the commit and fails once the transaction ended.
                    using DfsTransaction.TransactionOperation? txOperation = EnterTransaction();
                    if (first)
                    {
                        scope.Path = path;
                        await scope.ValidatedAsync().ConfigureAwait(false);
                    }

                    holder.ThrowIfExpired();
                    page = await ListAsync(path, options, continuation, binding, scope).ConfigureAwait(false);
                }

                if (first)
                {
                    // Later pages read the version pinned by the first one, in the volume that answered it.
                    binding ??= page.PinnedAt.IsNone ? Context.CurrentBinding : new VolumeBinding(page.PinnedAt.VolumeId, page.PinnedAt.VolumeEpoch);
                    // A snapshot listing borrows the view's pin. The view owns its renewal and release.
                    if (page.EnumerationPin != 0 && page.EnumerationPin != Pin)
                    {
                        pin = page.EnumerationPin;
                        Context.RegisterPin(pin, Context.CurrentSession, holder);
                    }
                }

                first = false;
                foreach (DirectoryEntryData entry in page.Entries)
                    yield return WireConvert.ToEntry(path, entry);
                if (page.Continuation is null)
                    yield break;
                continuation = page.Continuation;
            }
        }
        finally
        {
            if (pin != 0)
                await ReleasePinAsync(pin, binding).ConfigureAwait(false);
        }
    }

    private Task<ListDirectoryResultMessage> ListAsync(string path, DfsEnumerationOptions options, string? continuation, VolumeBinding? binding,
        OperationScope scope) =>
        Context.ReadAsync<ListDirectoryResultMessage>(scope, "ListDirectory",
            header => new ListDirectoryMessage(header, path, options.Pattern, WireConvert.ToWire(options.Kinds), options.PageSize, continuation, Pin),
            path, TxId, requireSession: true, binding: binding, sessionOnly: continuation is null && Pin == 0 && TxId is null);

    private async Task ReleasePinAsync(ulong pin, VolumeBinding? binding)
    {
        Context.UnregisterPin(pin);
        if (Runtime.IsDisposed)
            return;
        try
        {
            using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.Enumerate, Volume.Name, null, CancellationToken.None);
            await CloseSnapshotPinAsync(Context, pin, scope, binding).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // The pin expires on its own when it is no longer renewed.
            Runtime.Emit("enumeration.release-failed", $"Releasing enumeration pin {pin} failed ({ex.GetType().Name}).", volume: Volume.Name, exception: ex);
        }
    }

    /// <summary>Releases a reader pin (<c>CloseSnapshot</c>; idempotent) of <paramref name="binding"/>'s volume.</summary>
    internal static async Task CloseSnapshotPinAsync(VolumeContext context, ulong pin, OperationScope scope, VolumeBinding? binding)
    {
        var request = new RpcRequest
        {
            Name = "CloseSnapshot",
            ReplyType = MessageType.CloseSnapshotResult,
            Kind = CallKind.Idempotent,
            Router = context,
            Build = _ => ValueTask.FromResult<IWireMessage>(new CloseSnapshotMessage(context.Header(context.CurrentSession, default, null, binding), pin)),
        };
        await context.Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
    }

    /// <summary>Starts an operation; transaction-bound operations are <see cref="DfsOperationKind.TxOp"/> operations.</summary>
    private OperationScope Begin(DfsOperationKind kind, CancellationToken ct) =>
        Runtime.BeginOperation(Transaction is not null ? DfsOperationKind.TxOp : kind, Volume.Name, null, ct);

    /// <summary>Begins an operation addressing <paramref name="path"/> (the raw argument: fault sites hit before validation carry it too).</summary>
    private OperationScope Begin(DfsOperationKind kind, string? path, CancellationToken ct) =>
        Runtime.BeginOperation(Transaction is not null ? DfsOperationKind.TxOp : kind, Volume.Name, path, ct);

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

    private Task<GetInfoResultMessage> InfoAsync(string path, OperationScope scope) =>
        Context.ReadAsync<GetInfoResultMessage>(scope, "GetInfo", header => new GetInfoMessage(header, path, default, Pin), path, TxId,
            requireSession: Pin != 0 || TxId is not null, binding: Bound);

    /// <summary>Holder of an enumeration's pin (renewed by keep-alive; an expiry fails the enumeration).</summary>
    private sealed class EnumerationPin : IPinHolder
    {
        private Exception? _expired;

        public void OnPinExpired(Exception reason) => Interlocked.CompareExchange(ref _expired, reason, null);

        public void ThrowIfExpired()
        {
            if (Volatile.Read(ref _expired) is { } expired)
                ExceptionDispatchInfo.Throw(expired);
        }
    }
}
