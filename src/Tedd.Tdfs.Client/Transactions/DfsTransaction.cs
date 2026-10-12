using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Data;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Rpc;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>
/// A multi-file transaction (FSD §10): its <see cref="Files"/> and <see cref="Directories"/> facades work on a private
/// view; <see cref="CommitAsync"/> publishes everything atomically or nothing. Disposing an uncommitted transaction
/// aborts it; nothing ever commits implicitly.
/// </summary>
public sealed class DfsTransaction : IAsyncDisposable, IDisposable
{
    private readonly Lock _sync = new();
    private readonly List<DfsStream> _streams = [];
    private int _state = (int)DfsTransactionState.Active;
    private int _inFlight;

    internal DfsTransaction(DfsVolume volume, DfsTransactionId id, CommitToken readVersion, StagingLease staging)
    {
        ArgumentNullException.ThrowIfNull(volume);
        ArgumentNullException.ThrowIfNull(staging);
        Volume = volume;
        Id = id;
        ReadVersion = readVersion;
        Staging = staging;
        Files = new DfsFileClient(volume, isReadOnly: false, transaction: this);
        Directories = new DfsDirectoryClient(volume, isReadOnly: false, transaction: this);
    }

    /// <summary>The owning volume facade.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>The transaction's staging session (uploads of transaction-bound writes).</summary>
    internal StagingLease Staging { get; }

    /// <summary>Transaction id (the operation id of its begin).</summary>
    public DfsTransactionId Id { get; }

    /// <summary>Current state.</summary>
    public DfsTransactionState State => (DfsTransactionState)Volatile.Read(ref _state);

    /// <summary>The committed version the transaction reads from.</summary>
    public CommitToken ReadVersion { get; }

    /// <summary>Transaction-bound file operations.</summary>
    public DfsFileClient Files { get; }

    /// <summary>Transaction-bound directory operations.</summary>
    public DfsDirectoryClient Directories { get; }

    private ClientRuntime Runtime => Volume.Runtime;

    /// <summary>Atomically commits every change of the transaction.</summary>
    /// <exception cref="DfsTransactionConflictException">A conflicting commit happened since the read version.</exception>
    /// <exception cref="DfsCommitOutcomeUnknownException">The outcome is unknown (resolve with <see cref="DfsTransactionClient.QueryOutcomeAsync"/>).</exception>
    public async Task<CommitToken> CommitAsync(CancellationToken ct = default)
    {
        TxCommitResultMessage result = await CommitCoreAsync(null, ct).ConfigureAwait(false);
        return result.Token;
    }

    /// <summary>Synchronous counterpart of <see cref="CommitAsync"/>.</summary>
    public CommitToken Commit() =>
        SyncBridge.Run(() => CommitAsync(CancellationToken.None));

    /// <summary>Commits and creates a snapshot named <paramref name="snapshotName"/> in the same decision.</summary>
    public async Task<DfsSnapshotInfo> CommitWithSnapshotAsync(string snapshotName, CancellationToken ct = default)
    {
        TxCommitResultMessage result = await CommitCoreAsync(snapshotName ?? throw new ArgumentNullException(nameof(snapshotName)), ct).ConfigureAwait(false);
        if (result.Snapshot is null)
            throw new DfsProtocolException("The transaction committed but the reply carries no snapshot.");
        return WireConvert.ToSnapshotInfo(result.Snapshot);
    }

    /// <summary>Synchronous counterpart of <see cref="CommitWithSnapshotAsync"/>.</summary>
    public DfsSnapshotInfo CommitWithSnapshot(string snapshotName) =>
        SyncBridge.Run(() => CommitWithSnapshotAsync(snapshotName, CancellationToken.None));

    private async Task<TxCommitResultMessage> CommitCoreAsync(string? snapshotName, CancellationToken ct)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.TxCommit, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        if (snapshotName is not null)
            Validation.Name(snapshotName, nameof(snapshotName));
        ThrowIfNotActive();
        await scope.ValidatedAsync().ConfigureAwait(false);
        lock (_sync)
        {
            // The commit captures exactly what the transaction-bound operations completed before it (TX-017): an
            // operation still in flight makes the commit fail, and later operations fail while it runs.
            ThrowIfNotActive();
            if (_inFlight > 0)
            {
                throw new InvalidOperationException(
                    $"Transaction {Id} cannot commit while {_inFlight} of its operations (stream or facade I/O) are still in progress.");
            }

            SetState(DfsTransactionState.Committing);
        }

        // Changes staged in the transaction's open streams belong to the transaction: they join it first. A failure
        // here commits nothing and leaves the transaction open.
        try
        {
            DfsStream[] streams;
            lock (_sync)
                streams = [.. _streams];
            foreach (DfsStream stream in streams)
                await stream.PublishForTransactionCommitAsync(scope).ConfigureAwait(false);
        }
        catch
        {
            SetState(DfsTransactionState.Active);
            throw;
        }

        try
        {

            TxCommitResultMessage result = await Volume.Context.MutateAsync<TxCommitResultMessage>(scope, "TxCommit", null,
                header => new TxCommitMessage(header, snapshotName), null, Id, rebindable: false, queryTarget: Id.Operation, queryAsTransaction: true)
                .ConfigureAwait(false);
            SetState(DfsTransactionState.Committed);
            ForgetStreams();
            Runtime.Emit("tx.committed", $"Transaction {Id} committed.", null, Id, Volume.Name, result.Token);
            return result;
        }
        catch (DfsCommitOutcomeUnknownException)
        {
            SetState(DfsTransactionState.Unknown);
            throw;
        }
        catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or TimeoutException or DfsUnavailableException)
        {
            // Not applied (never transmitted, or fenced before it executed): the transaction is still open on the leader
            // and can be committed again or aborted.
            SetState(DfsTransactionState.Active);
            throw;
        }
        catch (Exception)
        {
            SetState(DfsTransactionState.Aborted);
            throw;
        }
    }

    /// <summary>Aborts the transaction (idempotent). Never reverses a commit.</summary>
    /// <exception cref="InvalidOperationException">The transaction had already committed.</exception>
    public async Task AbortAsync(CancellationToken ct = default)
    {
        DfsTransactionState state = State;
        if (state == DfsTransactionState.Aborted)
            return;
        if (state == DfsTransactionState.Committed)
            throw new InvalidOperationException($"Transaction {Id} has committed; a commit is never reversed.");
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.TxAbort, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        await scope.ValidatedAsync().ConfigureAwait(false);
        TxAbortResultMessage result = await SendAbortAsync(scope).ConfigureAwait(false);
        if (result.FinalStatus == WireOutcomeStatus.Committed)
        {
            SetState(DfsTransactionState.Committed);
            throw new InvalidOperationException($"Transaction {Id} had committed; a commit is never reversed.");
        }

        SetState(DfsTransactionState.Aborted);
        ForgetStreams();
    }

    /// <summary>Synchronous counterpart of <see cref="AbortAsync"/>.</summary>
    public void Abort() =>
        SyncBridge.Run(() => AbortAsync(CancellationToken.None));

    /// <summary>Aborts the transaction unless it committed (best effort; never commits).</summary>
    public async ValueTask DisposeAsync()
    {
        if (State is not (DfsTransactionState.Active or DfsTransactionState.Committing))
            return;
        if (Runtime.IsDisposed)
        {
            // The client's disposal never aborts or commits transactions (FSD §5.1); the leader expires it.
            return;
        }

        try
        {
            using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.TxAbort, Volume.Name, null, CancellationToken.None);
            TxAbortResultMessage result = await SendAbortAsync(scope).ConfigureAwait(false);
            SetState(result.FinalStatus == WireOutcomeStatus.Committed ? DfsTransactionState.Committed : DfsTransactionState.Aborted);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // The leader aborts an abandoned transaction at its deadline; disposal must not throw for it.
            Runtime.Emit("tx.abort-failed", $"Aborting transaction {Id} on disposal failed ({ex.GetType().Name}); it expires at its deadline.", null, Id,
                Volume.Name, exception: ex);
            SetState(DfsTransactionState.Aborted);
        }
    }

    /// <summary>Synchronous counterpart of <see cref="DisposeAsync"/>.</summary>
    public void Dispose() =>
        SyncBridge.RunValueTask(DisposeAsync);

    private async Task<TxAbortResultMessage> SendAbortAsync(OperationScope scope)
    {
        var request = new RpcRequest
        {
            Name = "TxAbort",
            ReplyType = MessageType.TxAbortResult,
            Kind = CallKind.Idempotent,
            Router = Volume.Context,
            Transaction = Id,
            Build = _ => ValueTask.FromResult<IWireMessage>(new TxAbortMessage(Volume.Context.Header(Volume.Context.CurrentSession, default, Id))),
        };
        return (TxAbortResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
    }

    /// <summary>Registers an open transaction-bound stream (its staged changes join the transaction at commit).</summary>
    internal void Register(DfsStream stream)
    {
        lock (_sync)
            _streams.Add(stream);
    }

    /// <summary>Forgets a disposed transaction-bound stream.</summary>
    internal void Unregister(DfsStream stream)
    {
        lock (_sync)
            _streams.Remove(stream);
    }

    private void ForgetStreams()
    {
        lock (_sync)
            _streams.Clear();
    }

    /// <summary>
    /// Registers a transaction-bound operation in flight (a commit cannot start until it ends); throws
    /// <see cref="InvalidOperationException"/> unless the transaction is active.
    /// </summary>
    internal TransactionOperation Enter()
    {
        lock (_sync)
        {
            ThrowIfNotActive();
            _inFlight++;
        }

        return new TransactionOperation(this);
    }

    private void Exit()
    {
        lock (_sync)
            _inFlight--;
    }

    /// <summary>A transaction-bound operation in flight; disposing it ends the registration (idempotent).</summary>
    internal sealed class TransactionOperation : IDisposable
    {
        private DfsTransaction? _owner;

        internal TransactionOperation(DfsTransaction owner) => _owner = owner;

        /// <inheritdoc />
        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.Exit();
    }

    /// <summary>Throws <see cref="InvalidOperationException"/> unless the transaction is active.</summary>
    internal void ThrowIfNotActive()
    {
        DfsTransactionState state = State;
        if (state != DfsTransactionState.Active)
            throw new InvalidOperationException($"Transaction {Id} is {state}; it accepts no further operations.");
    }

    internal void SetState(DfsTransactionState state) => Volatile.Write(ref _state, (int)state);

    internal bool TryTransition(DfsTransactionState expected, DfsTransactionState state) =>
        Interlocked.CompareExchange(ref _state, (int)state, (int)expected) == (int)expected;
}
