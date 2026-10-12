using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Data;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>Begins transactions on a volume and resolves their outcomes (FSD §10).</summary>
public sealed class DfsTransactionClient
{
    internal DfsTransactionClient(DfsVolume volume)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
    }

    /// <summary>The owning volume facade.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>Begins a transaction (its id is the operation id of the begin).</summary>
    public async Task<DfsTransaction> BeginAsync(DfsTransactionOptions? options = null, CancellationToken ct = default)
    {
        ClientRuntime runtime = Volume.Runtime;
        using OperationScope scope = runtime.BeginOperation(DfsOperationKind.TxBegin, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        options ??= new DfsTransactionOptions();
        if (options.Timeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), timeout, "The transaction timeout must be positive.");
        if (options.OperationId is { IsEmpty: true })
            throw new ArgumentException("OperationId must not be the empty id; obtain one with DfsVolume.NewOperationIdAsync.", nameof(options));
        if (Volume.IsReadOnly)
            throw new DfsReadOnlyException("This volume is served read-only (backup host); transactions are not available.");
        await scope.ValidatedAsync().ConfigureAwait(false);
        TxBeginResultMessage begun = await Volume.Context.MutateAsync<TxBeginResultMessage>(scope, "TxBegin", options.OperationId,
            header => new TxBeginMessage(header, options.Timeout)).ConfigureAwait(false);
        ulong session = Volume.Context.CurrentSession?.Id ?? begun.Transaction.Operation.Session;
        var staging = new StagingLease(begun.Staging, default, begun.Transaction, session, begun.Capability, begun.CapabilityExpiry, begun.PlacementEpoch);
        runtime.Emit("tx.begun", $"Transaction {begun.Transaction} begun at {begun.ReadVersion}.", null, begun.Transaction, Volume.Name, begun.ReadVersion);
        return new DfsTransaction(Volume, begun.Transaction, begun.ReadVersion, staging);
    }

    /// <summary>Synchronous counterpart of <see cref="BeginAsync"/>.</summary>
    public DfsTransaction Begin(DfsTransactionOptions? options = null) =>
        SyncBridge.Run(() => BeginAsync(options, CancellationToken.None));

    /// <summary>
    /// Queries the outcome of a transaction's commit. With <paramref name="resolve"/> an unfinished commit is fenced so
    /// the answer is definite.
    /// </summary>
    public async Task<DfsOperationOutcome> QueryOutcomeAsync(DfsTransactionId id, bool resolve = false, CancellationToken ct = default)
    {
        using OperationScope scope = Volume.Runtime.BeginOperation(DfsOperationKind.QueryOperation, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        if (id.Operation.IsEmpty)
            throw new ArgumentException("The transaction id must not be empty.", nameof(id));
        await scope.ValidatedAsync().ConfigureAwait(false);
        QueryOperationResultMessage result = await Volume.Context.QueryAsync(id.Operation, isTransaction: true, resolve, scope).ConfigureAwait(false);
        return WireConvert.ToOutcome(id.Operation, result);
    }

    /// <summary>Synchronous counterpart of <see cref="QueryOutcomeAsync"/>.</summary>
    public DfsOperationOutcome QueryOutcome(DfsTransactionId id, bool resolve = false) =>
        SyncBridge.Run(() => QueryOutcomeAsync(id, resolve, CancellationToken.None));
}
