using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>Volume-scoped, strictly serializable transactions (FSD §8).</summary>
public sealed class DfsTransactionClient
{
    internal DfsTransactionClient(DfsVolume volume)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
    }

    /// <summary>The volume this facade operates on.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>Begins a transaction at a read version chosen by the volume leader.</summary>
    public Task<DfsTransaction> BeginAsync(DfsTransactionOptions? options = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsTransactionClient.BeginAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="BeginAsync"/>.</summary>
    public DfsTransaction Begin(DfsTransactionOptions? options = null) =>
        SyncBridge.Run(() => BeginAsync(options, CancellationToken.None));

    /// <summary>
    /// Queries a transaction's outcome by id (e.g. after <see cref="DfsCommitOutcomeUnknownException"/>). With
    /// <paramref name="resolve"/> an unfinished transaction is fenced so the answer is definite.
    /// </summary>
    public Task<DfsOperationOutcome> QueryOutcomeAsync(DfsTransactionId id, bool resolve = false, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsTransactionClient.QueryOutcomeAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="QueryOutcomeAsync"/>.</summary>
    public DfsOperationOutcome QueryOutcome(DfsTransactionId id, bool resolve = false) =>
        SyncBridge.Run(() => QueryOutcomeAsync(id, resolve, CancellationToken.None));
}
