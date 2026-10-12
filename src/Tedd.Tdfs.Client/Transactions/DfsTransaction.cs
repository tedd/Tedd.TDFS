using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>
/// An active transaction. Its <see cref="Files"/> and <see cref="Directories"/> facades stage changes privately until
/// <see cref="CommitAsync"/>. Disposing without committing aborts; disposal never commits.
/// </summary>
public sealed class DfsTransaction : IAsyncDisposable, IDisposable
{
    private int _state = (int)DfsTransactionState.Active;

    internal DfsTransaction(DfsVolume volume, DfsTransactionId id, CommitToken readVersion)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
        Id = id;
        ReadVersion = readVersion;
        Files = new DfsFileClient(volume, isReadOnly: false, transaction: this);
        Directories = new DfsDirectoryClient(volume, isReadOnly: false, transaction: this);
    }

    /// <summary>The volume the transaction belongs to.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>Durable transaction id (usable with <see cref="DfsTransactionClient.QueryOutcomeAsync"/>).</summary>
    public DfsTransactionId Id { get; }

    /// <summary>Current client-side state.</summary>
    public DfsTransactionState State => (DfsTransactionState)Volatile.Read(ref _state);

    /// <summary>The committed version all reads in the transaction observe.</summary>
    public CommitToken ReadVersion { get; }

    /// <summary>Transaction-bound file operations.</summary>
    public DfsFileClient Files { get; }

    /// <summary>Transaction-bound directory operations.</summary>
    public DfsDirectoryClient Directories { get; }

    /// <summary>
    /// Flushes bound streams and commits atomically. Throws <see cref="DfsTransactionConflictException"/> on conflict
    /// and <see cref="DfsCommitOutcomeUnknownException"/> when the outcome cannot be determined.
    /// </summary>
    public Task<CommitToken> CommitAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsTransaction.CommitAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CommitAsync"/>.</summary>
    public CommitToken Commit() =>
        SyncBridge.Run(() => CommitAsync(CancellationToken.None));

    /// <summary>Commits and creates snapshot <paramref name="snapshotName"/> in the same metadata decision (FSD §11.2).</summary>
    public Task<DfsSnapshotInfo> CommitWithSnapshotAsync(string snapshotName, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsTransaction.CommitWithSnapshotAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CommitWithSnapshotAsync"/>.</summary>
    public DfsSnapshotInfo CommitWithSnapshot(string snapshotName) =>
        SyncBridge.Run(() => CommitWithSnapshotAsync(snapshotName, CancellationToken.None));

    /// <summary>Aborts the transaction and discards its staged changes. Never reverses a committed decision.</summary>
    public Task AbortAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsTransaction.AbortAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="AbortAsync"/>.</summary>
    public void Abort() =>
        SyncBridge.Run(() => AbortAsync(CancellationToken.None));

    /// <summary>Aborts if not committed (never commits) and releases resources. Idempotent.</summary>
    public ValueTask DisposeAsync() =>
        throw new NotImplementedException("DfsTransaction.DisposeAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="DisposeAsync"/>.</summary>
    public void Dispose() =>
        throw new NotImplementedException("DfsTransaction.Dispose: implemented in a later phase");

    /// <summary>Sets the client-side state.</summary>
    internal void SetState(DfsTransactionState state) => Volatile.Write(ref _state, (int)state);

    /// <summary>Atomically moves from <paramref name="expected"/> to <paramref name="state"/>; returns false when the state differs.</summary>
    internal bool TryTransition(DfsTransactionState expected, DfsTransactionState state) =>
        Interlocked.CompareExchange(ref _state, (int)state, (int)expected) == (int)expected;
}
