namespace Tedd.Tdfs.Server;

/// <summary>One replica's applied state of a volume.</summary>
/// <param name="Id">Volume id.</param>
/// <param name="Epoch">Volume epoch.</param>
/// <param name="AppliedIndex">Applied Raft index.</param>
/// <param name="InodeCount">Inodes in the current root.</param>
/// <param name="Handles">Open handles.</param>
/// <param name="Snapshots">Retained snapshots.</param>
/// <param name="Sessions">Client sessions.</param>
/// <param name="Pins">Active pins.</param>
/// <param name="Staging">Staging sessions.</param>
/// <param name="ActiveTransactions">Transactions in the Active state.</param>
/// <param name="Backups">Backup progress per target.</param>
public sealed record VolumeStateInspection(VolumeId Id, ulong Epoch, ulong AppliedIndex, int InodeCount,
    IReadOnlyList<HandleInspection> Handles, IReadOnlyList<SnapshotInspection> Snapshots, IReadOnlyList<SessionInspection> Sessions,
    IReadOnlyList<PinInspection> Pins, IReadOnlyList<StagingInspection> Staging, int ActiveTransactions,
    IReadOnlyList<BackupProgressInspection> Backups)
{
    /// <summary>Resolves files in the inspected (immutable) state; set by the volume state machine.</summary>
    internal Func<string, FileInspection?>? FileResolver { get; init; }

    /// <summary>Computes the reachable object set of the inspected (immutable) state; set by the volume state machine.</summary>
    internal Func<IReadOnlySet<ObjectId>>? ReachableResolver { get; init; }

    /// <summary>
    /// The file at <paramref name="path"/> in the current root (a stream path inspects its file), or null when absent (or a
    /// directory).
    /// </summary>
    /// <exception cref="InvalidOperationException">The inspected state machine does not provide file inspection.</exception>
    public FileInspection? GetFile(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return (FileResolver ?? throw new InvalidOperationException("This volume state machine does not provide file inspection."))(path);
    }

    /// <summary>Objects reachable from the current root, snapshot roots, transaction roots and pinned inode versions.</summary>
    /// <exception cref="InvalidOperationException">The inspected state machine does not provide reachability.</exception>
    public IReadOnlySet<ObjectId> ReachableObjects()
        => (ReachableResolver ?? throw new InvalidOperationException("This volume state machine does not provide reachability."))();
}
