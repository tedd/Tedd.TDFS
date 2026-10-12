namespace Tedd.Tdfs.Consensus;

/// <summary>A state-machine snapshot: metadata plus the opaque payload produced by the host's state machine.</summary>
/// <param name="Meta">Index, term and membership covered by the snapshot.</param>
/// <param name="Data">Serialized state-machine image at <see cref="RaftSnapshotMeta.Index"/>.</param>
public sealed record RaftSnapshot(RaftSnapshotMeta Meta, ReadOnlyMemory<byte> Data);

/// <summary>
/// Supplied by the host so a leader can send a snapshot to a follower whose next entry has been compacted away.
/// </summary>
/// <remarks>
/// The returned snapshot must describe a committed state at or after the log view's snapshot index
/// (<see cref="IRaftLogView.Snapshot"/>) and no later than its last index, so that the entries following it are
/// still retained. Called on the node loop; it must not block. Returning null (snapshot not available yet) makes the
/// leader retry at the next heartbeat.
/// </remarks>
public interface IRaftSnapshotSource
{
    /// <summary>Returns the latest snapshot available for transfer, or null when none is available right now.</summary>
    RaftSnapshot? GetSnapshot();
}

/// <summary>Adapts a delegate to <see cref="IRaftSnapshotSource"/>.</summary>
public sealed class DelegateRaftSnapshotSource : IRaftSnapshotSource
{
    private readonly Func<RaftSnapshot?> _get;

    /// <summary>Creates the source.</summary>
    public DelegateRaftSnapshotSource(Func<RaftSnapshot?> get)
    {
        ArgumentNullException.ThrowIfNull(get);
        _get = get;
    }

    /// <inheritdoc />
    public RaftSnapshot? GetSnapshot() => _get();
}
