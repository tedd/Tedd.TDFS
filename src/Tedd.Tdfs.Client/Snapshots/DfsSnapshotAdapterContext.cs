namespace Tedd.Tdfs;

/// <summary>Context passed to <see cref="IDfsSnapshotAdapter"/> callbacks.</summary>
/// <param name="Volume">Volume being snapshotted.</param>
/// <param name="SnapshotName">Name of the snapshot being created.</param>
public sealed record DfsSnapshotAdapterContext(string Volume, string SnapshotName);
