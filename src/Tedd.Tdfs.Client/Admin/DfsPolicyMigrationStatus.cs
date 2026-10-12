namespace Tedd.Tdfs;

/// <summary>Progress of a storage-policy migration.</summary>
/// <param name="Volume">Volume name.</param>
/// <param name="Target">Target policy.</param>
/// <param name="Effective">True when every object satisfies the target policy.</param>
/// <param name="PendingObjects">Objects not yet migrated.</param>
public sealed record DfsPolicyMigrationStatus(string Volume, DfsStoragePolicy Target, bool Effective, long PendingObjects);
