namespace Tedd.Tdfs;

/// <summary>Volume metadata as observed at <see cref="LastCommit"/>.</summary>
/// <param name="Id">Volume id.</param>
/// <param name="Name">Volume name.</param>
/// <param name="Epoch">Volume epoch (increments on restore/promotion).</param>
/// <param name="Metadata">Metadata group policy.</param>
/// <param name="DefaultPolicy">Default storage policy for new files.</param>
/// <param name="NamedPolicies">Named storage policies (ordinal names).</param>
/// <param name="InlineThreshold">Maximum file length stored inline in metadata.</param>
/// <param name="Limits">Volume limits.</param>
/// <param name="LastCommit">Commit token at which this information was read.</param>
/// <param name="StoragePool">Storage pool the volume is bound to (Addendum A, POOL-02).</param>
/// <param name="EnforceTraverseChecking">True when Traverse is required on every ancestor directory (Addendum A, ACL-05).</param>
public sealed record DfsVolumeInfo(
    VolumeId Id,
    string Name,
    ulong Epoch,
    DfsMetadataPolicy Metadata,
    DfsStoragePolicy DefaultPolicy,
    IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies,
    int InlineThreshold,
    DfsVolumeLimits Limits,
    CommitToken LastCommit,
    string StoragePool = DfsPoolNames.Default,
    bool EnforceTraverseChecking = false);
