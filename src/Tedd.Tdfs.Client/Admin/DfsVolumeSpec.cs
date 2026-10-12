namespace Tedd.Tdfs;

/// <summary>Specification of a new volume for <see cref="DfsAdminClient.CreateVolumeAsync"/>.</summary>
public sealed record DfsVolumeSpec
{
    /// <summary>Volume name.</summary>
    public required string Name { get; init; }

    /// <summary>Metadata group policy.</summary>
    public DfsMetadataPolicy Metadata { get; init; } = new();

    /// <summary>Default storage policy for new files.</summary>
    public DfsStoragePolicy DefaultPolicy { get; init; } = DfsStoragePolicy.Default;

    /// <summary>Named storage policies (ordinal names).</summary>
    public IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies { get; init; } = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal);

    /// <summary>Maximum file length stored inline in metadata.</summary>
    public int InlineThreshold { get; init; } = 4096;

    /// <summary>Volume limits.</summary>
    public DfsVolumeLimits Limits { get; init; } = new();

    /// <summary>
    /// Storage pool the volume is bound to (Addendum A, POOL-02): its data objects are placed only on the pool's nodes and
    /// its metadata voters are chosen from them unless <see cref="DfsMetadataPolicy.ExplicitVoterNodes"/> is given.
    /// </summary>
    public string StoragePool { get; init; } = DfsPoolNames.Default;

    /// <summary>
    /// Security descriptor of the volume root (Addendum A, ACL-06); null means
    /// <see cref="DfsSecurityDescriptor.DefaultRoot"/> owned by the creating principal.
    /// </summary>
    public DfsSecurityDescriptor? RootSecurity { get; init; }

    /// <summary>When true, every access also requires Traverse on each ancestor directory (Addendum A, ACL-05).</summary>
    public bool EnforceTraverseChecking { get; init; }
}
