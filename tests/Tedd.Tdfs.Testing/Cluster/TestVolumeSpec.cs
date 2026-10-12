namespace Tedd.Tdfs.Testing;

/// <summary>A volume created by the harness when the cluster starts.</summary>
/// <param name="Name">Volume name.</param>
public sealed record TestVolumeSpec(string Name)
{
    /// <summary>Metadata (Raft voter) policy of the volume group.</summary>
    public DfsMetadataPolicy Metadata { get; init; } = new();

    /// <summary>Default data policy; null means the cluster default for its size (<see cref="TestClusterOptions.GetEffectiveDefaultPolicy()"/>).</summary>
    public DfsStoragePolicy? DefaultPolicy { get; init; }

    /// <summary>Named data policies.</summary>
    public IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies { get; init; } = new Dictionary<string, DfsStoragePolicy>();

    /// <summary>Inline threshold in bytes.</summary>
    public int InlineThreshold { get; init; } = 4096;

    /// <summary>Volume limits; null means the server defaults.</summary>
    public DfsVolumeLimits? Limits { get; init; }

    /// <summary>Storage pool the volume is bound to (Addendum A, POOL-02).</summary>
    public string StoragePool { get; init; } = DfsPoolNames.Default;

    /// <summary>Root security descriptor; null means the default root descriptor owned by the creating principal (ACL-06).</summary>
    public DfsSecurityDescriptor? RootSecurity { get; init; }

    /// <summary>Require Traverse on every ancestor directory (Addendum A, ACL-05).</summary>
    public bool EnforceTraverseChecking { get; init; }

    /// <summary>
    /// Builds the admin volume specification, substituting <paramref name="clusterDefault"/> when
    /// <see cref="DefaultPolicy"/> is null (for a volume on a named pool pass
    /// <see cref="TestClusterOptions.GetEffectiveDefaultPolicy(string)"/> of that pool).
    /// </summary>
    public DfsVolumeSpec ToVolumeSpec(DfsStoragePolicy clusterDefault)
    {
        ArgumentNullException.ThrowIfNull(clusterDefault);
        return new DfsVolumeSpec
        {
            Name = Name,
            Metadata = Metadata,
            DefaultPolicy = DefaultPolicy ?? clusterDefault,
            NamedPolicies = NamedPolicies,
            InlineThreshold = InlineThreshold,
            Limits = Limits ?? new DfsVolumeLimits(),
            StoragePool = StoragePool,
            RootSecurity = RootSecurity,
            EnforceTraverseChecking = EnforceTraverseChecking,
        };
    }
}
