namespace Tedd.Tdfs.Testing;

/// <summary>A volume created by the harness when the cluster starts.</summary>
/// <param name="Name">Volume name.</param>
public sealed record TestVolumeSpec(string Name)
{
    /// <summary>Metadata (Raft voter) policy of the volume group.</summary>
    public DfsMetadataPolicy Metadata { get; init; } = new();

    /// <summary>Default data policy; null means the cluster default for its size (<see cref="TestClusterOptions.GetEffectiveDefaultPolicy"/>).</summary>
    public DfsStoragePolicy? DefaultPolicy { get; init; }

    /// <summary>Named data policies.</summary>
    public IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies { get; init; } = new Dictionary<string, DfsStoragePolicy>();

    /// <summary>Inline threshold in bytes.</summary>
    public int InlineThreshold { get; init; } = 4096;

    /// <summary>Volume limits; null means the server defaults.</summary>
    public DfsVolumeLimits? Limits { get; init; }

    /// <summary>Builds the admin volume specification, substituting <paramref name="clusterDefault"/> when <see cref="DefaultPolicy"/> is null.</summary>
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
        };
    }
}
