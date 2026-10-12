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
}
