namespace Tedd.Tdfs.Testing;

/// <summary>Options for a client connected through <see cref="TestCluster.ConnectAsync"/>.</summary>
public sealed record TestClientOptions
{
    /// <summary>Client name; its network identity is <c>client:{Name}</c>.</summary>
    public string Name { get; init; } = "c0";

    /// <summary>Credentials; null means the cluster admin.</summary>
    public DfsCredentials? Credentials { get; init; }

    /// <summary>Seed node names; null means all live nodes.</summary>
    public IReadOnlyList<string>? SeedNodes { get; init; }

    /// <summary>Operation timeout; null means 10 s.</summary>
    public TimeSpan? OperationTimeout { get; init; }

    /// <summary>Connect timeout; null means the SDK default.</summary>
    public TimeSpan? ConnectTimeout { get; init; }

    /// <summary>Default stream buffer size; null means the SDK default.</summary>
    public int? BufferSize { get; init; }

    /// <summary>Maximum bytes materialised by ReadAll helpers; null means the SDK default.</summary>
    public long? MaxMaterializeBytes { get; init; }

    /// <summary>Expected cluster id; null means the cluster's own id.</summary>
    public Guid? ExpectedClusterId { get; init; }

    /// <summary>Client clock; null means a fresh independent client clock.</summary>
    public TestClock? Clock { get; init; }

    /// <summary>SDK-side safety mutations (negative controls only).</summary>
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;

    /// <summary>Diagnostics sink.</summary>
    public IDfsDiagnostics? Diagnostics { get; init; }

    /// <summary>The network identity of this client (<c>client:{Name}</c>).</summary>
    public string NetworkName => "client:" + Name;
}
