namespace Tedd.Tdfs.Server;

/// <summary>Configuration of a live node.</summary>
public sealed record NodeOptions
{
    /// <summary>Unique node name.</summary>
    public required string Name { get; init; }

    /// <summary>Topology labels.</summary>
    public required DfsTopologyLabels Labels { get; init; }

    /// <summary>Endpoint to listen on ("quic://127.0.0.1:0" or "mem://n0").</summary>
    public required string ListenEndpoint { get; init; }

    /// <summary>Data directory (a path within the storage medium).</summary>
    public required string DataDirectory { get; init; }

    /// <summary>Shared cluster secret (root of node authentication, receipt, capability and storage keys).</summary>
    public required ReadOnlyMemory<byte> ClusterSecret { get; init; }

    /// <summary>Static or single-node bootstrap description; null when joining or restarting.</summary>
    public ClusterBootstrap? Bootstrap { get; init; }

    /// <summary>Endpoints of existing members to contact when joining.</summary>
    public IReadOnlyList<string> JoinSeeds { get; init; } = [];

    /// <summary>Timing parameters.</summary>
    public NodeTimings Timings { get; init; } = NodeTimings.Production;

    /// <summary>Capacity offered for object storage.</summary>
    public long CapacityBytes { get; init; } = long.MaxValue;

    /// <summary>Space reserved for journals, checkpoints, compaction and repair.</summary>
    public long ReservedBytes { get; init; } = 64L << 20;

    /// <summary>Negative-control mutations (all off in production).</summary>
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;

    /// <summary>Encrypt object payloads, journals and checkpoints at rest.</summary>
    public bool EncryptAtRest { get; init; } = true;

    /// <summary>PFX certificate for QUIC; null generates a self-signed certificate (development/test).</summary>
    public string? CertificatePath { get; init; }
}
