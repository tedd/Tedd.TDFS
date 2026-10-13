namespace Tedd.Tdfs.Server;

/// <summary>Configuration of a late-sync backup node.</summary>
public sealed record BackupNodeOptions
{
    /// <summary>Unique backup node name.</summary>
    public required string Name { get; init; }

    /// <summary>Endpoint to listen on.</summary>
    public required string ListenEndpoint { get; init; }

    /// <summary>Data directory (a path within the storage medium).</summary>
    public required string DataDirectory { get; init; }

    /// <summary>Id of the source cluster whose exports are accepted.</summary>
    public required Guid SourceClusterId { get; init; }

    /// <summary>Backup key-encryption root; missing key material fails start with RecoveryKeyMissing.</summary>
    public required ReadOnlyMemory<byte> BackupSecret { get; init; }

    /// <summary>Principals: readers, the export principal (no retention rights) and retention administrators.</summary>
    public required IReadOnlyList<BootstrapPrincipal> Principals { get; init; }

    /// <summary>Permissions per principal name.</summary>
    public IReadOnlyDictionary<string, DfsPermissions> Grants { get; init; } = new Dictionary<string, DfsPermissions>();

    /// <summary>Retention applied to newly published snapshots.</summary>
    public DfsRetention DefaultRetention { get; init; } = new(null, false);

    /// <summary>Number of most recent published snapshots always kept per volume.</summary>
    public int KeepLastSnapshots { get; init; } = 16;

    /// <summary>Storage capacity.</summary>
    public long CapacityBytes { get; init; } = long.MaxValue;

    /// <summary>Timing parameters.</summary>
    public NodeTimings Timings { get; init; } = NodeTimings.Production;

    /// <summary>Deliberate safety mutations (mutation testing only; <see cref="SafetyMutations.None"/> in production).</summary>
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;
}
