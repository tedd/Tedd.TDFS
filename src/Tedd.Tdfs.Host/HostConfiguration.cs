using System.Text.Json;
using System.Text.Json.Serialization;
using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Host;

/// <summary>
/// JSON configuration of a live node process (<c>Tedd.Tdfs.Host node --config &lt;file&gt;</c>). Maps one-to-one onto
/// <see cref="NodeOptions"/> plus what the process itself needs: the storage directory, the QUIC certificate and the
/// trusted peer pins.
/// </summary>
public sealed record NodeHostConfiguration
{
    /// <summary>Unique node name.</summary>
    public required string Name { get; init; }

    /// <summary>Topology labels.</summary>
    public required DfsTopologyLabels Labels { get; init; }

    /// <summary>QUIC endpoint to listen on (for example <c>quic://127.0.0.1:7100</c>).</summary>
    public required string ListenEndpoint { get; init; }

    /// <summary>Directory holding the node's storage medium (created when missing).</summary>
    public required string StorageRoot { get; init; }

    /// <summary>Data directory within the storage medium.</summary>
    public string DataDirectory { get; init; } = "data";

    /// <summary>Shared cluster secret, base64.</summary>
    public required string ClusterSecret { get; init; }

    /// <summary>Static or single-node bootstrap description; null when joining or restarting.</summary>
    public ClusterBootstrap? Bootstrap { get; init; }

    /// <summary>Endpoints of existing members to contact when joining.</summary>
    public IReadOnlyList<string> JoinSeeds { get; init; } = [];

    /// <summary>Explicit timings; null selects <see cref="TimingsPreset"/>.</summary>
    public NodeTimings? Timings { get; init; }

    /// <summary>"Production" (default) or "Fast"; used when <see cref="Timings"/> is null.</summary>
    public string? TimingsPreset { get; init; }

    /// <summary>Capacity offered for object storage (also the medium's capacity).</summary>
    public long CapacityBytes { get; init; } = long.MaxValue;

    /// <summary>Space reserved for journals, checkpoints, compaction and repair.</summary>
    public long ReservedBytes { get; init; } = 64L << 20;

    /// <summary>Encrypt object payloads, journals and checkpoints at rest.</summary>
    public bool EncryptAtRest { get; init; } = true;

    /// <summary>Negative-control mutations (tests only); null = none.</summary>
    public SafetyMutations? Mutations { get; init; }

    /// <summary>PFX certificate presented to peers and clients; null generates a development certificate.</summary>
    public string? CertificatePath { get; init; }

    /// <summary>Password of <see cref="CertificatePath"/>, if any.</summary>
    public string? CertificatePassword { get; init; }

    /// <summary>Trusted SHA-256 SPKI pins (hex) of the peers this node connects to.</summary>
    public IReadOnlyList<string> TrustedPins { get; init; } = [];

    /// <summary>The <see cref="NodeOptions"/> this configuration describes.</summary>
    public NodeOptions ToNodeOptions() => new()
    {
        Name = Name,
        Labels = Labels,
        ListenEndpoint = ListenEndpoint,
        DataDirectory = DataDirectory,
        ClusterSecret = Convert.FromBase64String(ClusterSecret),
        Bootstrap = Bootstrap,
        JoinSeeds = JoinSeeds,
        Timings = HostConfigurationJson.ResolveTimings(Timings, TimingsPreset),
        CapacityBytes = CapacityBytes,
        ReservedBytes = ReservedBytes,
        Mutations = Mutations ?? SafetyMutations.None,
        EncryptAtRest = EncryptAtRest,
        CertificatePath = CertificatePath,
    };
}

/// <summary>JSON configuration of a backup node process (<c>Tedd.Tdfs.Host backup --config &lt;file&gt;</c>).</summary>
public sealed record BackupHostConfiguration
{
    /// <summary>Unique backup node name.</summary>
    public required string Name { get; init; }

    /// <summary>QUIC endpoint to listen on.</summary>
    public required string ListenEndpoint { get; init; }

    /// <summary>Directory holding the node's storage medium.</summary>
    public required string StorageRoot { get; init; }

    /// <summary>Data directory within the storage medium.</summary>
    public string DataDirectory { get; init; } = "data";

    /// <summary>Id of the source cluster whose exports are accepted.</summary>
    public required Guid SourceClusterId { get; init; }

    /// <summary>Backup key-encryption root, base64; empty means missing key material.</summary>
    public string BackupSecret { get; init; } = string.Empty;

    /// <summary>Principals: readers, the export principal and retention administrators.</summary>
    public IReadOnlyList<BootstrapPrincipal> Principals { get; init; } = [];

    /// <summary>Permissions per principal name.</summary>
    public IReadOnlyDictionary<string, DfsPermissions> Grants { get; init; } = new Dictionary<string, DfsPermissions>();

    /// <summary>Number of most recent published snapshots always kept per volume.</summary>
    public int KeepLastSnapshots { get; init; } = 16;

    /// <summary>Storage capacity.</summary>
    public long CapacityBytes { get; init; } = long.MaxValue;

    /// <summary>Explicit timings; null selects <see cref="TimingsPreset"/>.</summary>
    public NodeTimings? Timings { get; init; }

    /// <summary>"Production" (default) or "Fast".</summary>
    public string? TimingsPreset { get; init; }

    /// <summary>PFX certificate; null generates a development certificate.</summary>
    public string? CertificatePath { get; init; }

    /// <summary>Password of <see cref="CertificatePath"/>, if any.</summary>
    public string? CertificatePassword { get; init; }

    /// <summary>Trusted SHA-256 SPKI pins (hex).</summary>
    public IReadOnlyList<string> TrustedPins { get; init; } = [];

    /// <summary>The <see cref="BackupNodeOptions"/> this configuration describes.</summary>
    public BackupNodeOptions ToBackupNodeOptions() => new()
    {
        Name = Name,
        ListenEndpoint = ListenEndpoint,
        DataDirectory = DataDirectory,
        SourceClusterId = SourceClusterId,
        BackupSecret = string.IsNullOrEmpty(BackupSecret) ? ReadOnlyMemory<byte>.Empty : Convert.FromBase64String(BackupSecret),
        Principals = Principals,
        Grants = Grants,
        KeepLastSnapshots = KeepLastSnapshots,
        CapacityBytes = CapacityBytes,
        Timings = HostConfigurationJson.ResolveTimings(Timings, TimingsPreset),
    };
}

/// <summary>What a host process prints (one line, prefixed <see cref="HostConfigurationJson.ReadyPrefix"/>) once it serves.</summary>
/// <param name="Name">Node name.</param>
/// <param name="Endpoint">Bound endpoint.</param>
/// <param name="NodeId">Node id (live nodes; empty for backup nodes).</param>
/// <param name="Incarnation">Incarnation (live nodes; 0 for backup nodes).</param>
public sealed record HostReadyMessage(string Name, string Endpoint, string NodeId, ulong Incarnation);

/// <summary>JSON settings and helpers shared by the host and the processes that launch it.</summary>
public static class HostConfigurationJson
{
    /// <summary>Prefix of the readiness line on standard output.</summary>
    public const string ReadyPrefix = "TDFS-READY ";

    /// <summary>Prefix of the start-failure line on standard output.</summary>
    public const string FailedPrefix = "TDFS-FAILED ";

    /// <summary>Serializer options (camelCase, enums as strings, indented).</summary>
    public static JsonSerializerOptions Options { get; } = CreateOptions();

    /// <summary>Reads a configuration file.</summary>
    /// <exception cref="InvalidDataException">The file is not a valid configuration.</exception>
    public static T Read<T>(string path) where T : class
    {
        string json = File.ReadAllText(path);
        try
        {
            return JsonSerializer.Deserialize<T>(json, Options) ?? throw new InvalidDataException($"{path} is empty.");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException($"{path} is not a valid {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    /// <summary>Writes a configuration file.</summary>
    public static void Write<T>(string path, T value) => File.WriteAllText(path, JsonSerializer.Serialize(value, Options));

    /// <summary>The timings a configuration selects.</summary>
    public static NodeTimings ResolveTimings(NodeTimings? timings, string? preset)
    {
        if (timings is not null)
        {
            timings.Validate();
            return timings;
        }

        return preset?.Trim().ToUpperInvariant() switch
        {
            null or "" or "PRODUCTION" => NodeTimings.Production,
            "FAST" => NodeTimings.Fast,
            _ => throw new InvalidDataException($"Unknown timings preset '{preset}' (expected Production or Fast)."),
        };
    }

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        options.Converters.Add(new JsonStringEnumConverter());
        options.Converters.Add(new NodeIdConverter());
        return options;
    }

    /// <summary>Node ids as their canonical string form.</summary>
    private sealed class NodeIdConverter : JsonConverter<NodeId>
    {
        public override NodeId Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            NodeId.TryParse(reader.GetString(), out NodeId id) ? id : throw new JsonException("Invalid node id.");

        public override void Write(Utf8JsonWriter writer, NodeId value, JsonSerializerOptions options) => writer.WriteStringValue(value.ToString());
    }
}
