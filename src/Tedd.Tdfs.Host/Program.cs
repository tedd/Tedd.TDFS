using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Host;

/// <summary>
/// The Tedd.TDFS server host executable.
/// <list type="bullet">
/// <item><c>node --config &lt;file&gt; [--log &lt;level&gt;]</c> runs a live node from a <see cref="NodeHostConfiguration"/>.</item>
/// <item><c>backup --config &lt;file&gt; [--log &lt;level&gt;]</c> runs a late-sync backup node from a <see cref="BackupHostConfiguration"/>.</item>
/// <item><c>bootstrap --name &lt;n&gt; --listen &lt;endpoint&gt; --storage &lt;dir&gt; --out &lt;file&gt; [...]</c> initialises a new
/// single-node cluster configuration (new cluster id and secret, development certificate, "admin" principal), the
/// recovery runbook's clean-host step.</item>
/// </list>
/// A running host prints one <c>TDFS-READY {json}</c> line on standard output once it serves (or <c>TDFS-FAILED {json}</c>),
/// logs to standard error, and accepts the commands <c>stop</c>, <c>pause</c> and <c>resume</c> on standard input; Ctrl+C
/// and the end of standard input also stop it gracefully.
/// </summary>
internal static class Program
{
    private const int ExitOk = 0;
    private const int ExitUsage = 1;
    private const int ExitConfiguration = 2;
    private const int ExitStartFailed = 3;
    private const int ExitRecoveryKeyMissing = 4;

    private static readonly JsonSerializerOptions OneLine = new(HostConfigurationJson.Options) { WriteIndented = false };

    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
        {
            PrintUsage();
            return args.Length == 0 ? ExitUsage : ExitOk;
        }

        Dictionary<string, string> flags;
        try
        {
            flags = ParseFlags(args.AsSpan(1));
        }
        catch (ArgumentException ex)
        {
            await Console.Error.WriteLineAsync(ex.Message);
            PrintUsage();
            return ExitUsage;
        }

        try
        {
            return args[0] switch
            {
                "node" => await RunNodeAsync(HostConfigurationJson.Read<NodeHostConfiguration>(Require(flags, "config")), LogLevel(flags)),
                "backup" => await RunBackupAsync(HostConfigurationJson.Read<BackupHostConfiguration>(Require(flags, "config")), LogLevel(flags)),
                "bootstrap" => Bootstrap(flags),
                _ => Unknown(args[0]),
            };
        }
        catch (Exception ex) when (ex is InvalidDataException or FileNotFoundException or DirectoryNotFoundException or ArgumentException or FormatException)
        {
            await Console.Error.WriteLineAsync("Configuration error: " + ex.Message);
            return ExitConfiguration;
        }
    }

    private static async Task<int> RunNodeAsync(NodeHostConfiguration configuration, NodeLogLevel logLevel)
    {
        NodeOptions options = configuration.ToNodeOptions();
        using X509Certificate2 certificate = LoadCertificate(configuration.CertificatePath, configuration.CertificatePassword, options.Name);
        var log = new ConsoleNodeLog(logLevel);
        Directory.CreateDirectory(configuration.StorageRoot);
        var environment = new NodeEnvironment
        {
            Medium = new FileSystemMedium(configuration.StorageRoot, configuration.CapacityBytes),
            TransportFactory = new QuiclyTransportFactory(new QuiclyTransportOptions(certificate, DevelopmentCertificates.ParsePins(configuration.TrustedPins))),
            Time = TimeProvider.System,
            Random = SystemRandomSource.Instance,
            Faults = NoFaults.Instance,
            Log = log,
            LoopFactory = new ThreadNodeLoopFactory(log),
        };

        using var control = new HostControl();
        NodeHost host;
        try
        {
            host = await NodeHost.StartAsync(options, environment, control.Stopping);
        }
        catch (Exception ex)
        {
            return Failed(options.Name, ex);
        }

        Ready(new HostReadyMessage(host.Name, host.Endpoint, host.Id.ToString(), host.Incarnation));
        await control.RunAsync(host.Pause, host.Resume);
        await host.StopAsync();
        return ExitOk;
    }

    private static async Task<int> RunBackupAsync(BackupHostConfiguration configuration, NodeLogLevel logLevel)
    {
        BackupNodeOptions options = configuration.ToBackupNodeOptions();
        using X509Certificate2 certificate = LoadCertificate(configuration.CertificatePath, configuration.CertificatePassword, options.Name);
        var log = new ConsoleNodeLog(logLevel);
        Directory.CreateDirectory(configuration.StorageRoot);
        var environment = new NodeEnvironment
        {
            Medium = new FileSystemMedium(configuration.StorageRoot, configuration.CapacityBytes),
            TransportFactory = new QuiclyTransportFactory(new QuiclyTransportOptions(certificate, DevelopmentCertificates.ParsePins(configuration.TrustedPins))),
            Time = TimeProvider.System,
            Random = SystemRandomSource.Instance,
            Faults = NoFaults.Instance,
            Log = log,
            LoopFactory = new ThreadNodeLoopFactory(log),
        };

        using var control = new HostControl();
        BackupNodeHost host;
        try
        {
            host = await BackupNodeHost.StartAsync(options, environment, control.Stopping);
        }
        catch (Exception ex)
        {
            return Failed(options.Name, ex);
        }

        Ready(new HostReadyMessage(host.Name, host.Endpoint, string.Empty, 0));
        await control.RunAsync(host.Pause, host.Resume);
        await host.StopAsync();
        return ExitOk;
    }

    /// <summary>Writes a new single-node cluster configuration (the recovery runbook's clean-host initialisation).</summary>
    private static int Bootstrap(Dictionary<string, string> flags)
    {
        string name = Require(flags, "name");
        string listen = Require(flags, "listen");
        string storage = Path.GetFullPath(Require(flags, "storage"));
        string output = Path.GetFullPath(Require(flags, "out"));
        if (!RpcAddress.TryParse(listen, out RpcAddress address, out string? error) || !address.IsQuic || address.Port == 0)
            throw new ArgumentException($"--listen must be a QUIC endpoint with a port ({error ?? listen}).");
        if (File.Exists(output))
            throw new ArgumentException($"{output} already exists; the bootstrap never overwrites a configuration.");

        var labels = new DfsTopologyLabels(Flag(flags, "region", "r1"), Flag(flags, "zone", "z1"), Flag(flags, "rack", "rk1"), Flag(flags, "host", name),
            Flag(flags, "device", "d1"));
        IRandomSource random = SystemRandomSource.Instance;
        Guid clusterId = random.NextGuid();
        NodeId nodeId = NodeId.New(random);
        byte[] secret = new byte[32];
        random.Fill(secret);
        byte[] adminSecretBytes = new byte[24];
        random.Fill(adminSecretBytes);
        string adminSecret = flags.TryGetValue("admin-secret", out string? given) && !string.IsNullOrEmpty(given)
            ? given
            : Convert.ToBase64String(adminSecretBytes);

        (byte[] pfx, byte[] pin) = DevelopmentCertificates.CreatePfx("CN=tdfs-" + name, TimeSpan.FromDays(365));
        string certificatePath = Path.ChangeExtension(output, ".pfx");
        Directory.CreateDirectory(Path.GetDirectoryName(output)!);
        File.WriteAllBytes(certificatePath, pfx);

        var configuration = new NodeHostConfiguration
        {
            Name = name,
            Labels = labels,
            ListenEndpoint = address.ToString(),
            StorageRoot = storage,
            ClusterSecret = Convert.ToBase64String(secret),
            Bootstrap = new ClusterBootstrap(clusterId, Flag(flags, "cluster-name", "tdfs"), [new BootstrapMember(nodeId, name, labels, [address.ToString()])],
                [name], [new BootstrapPrincipal("admin", adminSecret, ClusterAdmin: true)]),
            TimingsPreset = Flag(flags, "timings", "Production"),
            CertificatePath = certificatePath,
            TrustedPins = [Convert.ToHexString(pin)],
        };
        HostConfigurationJson.Write(output, configuration);
        Console.WriteLine(JsonSerializer.Serialize(new BootstrapSummary(output, certificatePath, Convert.ToHexString(pin), clusterId, nodeId.ToString(), "admin",
            adminSecret), HostConfigurationJson.Options));
        return ExitOk;
    }

    private static X509Certificate2 LoadCertificate(string? path, string? password, string name)
    {
        if (string.IsNullOrEmpty(path))
            return QuiclyTransportFactory.CreateDevelopmentCertificate("CN=tdfs-" + name, TimeSpan.FromDays(30));
        return DevelopmentCertificates.Load(File.ReadAllBytes(path), password);
    }

    private static void Ready(HostReadyMessage message)
    {
        Console.Out.WriteLine(HostConfigurationJson.ReadyPrefix + JsonSerializer.Serialize(message, OneLine));
        Console.Out.Flush();
    }

    private static int Failed(string name, Exception ex)
    {
        DfsErrorCode code = DfsErrors.ToCode(ex);
        Console.Out.WriteLine(HostConfigurationJson.FailedPrefix + JsonSerializer.Serialize(new HostFailedMessage(name, code.ToString(), ex.GetType().FullName ?? "?",
            ex.Message), OneLine));
        Console.Out.Flush();
        Console.Error.WriteLine(ex.ToString());
        return ex is DfsRecoveryKeyMissingException ? ExitRecoveryKeyMissing : ExitStartFailed;
    }

    private static Dictionary<string, string> ParseFlags(ReadOnlySpan<string> args)
    {
        var flags = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];
            if (!arg.StartsWith("--", StringComparison.Ordinal) || arg.Length < 3)
                throw new ArgumentException($"Unexpected argument '{arg}'.");
            if (i + 1 >= args.Length)
                throw new ArgumentException($"Option '{arg}' needs a value.");
            flags[arg[2..]] = args[++i];
        }

        return flags;
    }

    private static string Require(Dictionary<string, string> flags, string name) =>
        flags.TryGetValue(name, out string? value) && !string.IsNullOrWhiteSpace(value) ? value : throw new ArgumentException($"--{name} is required.");

    private static string Flag(Dictionary<string, string> flags, string name, string fallback) =>
        flags.TryGetValue(name, out string? value) && !string.IsNullOrWhiteSpace(value) ? value : fallback;

    private static NodeLogLevel LogLevel(Dictionary<string, string> flags) =>
        flags.TryGetValue("log", out string? value)
            ? Enum.TryParse(value, ignoreCase: true, out NodeLogLevel level) ? level : throw new ArgumentException($"Unknown log level '{value}'.")
            : NodeLogLevel.Info;

    private static int Unknown(string command)
    {
        Console.Error.WriteLine($"Unknown command '{command}'.");
        PrintUsage();
        return ExitUsage;
    }

    private static void PrintUsage() => Console.Error.WriteLine(string.Join(Environment.NewLine,
        "Usage:",
        "  Tedd.Tdfs.Host node --config <file.json> [--log trace|debug|info|warn|error]",
        "  Tedd.Tdfs.Host backup --config <file.json> [--log <level>]",
        "  Tedd.Tdfs.Host bootstrap --name <node> --listen quic://<ip>:<port> --storage <dir> --out <file.json>",
        "                           [--zone z1] [--region r1] [--rack rk1] [--host <host>] [--device d1]",
        "                           [--cluster-name <name>] [--admin-secret <secret>] [--timings Production|Fast]",
        "A running host reads 'stop', 'pause' and 'resume' from standard input.",
        string.Create(CultureInfo.InvariantCulture,
            $"Exit codes: {ExitOk} ok, {ExitUsage} usage, {ExitConfiguration} configuration, {ExitStartFailed} start failed, {ExitRecoveryKeyMissing} recovery key missing.")));

    /// <summary>What the bootstrap command prints.</summary>
    private sealed record BootstrapSummary(string Configuration, string Certificate, string CertificatePin, Guid ClusterId, string NodeId,
        string AdminPrincipal, string AdminSecret);
}

/// <summary>What a host prints (one line, prefixed <see cref="HostConfigurationJson.FailedPrefix"/>) when it could not start.</summary>
/// <param name="Name">Node name.</param>
/// <param name="Code">Error code of the failure.</param>
/// <param name="Type">Exception type.</param>
/// <param name="Message">Exception message.</param>
public sealed record HostFailedMessage(string Name, string Code, string Type, string Message);
