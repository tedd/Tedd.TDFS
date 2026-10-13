using System.Globalization;
using Tedd.Tdfs.Host;
using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Starts the nodes of an out-of-process cluster as real <c>Tedd.Tdfs.Host</c> processes (QUIC + real disks): writes each
/// start's JSON configuration under the data root, launches <c>dotnet exec Tedd.Tdfs.Host.dll node|backup --config ...</c>,
/// waits for the process to serve and hands the process to the <see cref="TestNode"/> / <see cref="TestBackupNode"/>.
/// </summary>
/// <remarks>
/// The host assembly is found through the <c>TDFS_HOST_DLL</c> environment variable, next to the test assembly (when its
/// runtime configuration was copied too), or in <c>src/Tedd.Tdfs.Host/bin/&lt;configuration&gt;/&lt;tfm&gt;/</c> of the
/// repository the tests run from. Network and fault injection of the harness act on the harness's own transports only:
/// inside the processes there is no link policy and no fault injector.
/// </remarks>
internal sealed class OutOfProcessLauncher : IAsyncDisposable
{
    private readonly TestCluster _cluster;
    private readonly List<HostProcess> _processes = [];
    private readonly string _configDirectory;
    private int _starts;

    public OutOfProcessLauncher(TestCluster cluster)
    {
        _cluster = cluster;
        HostDll = LocateHost();
        _configDirectory = Path.Combine(cluster.DataRoot, "host-config");
        Directory.CreateDirectory(_configDirectory);
        LivePfxPath = WritePfx("live", cluster.LiveCertificate);
        BackupPfxPath = WritePfx("backup", cluster.BackupCertificate);
    }

    /// <summary>Path of the host assembly.</summary>
    public string HostDll { get; }

    /// <summary>Certificate file of live nodes.</summary>
    public string LivePfxPath { get; }

    /// <summary>Certificate file of backup nodes.</summary>
    public string BackupPfxPath { get; }

    /// <summary>Bound on a process becoming ready.</summary>
    public TimeSpan StartTimeout { get; } = TimeSpan.FromSeconds(90);

    /// <summary>Starts a live node process from the node's current options and its disk directory.</summary>
    public async Task<HostProcess> StartNodeAsync(TestNode node, CancellationToken ct)
    {
        NodeOptions o = node.Options;
        var configuration = new NodeHostConfiguration
        {
            Name = o.Name,
            Labels = o.Labels,
            ListenEndpoint = o.ListenEndpoint,
            StorageRoot = node.Disk.RootDirectory ?? throw new InvalidOperationException($"Node {node.Name} has no disk directory."),
            DataDirectory = o.DataDirectory,
            ClusterSecret = Convert.ToBase64String(o.ClusterSecret.Span),
            Bootstrap = o.Bootstrap,
            JoinSeeds = o.JoinSeeds,
            Timings = o.Timings,
            CapacityBytes = o.CapacityBytes,
            ReservedBytes = o.ReservedBytes,
            EncryptAtRest = o.EncryptAtRest,
            Mutations = o.Mutations,
            CertificatePath = LivePfxPath,
            TrustedPins = _cluster.TrustedPins.Select(Convert.ToHexString).ToArray(),
        };
        string path = ConfigPath(node.Name);
        HostConfigurationJson.Write(path, configuration);
        (HostProcess process, HostReadyMessage ready) = await HostProcess.StartAsync(HostDll, "node", path, node.Name, StartTimeout, _cluster.Record, ct);
        Track(process);
        node.OnProcessStarted(process, ready.Incarnation, ready.Endpoint, NodeId.Parse(ready.NodeId));
        return process;
    }

    /// <summary>Starts a backup node process.</summary>
    public async Task<HostProcess> StartBackupAsync(TestBackupNode node, CancellationToken ct)
    {
        BackupNodeOptions o = node.Options;
        var configuration = new BackupHostConfiguration
        {
            Name = o.Name,
            ListenEndpoint = o.ListenEndpoint,
            StorageRoot = node.Disk.RootDirectory ?? throw new InvalidOperationException($"Backup node {node.Name} has no disk directory."),
            DataDirectory = o.DataDirectory,
            SourceClusterId = o.SourceClusterId,
            BackupSecret = Convert.ToBase64String(o.BackupSecret.Span),
            Principals = o.Principals,
            Grants = o.Grants,
            KeepLastSnapshots = o.KeepLastSnapshots,
            CapacityBytes = o.CapacityBytes,
            Timings = o.Timings,
            CertificatePath = BackupPfxPath,
            TrustedPins = _cluster.TrustedPins.Select(Convert.ToHexString).ToArray(),
        };
        string path = ConfigPath(node.Name);
        HostConfigurationJson.Write(path, configuration);
        (HostProcess process, HostReadyMessage ready) = await HostProcess.StartAsync(HostDll, "backup", path, node.Name, StartTimeout, _cluster.Record, ct);
        Track(process);
        node.OnProcessStarted(process, ready.Endpoint);
        return process;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        HostProcess[] processes;
        lock (_processes)
        {
            processes = [.. _processes];
            _processes.Clear();
        }

        foreach (HostProcess process in processes)
            await process.DisposeAsync();
    }

    private void Track(HostProcess process)
    {
        lock (_processes)
            _processes.Add(process);
    }

    private string ConfigPath(string name) =>
        Path.Combine(_configDirectory, string.Create(CultureInfo.InvariantCulture, $"{name}-{Interlocked.Increment(ref _starts)}.json"));

    private string WritePfx(string role, TestCluster.QuicCertificate? certificate)
    {
        if (certificate is null)
            throw new InvalidOperationException("Out-of-process clusters use QUIC certificates.");
        string path = Path.Combine(_configDirectory, role + ".pfx");
        File.WriteAllBytes(path, certificate.Pfx);
        return path;
    }

    /// <summary>Finds <c>Tedd.Tdfs.Host.dll</c> (see the remarks of the class).</summary>
    internal static string LocateHost()
    {
        const string dll = "Tedd.Tdfs.Host.dll";
        string? configured = Environment.GetEnvironmentVariable("TDFS_HOST_DLL");
        if (!string.IsNullOrWhiteSpace(configured))
        {
            return File.Exists(configured)
                ? Path.GetFullPath(configured)
                : throw new InvalidOperationException($"TDFS_HOST_DLL points to {configured}, which does not exist.");
        }

        string baseDir = AppContext.BaseDirectory;
        string local = Path.Combine(baseDir, dll);
        if (File.Exists(local) && File.Exists(Path.Combine(baseDir, "Tedd.Tdfs.Host.runtimeconfig.json")))
            return local;

        // .../tests/<project>/bin/<configuration>/<tfm>/ -> repository root
        var dir = new DirectoryInfo(baseDir);
        string tfm = dir.Name;
        string configuration = dir.Parent?.Name ?? "Debug";
        for (DirectoryInfo? d = dir; d is not null; d = d.Parent)
        {
            string candidate = Path.Combine(d.FullName, "src", "Tedd.Tdfs.Host", "bin", configuration, tfm, dll);
            if (File.Exists(candidate))
                return candidate;
        }

        throw new InvalidOperationException(
            $"{dll} was not found (set TDFS_HOST_DLL, or build src/Tedd.Tdfs.Host in the {configuration} configuration).");
    }
}
