using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Testing;

/// <summary>A late-sync backup node of a <see cref="TestCluster"/>.</summary>
/// <remarks>
/// Lifecycle as for <see cref="TestNode"/> (crash, stop, restart, pause), driving a <see cref="BackupNodeHost"/>. The node
/// has its own principals: an export principal (used by the live cluster's exporter, no retention rights), a reader and a
/// retention administrator; <see cref="RegisterAsync"/> registers it as a backup target of a live volume.
/// </remarks>
public sealed partial class TestBackupNode : IFaultTarget, IObjectStoreAccess
{
    private readonly object _gate = new();
    private readonly TestCluster? _cluster;
    private BackupNodeHost? _host;
    private HostProcess? _process;
    private FencedMedium? _view;
    private TestNodeState _state = TestNodeState.Stopped;
    private string _endpoint = string.Empty;
    private int _starts;
    private bool _starting;
    private bool _startCrashed;

    internal TestBackupNode(TestCluster cluster, string name, int index, BackupNodeOptions options, TestClock clock, TestDisk disk,
        Tedd.Tdfs.Transport.IRpcTransportFactory transportFactory, DfsCredentials export, DfsCredentials reader, DfsCredentials retention)
    {
        _cluster = cluster;
        Name = name;
        Index = index;
        Options = options;
        Clock = clock;
        Disk = disk;
        TransportFactory = transportFactory;
        ExportCredentials = export;
        ReaderCredentials = reader;
        RetentionAdminCredentials = retention;
        _endpoint = options.ListenEndpoint;
        disk.Owner = this;
    }

    /// <summary>Node name ("b0", ...).</summary>
    public string Name { get; }

    /// <summary>Creation index.</summary>
    public int Index { get; }

    /// <summary>Bound endpoint.</summary>
    public string Endpoint
    {
        get
        {
            lock (_gate)
                return _endpoint;
        }
    }

    /// <summary>Lifecycle state.</summary>
    public TestNodeState State
    {
        get
        {
            lock (_gate)
                return _state;
        }
    }

    /// <summary>This node's disk.</summary>
    public TestDisk Disk { get; }

    /// <summary>This node's clock.</summary>
    public TestClock Clock { get; }

    /// <summary>Read-only diagnostics; throws <see cref="InvalidOperationException"/> unless <see cref="TestNodeState.Running"/>.</summary>
    public BackupInspector Inspect
    {
        get
        {
            lock (_gate)
            {
                if (_state != TestNodeState.Running)
                    throw new InvalidOperationException($"Backup node {Name} is {_state}; its inspector is only available while it runs.");
                return (_host ?? throw new InvalidOperationException($"Backup node {Name} runs out of process; it has no in-process inspector.")).Inspect;
            }
        }
    }

    /// <summary>Registration name used with <see cref="DfsBackupClient"/> ("b0", ...).</summary>
    public string TargetName => Name;

    /// <summary>Export principal credentials (no retention rights).</summary>
    public DfsCredentials ExportCredentials { get; }

    /// <summary>Reader principal credentials.</summary>
    public DfsCredentials ReaderCredentials { get; }

    /// <summary>Retention-admin principal credentials.</summary>
    public DfsCredentials RetentionAdminCredentials { get; }

    /// <summary>The in-process host; null unless running or paused.</summary>
    internal BackupNodeHost? Host
    {
        get
        {
            lock (_gate)
                return _state is TestNodeState.Running or TestNodeState.Paused ? _host : null;
        }
    }

    /// <summary>Options of the next start (reconfigurations persist).</summary>
    internal BackupNodeOptions Options { get; private set; } = null!;

    /// <summary>The node's (wrapped) transport factory.</summary>
    internal Tedd.Tdfs.Transport.IRpcTransportFactory? TransportFactory { get; }

    /// <summary>The simulated process of the running incarnation (deterministic mode).</summary>
    public SimProcess? CurrentProcess => Cluster.Runtime.Scheduler?.FindProcess(Name);

    private TestCluster Cluster => _cluster ?? throw new InvalidOperationException($"Backup node {Name} is not attached to a cluster.");

    /// <summary>Crashes the node abruptly.</summary>
    public Task CrashAsync(CrashMode mode = CrashMode.LoseUnflushed)
    {
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown crash mode.");
        Crash(mode, "crash requested");
        return Task.CompletedTask;
    }

    /// <summary>Stops the node gracefully.</summary>
    public async Task StopAsync()
    {
        BackupNodeHost? host;
        HostProcess? process;
        lock (_gate)
        {
            if (_state is TestNodeState.Stopped or TestNodeState.Crashed)
                return;
            host = _host;
            process = _process;
        }

        Cluster.Record($"backup {Name}: stop");
        if (process is not null)
        {
            await process.StopAsync(Cluster.StopTimeout);
        }
        else if (host is not null)
        {
            host.Resume();
            Task stop = host.StopAsync();
            if (!await Cluster.Runtime.WaitAsync(stop, Cluster.StopTimeout))
            {
                Cluster.Record($"backup {Name}: graceful stop did not finish within {Cluster.StopTimeout}; killed");
                host.Kill();
            }
        }

        lock (_gate)
        {
            // A crash during the graceful stop already ended this process: the node stays Crashed.
            if (!ReferenceEquals(_host, host) || !ReferenceEquals(_process, process))
                return;
            _view?.Fence();
            _view = null;
            _host = null;
            _process = null;
            _state = TestNodeState.Stopped;
        }

        Cluster.Fabric.ClearInventoryClaims(Name);
    }

    /// <summary>Restarts the node from its durable image.</summary>
    public async Task RestartAsync()
    {
        if (State is TestNodeState.Running or TestNodeState.Paused)
            await StopAsync();
        await StartAsync(CancellationToken.None);
    }

    /// <summary>Freezes the node loop.</summary>
    public Task PauseAsync()
    {
        if (!Pause("pause requested"))
            throw new InvalidOperationException($"Backup node {Name} is {State}; only a running node can be paused.");
        return Task.CompletedTask;
    }

    /// <summary>Unfreezes the node loop.</summary>
    public Task ResumeAsync()
    {
        BackupNodeHost? host;
        HostProcess? process;
        lock (_gate)
        {
            if (_state != TestNodeState.Paused)
                return Task.CompletedTask;
            host = _host;
            process = _process;
            _state = TestNodeState.Running;
        }

        Cluster.Record($"backup {Name}: resume");
        if (process is not null)
            return process.ResumeAsync();
        host?.Resume();
        return Task.CompletedTask;
    }

    /// <summary>Permanently destroys the disk.</summary>
    /// <remarks>The node goes down (crashes when it was running) and its disk is replaced by an empty one.</remarks>
    public Task DestroyDiskAsync()
    {
        Crash(CrashMode.LoseUnflushed, "disk destroyed");
        SimulatedLossReport lost = Disk.Replace();
        lock (_gate)
            _state = TestNodeState.Crashed;
        Cluster.Record($"backup {Name}: disk destroyed");
        if (!lost.IsEmpty)
            Cluster.RecordAssumptionViolation(new AssumptionViolation(AssumptionViolation.DurableFlush, Name, "disk destroyed: " + lost));
        return Task.CompletedTask;
    }

    /// <summary>Removes the backup recovery key material (RST-005).</summary>
    /// <remarks>
    /// The node's backup secret (its KEK root) is removed from its configuration; a running node is restarted without it, and
    /// a start that the node refuses for missing key material (<see cref="DfsRecoveryKeyMissingException"/>) leaves it
    /// <see cref="TestNodeState.Stopped"/>.
    /// </remarks>
    public async Task RemoveRecoveryKeyAsync()
    {
        bool wasRunning = State is TestNodeState.Running or TestNodeState.Paused;
        if (wasRunning)
            await StopAsync();
        Options = Options with { BackupSecret = ReadOnlyMemory<byte>.Empty };
        Cluster.Record($"backup {Name}: recovery key material removed");
        if (!wasRunning)
            return;
        try
        {
            await StartAsync(CancellationToken.None);
        }
        catch (DfsRecoveryKeyMissingException ex)
        {
            Cluster.Record($"backup {Name}: refuses to start without key material ({ex.Message})");
        }
    }

    /// <summary>Registers this target on a live volume.</summary>
    /// <remarks>
    /// Registered through the cluster's admin client (<c>DfsBackupClient.RegisterTargetAsync</c>) with this node's endpoint
    /// and export credentials. Defaults: no periodic schedule (synchronisation on request) and budgets of one hour of pin
    /// age with unlimited pinned bytes and rate.
    /// </remarks>
    public async Task RegisterAsync(string volume, DfsBackupSchedule? schedule = null, DfsBackupBudgets? budgets = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        DfsClient admin = Cluster.AdminClientForHarness;
        var registration = new DfsBackupTargetRegistration(TargetName, [Endpoint], ExportCredentials,
            schedule ?? new DfsBackupSchedule(), budgets ?? new DfsBackupBudgets(TimeSpan.FromHours(1), long.MaxValue));
        await admin.GetVolume(volume).Backups.RegisterTargetAsync(registration);
        Cluster.Record($"backup {Name}: registered as target of volume {volume}");
    }

    /// <inheritdoc />
    public override string ToString() => Name;

    // -------------------------------------------------------------------------------------------- harness internals

    /// <summary>Starts a new process from the disk's durable image.</summary>
    internal async Task StartAsync(CancellationToken ct)
    {
        TestCluster cluster = Cluster;
        int start;
        lock (_gate)
        {
            if (_state is TestNodeState.Running or TestNodeState.Paused || _starting)
                throw new InvalidOperationException($"Backup node {Name} is already {(_starting ? "starting" : _state)}.");
            start = ++_starts;
            _starting = true;
            _startCrashed = false;
        }

        cluster.Fabric.ClearInventoryClaims(Name);
        try
        {
            if (cluster.Options.OutOfProcess)
                await cluster.OutOfProcess.StartBackupAsync(this, ct);
            else
                await StartInProcessAsync(cluster, start, ct);
        }
        finally
        {
            lock (_gate)
                _starting = false;
        }
    }

    /// <summary>True when the node is crashed and no new incarnation is starting (or the starting one crashed too).</summary>
    bool IFaultTarget.IsCrashedIncarnation
    {
        get
        {
            lock (_gate)
                return _state == TestNodeState.Crashed && (!_starting || _startCrashed);
        }
    }

    private async Task StartInProcessAsync(TestCluster cluster, int start, CancellationToken ct)
    {
        var view = new FencedMedium(Disk.Medium, $"{Name} (start {start.ToString(CultureInfo.InvariantCulture)})");
        if (Disk.Simulated is { } sim)
            sim.TimeProvider = Clock;
        else if (Disk.Medium is FaultingFileSystemMedium fs)
            fs.TimeProvider = Clock;
        NodeEnvironment environment = cluster.CreateBackupEnvironment(this, view, start);
        lock (_gate)
            _view = view;
        cluster.Record($"backup {Name}: start #{start.ToString(CultureInfo.InvariantCulture)}");
        await PortAllocator.WaitUntilFreeAsync(Options.ListenEndpoint, TimeSpan.FromSeconds(10), cluster.Runtime);

        BackupNodeHost host;
        try
        {
            host = await BackupNodeHost.StartAsync(Options, environment, ct);
        }
        catch (Exception ex) when (StartCrashed)
        {
            cluster.Record($"backup {Name}: crashed during startup ({ex.GetType().Name})");
            return;
        }
        catch (Exception ex)
        {
            view.Fence();
            lock (_gate)
            {
                _view = null;
                _state = TestNodeState.Stopped;
            }

            cluster.Record($"backup {Name}: start failed: {ex.GetType().Name}: {ex.Message}");
            throw;
        }

        lock (_gate)
        {
            if (_startCrashed || view.IsFenced)
            {
                host.Kill();
                return;
            }

            _host = host;
            _endpoint = host.Endpoint;
            _state = TestNodeState.Running;
        }

        cluster.Record($"backup {Name}: running");
    }

    private bool StartCrashed
    {
        get
        {
            lock (_gate)
                return _startCrashed;
        }
    }

    /// <summary>Replaces the options of the next start.</summary>
    internal void Reconfigure(Func<BackupNodeOptions, BackupNodeOptions> reconfigure) =>
        Options = reconfigure(Options) ?? throw new InvalidOperationException("The reconfiguration returned null options.");

    /// <summary>Marks a backup node whose process was started as running (out-of-process start).</summary>
    internal void OnProcessStarted(HostProcess process, string endpoint)
    {
        lock (_gate)
        {
            _endpoint = endpoint;
            if (!_startCrashed)
            {
                _process = process;
                _state = TestNodeState.Running;
                return;
            }
        }

        process.Kill(); // crashed while it was starting
    }

    internal void Crash(CrashMode mode, string reason)
    {
        BackupNodeHost? host;
        HostProcess? process;
        FencedMedium? view;
        lock (_gate)
        {
            if (_starting && !_startCrashed)
            {
                _startCrashed = true;
            }
            else if (_state is TestNodeState.Stopped or TestNodeState.Crashed)
            {
                return;
            }

            host = _host;
            process = _process;
            view = _view;
            _host = null;
            _process = null;
            _view = null;
            _state = TestNodeState.Crashed;
        }

        Cluster.Record($"backup {Name}: crash ({mode}; {reason})");
        Cluster.Fabric.ClearInventoryClaims(Name);
        if (process is not null)
        {
            process.Kill();
            return;
        }

        view?.Fence();
        host?.Kill();
        SimulatedLossReport lost = Disk.Crash(mode);
        if (!lost.IsEmpty)
            Cluster.RecordAssumptionViolation(new AssumptionViolation(AssumptionViolation.DurableFlush, Name, $"crash ({mode}) lost {lost}"));
    }

    /// <summary>Ends the process for cluster disposal (no graceful shutdown, no simulated power loss).</summary>
    internal void Terminate()
    {
        BackupNodeHost? host;
        HostProcess? process;
        FencedMedium? view;
        lock (_gate)
        {
            host = _host;
            process = _process;
            view = _view;
            _host = null;
            _process = null;
            _view = null;
            if (_state is TestNodeState.Running or TestNodeState.Paused)
                _state = TestNodeState.Stopped;
        }

        view?.Fence();
        host?.Kill();
        process?.Kill();
    }

    internal bool Pause(string reason)
    {
        BackupNodeHost? host;
        HostProcess? process;
        lock (_gate)
        {
            if (_state != TestNodeState.Running)
                return false;
            host = _host;
            process = _process;
            _state = TestNodeState.Paused;
        }

        Cluster.Record($"backup {Name}: pause ({reason})");
        if (process is not null)
            _ = process.PauseAsync();
        else
            host?.Pause();
        return true;
    }

    async Task<(ObjectPhysicalLocation Location, long PlaintextLength)?> IObjectStoreAccess.LocateAsync(ObjectId id)
    {
        BackupNodeHost host = Host ?? throw new InvalidOperationException($"Backup node {Name} is {State}; its object store is only reachable while it runs in process.");
        return await host.Inspect.LocateObjectAsync(id);
    }

    Task<bool> IObjectStoreAccess.DeleteLocallyAsync(ObjectId id)
    {
        BackupNodeHost host = Host ?? throw new InvalidOperationException($"Backup node {Name} is {State}; its object store is only reachable while it runs in process.");
        return host.Inspect.DeleteObjectLocallyAsync(id);
    }

    void IFaultTarget.CrashFromFault(CrashMode mode, string reason) => Crash(mode, reason);

    bool IFaultTarget.PauseFromFault(string reason) => Pause(reason);

    void IFaultTarget.ResumeFromFault(string reason)
    {
        if (State != TestNodeState.Paused)
            return;
        Cluster.Record($"backup {Name}: resume ({reason})");
        _ = ResumeAsync();
    }
}
