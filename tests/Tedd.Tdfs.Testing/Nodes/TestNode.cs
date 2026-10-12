using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Testing;

/// <summary>A live node of a <see cref="TestCluster"/>.</summary>
/// <remarks>
/// The node keeps its identity, clock and disk across process lifetimes; each start is a new process incarnation
/// (<see cref="Incarnation"/> + 1) that recovers from the disk's durable image. A crash discards the process (its loop,
/// heap and the disk's volatile state per <see cref="CrashMode"/>); a pause freezes the process with its memory; a stop is
/// graceful. In <see cref="TestClusterOptions.OutOfProcess"/> clusters the process is a real <c>Tedd.Tdfs.Host</c>
/// process (crash = process kill) and no in-process <see cref="Host"/> or <see cref="Inspect"/> exists.
/// </remarks>
public sealed partial class TestNode : IFaultTarget, IObjectStoreAccess
{
    private readonly object _gate = new();
    private readonly TestCluster? _cluster;
    private NodeHost? _host;
    private HostProcess? _process;
    private FencedMedium? _view;
    private IDisposable? _leadership;
    private TestNodeState _state = TestNodeState.Stopped;
    private ulong _incarnation;
    private string _endpoint = string.Empty;
    private int _starts;
    private bool _starting;
    private bool _startCrashed;

    internal TestNode(TestCluster cluster, TestNodeSpec spec, int index, NodeId id, NodeOptions options, TestClock clock, TestDisk disk,
        Tedd.Tdfs.Transport.IRpcTransportFactory transportFactory)
    {
        _cluster = cluster;
        Name = spec.Name;
        Index = index;
        Spec = spec;
        Id = id;
        Labels = spec.ToLabels();
        Options = options;
        Clock = clock;
        Disk = disk;
        TransportFactory = transportFactory;
        _endpoint = options.ListenEndpoint;
        disk.Owner = this;
    }

    /// <summary>Node name ("n0", ...).</summary>
    public string Name { get; }

    /// <summary>Creation index.</summary>
    public int Index { get; }

    /// <summary>Persistent node id.</summary>
    public NodeId Id { get; private set; }

    /// <summary>Topology labels.</summary>
    public DfsTopologyLabels Labels { get; } = new("r1", "z1", "rk1", "h", "d1");

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

    /// <summary>Current incarnation (incremented durably on every start).</summary>
    public ulong Incarnation
    {
        get
        {
            lock (_gate)
                return _incarnation;
        }
    }

    /// <summary>This node's clock.</summary>
    public TestClock Clock { get; }

    /// <summary>This node's disk.</summary>
    public TestDisk Disk { get; }

    /// <summary>Read-only diagnostics; throws <see cref="InvalidOperationException"/> unless <see cref="TestNodeState.Running"/>.</summary>
    public NodeInspector Inspect
    {
        get
        {
            lock (_gate)
            {
                if (_state != TestNodeState.Running)
                    throw new InvalidOperationException($"Node {Name} is {_state}; its inspector is only available while it runs.");
                return (_host ?? throw new InvalidOperationException($"Node {Name} runs out of process; it has no in-process inspector.")).Inspect;
            }
        }
    }

    /// <summary>The in-process host; null unless running or paused.</summary>
    public NodeHost? Host
    {
        get
        {
            lock (_gate)
                return _state is TestNodeState.Running or TestNodeState.Paused ? _host : null;
        }
    }

    /// <summary>The node's specification.</summary>
    internal TestNodeSpec Spec { get; }

    /// <summary>Options of the next start (reconfigurations persist).</summary>
    internal NodeOptions Options { get; private set; } = null!;

    /// <summary>The node's (wrapped) transport factory.</summary>
    internal Tedd.Tdfs.Transport.IRpcTransportFactory? TransportFactory { get; }

    /// <summary>The running out-of-process host, if any.</summary>
    internal HostProcess? Process
    {
        get
        {
            lock (_gate)
                return _process;
        }
    }

    /// <summary>The simulated process of the running incarnation (deterministic mode).</summary>
    public SimProcess? CurrentProcess
    {
        get
        {
            lock (_gate)
                return (_host?.Loop as SimNodeLoop)?.Process;
        }
    }

    private TestCluster Cluster => _cluster ?? throw new InvalidOperationException($"Node {Name} is not attached to a cluster.");

    /// <summary>Crashes the node abruptly; the disk keeps only what <paramref name="mode"/> allows.</summary>
    /// <remarks>No-op for a node that is not running or paused. Out of process: the process is killed (the operating system
    /// keeps its page cache, so the disk keeps what a process kill keeps).</remarks>
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
        NodeHost? host;
        HostProcess? process;
        lock (_gate)
        {
            if (_state is TestNodeState.Stopped or TestNodeState.Crashed)
                return;
            host = _host;
            process = _process;
        }

        Cluster.Record($"node {Name}: stop");
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
                Cluster.Record($"node {Name}: graceful stop did not finish within {Cluster.StopTimeout}; killed");
                host.Kill();
            }
            else if (stop.Exception is { } failure)
            {
                Cluster.Record($"node {Name}: stop reported {failure.GetBaseException().GetType().Name}: {failure.GetBaseException().Message}");
            }
        }

        lock (_gate)
        {
            _view?.Fence();
            _view = null;
            _host = null;
            _process = null;
            _state = TestNodeState.Stopped;
        }

        _leadership?.Dispose();
        _leadership = null;
    }

    /// <summary>Restarts the node from its durable image (incarnation + 1).</summary>
    /// <remarks>
    /// A running or paused node is stopped gracefully first. Completes once the new process recovered and serves (also when
    /// the node is isolated). A crash injected during startup completes the task with <see cref="State"/> =
    /// <see cref="TestNodeState.Crashed"/>.
    /// </remarks>
    public async Task RestartAsync(Func<NodeOptions, NodeOptions>? reconfigure = null)
    {
        if (State is TestNodeState.Running or TestNodeState.Paused)
            await StopAsync();
        if (reconfigure is not null)
            Options = reconfigure(Options) ?? throw new InvalidOperationException("The reconfiguration returned null options.");
        await StartAsync(CancellationToken.None);
    }

    /// <summary>Freezes the node loop.</summary>
    public Task PauseAsync()
    {
        if (!Pause("pause requested"))
            throw new InvalidOperationException($"Node {Name} is {State}; only a running node can be paused.");
        return Task.CompletedTask;
    }

    /// <summary>Unfreezes the node loop.</summary>
    public Task ResumeAsync()
    {
        NodeHost? host;
        HostProcess? process;
        lock (_gate)
        {
            if (_state != TestNodeState.Paused)
                return Task.CompletedTask;
            host = _host;
            process = _process;
            _state = TestNodeState.Running;
        }

        Cluster.Record($"node {Name}: resume");
        if (process is not null)
            return process.ResumeAsync();
        host?.Resume();
        return Task.CompletedTask;
    }

    /// <summary>Replaces the disk with an empty one (node must be stopped or crashed).</summary>
    public Task ReplaceDiskWithEmptyAsync()
    {
        RequireDown("replace its disk");
        SimulatedLossReport lost = Disk.Replace();
        Cluster.Record($"node {Name}: disk replaced with an empty one");
        if (!lost.IsEmpty)
            Cluster.RecordAssumptionViolation(new AssumptionViolation(AssumptionViolation.DurableFlush, Name, "disk replaced: " + lost));
        return Task.CompletedTask;
    }

    /// <summary>Clones another node's durable disk image onto this node (node must be stopped or crashed; RSY-021).</summary>
    public Task CloneDiskFromAsync(TestNode source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (ReferenceEquals(source, this))
            throw new ArgumentException("A node cannot clone its own disk.", nameof(source));
        RequireDown("clone a disk onto it");
        Disk.CloneFrom(source.Disk);
        Cluster.Record($"node {Name}: disk cloned from {source.Name}");
        return Task.CompletedTask;
    }

    /// <summary>Reads the node counters.</summary>
    public Task<NodeCounters> GetCountersAsync() => Inspect.GetCountersAsync();

    /// <inheritdoc />
    public override string ToString() => Name;

    // -------------------------------------------------------------------------------------------- harness internals

    /// <summary>Starts a new incarnation from the disk's durable image.</summary>
    internal async Task StartAsync(CancellationToken ct)
    {
        TestCluster cluster = Cluster;
        int start;
        lock (_gate)
        {
            if (_state is TestNodeState.Running or TestNodeState.Paused || _starting)
                throw new InvalidOperationException($"Node {Name} is already {(_starting ? "starting" : _state)}.");
            start = ++_starts;
            _starting = true;
            _startCrashed = false;
        }

        try
        {
            if (cluster.Options.OutOfProcess)
                await StartOutOfProcessAsync(cluster, ct);
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
        NodeEnvironment environment = cluster.CreateNodeEnvironment(this, view, start);
        lock (_gate)
            _view = view;
        cluster.Record($"node {Name}: start #{start.ToString(CultureInfo.InvariantCulture)}");

        NodeHost host;
        try
        {
            host = await NodeHost.StartAsync(Options, environment, ct);
        }
        catch (Exception ex) when (StartCrashed)
        {
            // An injected crash during startup: the node is down, which is what the test asked for.
            cluster.Record($"node {Name}: crashed during startup ({ex.GetType().Name})");
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

            cluster.Record($"node {Name}: start failed: {ex.GetType().Name}: {ex.Message}");
            throw;
        }

        bool crashedMeanwhile;
        lock (_gate)
        {
            crashedMeanwhile = _startCrashed || view.IsFenced;
            if (!crashedMeanwhile)
            {
                _host = host;
                _incarnation = host.Incarnation;
                _endpoint = host.Endpoint;
                Id = host.Id;
                _state = TestNodeState.Running;
            }
        }

        if (crashedMeanwhile)
        {
            host.Kill();
            return;
        }

        cluster.Record($"node {Name}: running, incarnation {host.Incarnation.ToString(CultureInfo.InvariantCulture)}");
        _leadership = await cluster.ObserveLeadershipAsync(this, host);
    }

    /// <summary>Replaces the options of the next start.</summary>
    internal void Reconfigure(Func<NodeOptions, NodeOptions> reconfigure) => Options = reconfigure(Options);

    private bool StartCrashed
    {
        get
        {
            lock (_gate)
                return _startCrashed;
        }
    }

    /// <summary>Marks a node whose process was started as running (out-of-process start).</summary>
    internal void OnProcessStarted(HostProcess process, ulong incarnation, string endpoint, NodeId id)
    {
        lock (_gate)
        {
            _incarnation = incarnation;
            _endpoint = endpoint;
            Id = id;
            if (!_startCrashed)
            {
                _process = process;
                _state = TestNodeState.Running;
                return;
            }
        }

        process.Kill(); // crashed while it was starting
    }

    /// <summary>Crashes the node (from the test, a fault or cluster disposal).</summary>
    internal void Crash(CrashMode mode, string reason)
    {
        NodeHost? host;
        HostProcess? process;
        FencedMedium? view;
        lock (_gate)
        {
            if (_starting && !_startCrashed)
            {
                _startCrashed = true; // the incarnation being started dies before it serves
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

        Cluster.Record($"node {Name}: crash ({mode}; {reason})");
        if (process is not null)
        {
            process.Kill();
        }
        else
        {
            view?.Fence();
            host?.Kill();
            SimulatedLossReport lost = Disk.Crash(mode);
            if (!lost.IsEmpty)
                Cluster.RecordAssumptionViolation(new AssumptionViolation(AssumptionViolation.DurableFlush, Name, $"crash ({mode}) lost {lost}"));
        }

        _leadership?.Dispose();
        _leadership = null;
    }

    /// <summary>
    /// Ends the process for cluster disposal: no graceful shutdown and no simulated power loss (the disk is left as is and no
    /// assumption violation is recorded). The node ends <see cref="TestNodeState.Stopped"/>.
    /// </summary>
    internal void Terminate()
    {
        NodeHost? host;
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
        _leadership?.Dispose();
        _leadership = null;
    }

    /// <summary>Pauses the running process; false when it is not running.</summary>
    internal bool Pause(string reason)
    {
        NodeHost? host;
        HostProcess? process;
        lock (_gate)
        {
            if (_state != TestNodeState.Running)
                return false;
            host = _host;
            process = _process;
            _state = TestNodeState.Paused;
        }

        Cluster.Record($"node {Name}: pause ({reason})");
        if (process is not null)
            _ = process.PauseAsync();
        else
            host?.Pause();
        return true;
    }

    void IFaultTarget.CrashFromFault(CrashMode mode, string reason) => Crash(mode, reason);

    bool IFaultTarget.PauseFromFault(string reason) => Pause(reason);

    void IFaultTarget.ResumeFromFault(string reason)
    {
        lock (_gate)
        {
            if (_state != TestNodeState.Paused)
                return;
        }

        Cluster.Record($"node {Name}: resume ({reason})");
        _ = ResumeAsync();
    }

    async Task<(ObjectPhysicalLocation Location, long PlaintextLength)?> IObjectStoreAccess.LocateAsync(ObjectId id)
    {
        NodeInspector inspector = Inspect;
        ObjectPhysicalLocation? location = await inspector.GetObjectLocationAsync(id);
        if (location is null)
            return null;
        ObjectRecordInfo? info = (await inspector.ListObjectsAsync()).FirstOrDefault(o => o.Id == id);
        return info is null ? null : (location, info.Length);
    }

    Task<bool> IObjectStoreAccess.DeleteLocallyAsync(ObjectId id) => Inspect.DeleteObjectLocallyAsync(id);

    private void RequireDown(string action)
    {
        if (State is not (TestNodeState.Stopped or TestNodeState.Crashed))
            throw new InvalidOperationException($"Node {Name} is {State}; stop or crash it before you {action}.");
    }

    private async Task StartOutOfProcessAsync(TestCluster cluster, CancellationToken ct)
    {
        cluster.Record($"node {Name}: start process");
        HostProcess process = await cluster.OutOfProcess.StartNodeAsync(this, ct);
        cluster.Record($"node {Name}: process running, incarnation {Incarnation.ToString(CultureInfo.InvariantCulture)}");
        _ = process;
    }
}
