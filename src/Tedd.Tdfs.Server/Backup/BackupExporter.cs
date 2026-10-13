using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Server.Volumes.State;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// Node-level registry of backup exporters (ARCHITECTURE §12): one <see cref="VolumeExporter"/> per volume this node leads,
/// bound to the leader's term. Exporters run on the node loop, read only applied (committed) volume state and change it only
/// through committed volume entries (transfer pins, progress, lineage abandonment), so a new leader resumes exactly where the
/// replicated state says the previous one stood.
/// </summary>
internal sealed class BackupExporters
{
    private readonly NodeServices _s;
    private readonly Dictionary<VolumeId, VolumeExporter> _volumes = [];
    private IRpcTransport? _transport;
    private bool _jobStarted;

    public BackupExporters(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>The transport outbound export connections are opened on.</summary>
    public IRpcTransport Transport => _transport ?? throw new InvalidOperationException("The backup exporters have no transport.");

    /// <summary>Binds the node's transport.</summary>
    public void Attach(IRpcTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _transport = transport;
    }

    /// <summary>
    /// Volume hook (leadership or control record changed): starts the periodic reconciliation the first time this node hosts
    /// a volume with backup work — a registered target, or backup progress or transfer pins left in its state. Nodes of
    /// clusters that use no backup run no backup job at all.
    /// </summary>
    public void OnVolumeChanged(VolumeService service)
    {
        ArgumentNullException.ThrowIfNull(service);
        if (_jobStarted || _s.IsStopping || !HasBackupWork(service))
            return;
        _jobStarted = true;
        _s.Jobs.StartPeriodic("backup-exporters", _s.Timings.Tick * 20, ReconcileAsync);
    }

    private static bool HasBackupWork(VolumeService service)
    {
        if (service.Record.BackupTargets.Count > 0)
            return true;
        VolumeState state = service.CommittedState;
        return !state.BackupProgress.IsEmpty || state.Pins.Values.Any(p => p.Kind == PinKind.Transfer);
    }

    /// <summary>The exporter of a volume this node leads (created on demand for the current term); null when not leading.</summary>
    public VolumeExporter? For(VolumeService service)
    {
        ArgumentNullException.ThrowIfNull(service);
        if (!service.IsLeader || _s.IsStopping || _transport is null)
            return null;
        ulong term = service.Group.Term;
        VolumeId id = service.Record.Id;
        if (_volumes.TryGetValue(id, out VolumeExporter? exporter))
        {
            if (ReferenceEquals(exporter.Service, service) && exporter.Term == term && !exporter.IsStopped)
                return exporter;
            exporter.Stop();
        }

        exporter = new VolumeExporter(this, _s, service, term);
        _volumes[id] = exporter;
        return exporter;
    }

    /// <summary>The running exporter of a volume, or null.</summary>
    public VolumeExporter? Find(VolumeId volume) => _volumes.TryGetValue(volume, out VolumeExporter? e) && !e.IsStopped ? e : null;

    /// <summary>Periodic job: starts exporters of led volumes, reconciles their targets and stops exporters of lost leaderships.</summary>
    public async Task ReconcileAsync()
    {
        if (_s.IsStopping)
        {
            StopAll();
            return;
        }

        var led = new HashSet<VolumeId>();
        foreach (VolumeHost host in _s.Volumes.Hosts)
        {
            if (host.Service is not { IsLeader: true } service || host.Group.IsClosed)
                continue;
            led.Add(service.Record.Id);
            if (For(service) is { } exporter)
                await exporter.ReconcileAsync();
        }

        foreach (KeyValuePair<VolumeId, VolumeExporter> pair in _volumes.ToArray())
        {
            if (!led.Contains(pair.Key) || !pair.Value.IsCurrent)
            {
                pair.Value.Stop();
                _volumes.Remove(pair.Key);
            }
        }
    }

    /// <summary>Stops every exporter (node stopping).</summary>
    public void StopAll()
    {
        foreach (VolumeExporter exporter in _volumes.Values)
            exporter.Stop();
        _volumes.Clear();
    }
}

/// <summary>
/// The exporter of one volume for one leadership term: a <see cref="TargetExporter"/> per registered backup target, and the
/// clean-up of volume state left by removed targets.
/// </summary>
internal sealed class VolumeExporter
{
    private readonly SortedDictionary<string, TargetExporter> _targets = new(StringComparer.Ordinal);
    private readonly HashSet<string> _cleaning = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _cts = new();

    public VolumeExporter(BackupExporters owner, NodeServices services, VolumeService service, ulong term)
    {
        Owner = owner;
        S = services;
        Service = service;
        Term = term;
    }

    public BackupExporters Owner { get; }

    public NodeServices S { get; }

    public VolumeService Service { get; }

    public ulong Term { get; }

    public bool IsStopped { get; private set; }

    /// <summary>Cancelled when the exporter stops (leadership lost, node stopping).</summary>
    public CancellationToken Stopping => _cts.Token;

    /// <summary>True while this node still leads the volume in the exporter's term.</summary>
    public bool IsCurrent => !IsStopped && Service.IsLeader && Service.Group.Term == Term;

    /// <summary>The runner of a registered target (created on demand), or null when the target is not registered.</summary>
    public TargetExporter? Target(string name)
    {
        if (!IsCurrent || !Service.Record.BackupTargets.ContainsKey(name))
            return null;
        if (!_targets.TryGetValue(name, out TargetExporter? target))
        {
            target = new TargetExporter(this, name);
            _targets[name] = target;
            target.Start();
        }

        return target;
    }

    /// <summary>Starts runners of new targets, stops runners of removed ones and forgets the state of removed targets.</summary>
    public async Task ReconcileAsync()
    {
        if (!IsCurrent)
        {
            Stop();
            return;
        }

        IReadOnlyDictionary<string, BackupTargetRecord> registered = Service.Record.BackupTargets;
        foreach (string name in registered.Keys)
            Target(name);
        foreach (KeyValuePair<string, TargetExporter> pair in _targets.ToArray())
        {
            if (!registered.ContainsKey(pair.Key))
            {
                pair.Value.Stop();
                _targets.Remove(pair.Key);
            }
        }

        if (!Service.IsReadyLeader)
            return;
        VolumeState state = Service.CommittedState;
        var leftovers = new SortedSet<string>(StringComparer.Ordinal);
        foreach (string name in state.BackupProgress.Keys)
        {
            if (!registered.ContainsKey(name))
                leftovers.Add(name);
        }

        foreach (PinRecord pin in state.Pins.Values)
        {
            if (pin.Kind == PinKind.Transfer && pin.Target is { } t && !registered.ContainsKey(t))
                leftovers.Add(t);
        }

        if (leftovers.Count == 0)
            return;

        // A lagging control replica could miss a recent registration: confirm against the control leader first.
        try
        {
            await Service.WaitForControlAsync(Stopping);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        foreach (string name in leftovers)
        {
            if (Service.Record.BackupTargets.ContainsKey(name) || !_cleaning.Add(name))
                continue;
            try
            {
                await Service.ProposeBackupHousekeepingAsync(new RemoveBackupTargetStateCommand(name));
            }
            finally
            {
                _cleaning.Remove(name);
            }
        }
    }

    /// <summary>Wakes the runner of <paramref name="target"/> (a sync was requested).</summary>
    public void Wake(string target) => Target(target)?.Wake(resetBackoff: true);

    /// <summary>Stops every runner of this exporter.</summary>
    public void Stop()
    {
        if (IsStopped)
            return;
        IsStopped = true;
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already stopped.
        }

        foreach (TargetExporter target in _targets.Values)
            target.Stop();
        _targets.Clear();
    }
}
