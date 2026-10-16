using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Volumes;

/// <summary>
/// Opens and closes the volume groups this node participates in, following the applied control state: a node that is a
/// desired voter or learner of a volume opens its group (initial voters write the identical bootstrap membership; members
/// added later start empty and receive the log or a snapshot from the leader); a volume that was deleted, or no longer
/// lists this node and has stopped counting it, is closed and its local data removed.
/// </summary>
internal sealed class VolumeRegistry
{
    private readonly NodeServices _s;
    private readonly SortedDictionary<VolumeId, VolumeHost> _hosts = new(VolumeIdComparer.Instance);
    private readonly SortedDictionary<VolumeId, long> _unlistedSince = new(VolumeIdComparer.Instance);
    private readonly HashSet<VolumeId> _opening = [];
    private bool _scheduled;
    private bool _running;
    private bool _again;
    private bool _warnedNoFactory;
    private bool _stopped;

    public VolumeRegistry(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>Volume hosts on this node, in id order.</summary>
    public IReadOnlyList<VolumeHost> Hosts => [.. _hosts.Values];

    /// <summary>The host of a volume by id.</summary>
    public VolumeHost? TryGet(VolumeId id) => _hosts.GetValueOrDefault(id);

    /// <summary>The host of a volume by name.</summary>
    public VolumeHost? TryGet(string name)
    {
        foreach (VolumeHost host in _hosts.Values)
        {
            if (string.Equals(host.Name, name, StringComparison.Ordinal))
                return host;
        }

        return null;
    }

    /// <summary>The host of a volume group by wire group name.</summary>
    public VolumeHost? TryGetByGroup(string group)
    {
        foreach (VolumeHost host in _hosts.Values)
        {
            if (string.Equals(host.Group.Name, group, StringComparison.Ordinal))
                return host;
        }

        return null;
    }

    /// <summary>Hooks control-state changes and leadership changes; reconciles now.</summary>
    public void Start()
    {
        _s.Control.StateMachine.Changed += _ => Schedule();
        _s.Raft.LeadershipChanged += group =>
        {
            if (TryGetByGroup(group.Name) is { } host)
                host.NotifyLeadershipChanged();
        };
        Schedule();
    }

    /// <summary>Requests a reconciliation (coalesced, runs as separate loop work).</summary>
    public void Schedule()
    {
        if (_stopped)
            return;
        if (_running)
        {
            _again = true;
            return;
        }

        if (_scheduled)
            return;
        _scheduled = true;
        _s.Loop.Post(() =>
        {
            _scheduled = false;
            _ = ReconcileLoopAsync();
        });
    }

    /// <summary>Leader-side membership reconciliation of every volume group this node leads (periodic job).</summary>
    public async Task ReconcileMembershipAsync()
    {
        foreach (VolumeHost host in _hosts.Values.ToArray())
        {
            if (host.IsLeader && !host.Group.IsClosed)
                await host.ReconcileMembershipAsync();
        }
    }

    /// <summary>Closes every volume group (stop).</summary>
    public async Task CloseAllAsync()
    {
        _stopped = true;
        foreach (VolumeHost host in _hosts.Values.ToArray())
        {
            host.NotifyClosing();
            await _s.Raft.CloseAsync(host.Group.Name, deleteData: false);
        }

        _hosts.Clear();
    }

    private async Task ReconcileLoopAsync()
    {
        if (_running || _stopped)
            return;
        _running = true;
        try
        {
            do
            {
                _again = false;
                await ReconcileAsync();
            }
            while (_again && !_stopped);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _s.Error("volume", NodeServices.Inv($"Volume reconciliation failed: {ex.GetType().Name}: {ex.Message}"));
        }
        finally
        {
            _running = false;
        }
    }

    private async Task ReconcileAsync()
    {
        ControlState state = _s.Control.State;
        if (!state.IsBootstrapped)
            return;
        foreach (VolumeRecord record in state.Volumes.Values.ToArray())
        {
            if (_stopped)
                return;
            if (_hosts.TryGetValue(record.Id, out VolumeHost? host))
            {
                host.UpdateRecord(record);
                continue;
            }

            if (record.Hosts(_s.NodeId) && !_opening.Contains(record.Id))
                await OpenAsync(record);
        }

        long now = _s.Time.GetTimestamp();
        TimeSpan grace = _s.Timings.Tick * _s.Timings.ElectionTicksMax * 10;
        foreach (VolumeHost host in _hosts.Values.ToArray())
        {
            if (_stopped)
                return;
            VolumeRecord? record = state.FindVolume(host.Id);
            bool close;
            bool deleteData = true;
            if (record is null)
            {
                close = true;
            }
            else if (record.Hosts(_s.NodeId))
            {
                _unlistedSince.Remove(host.Id);
                close = false;
            }
            else
            {
                // Keep participating until the group stopped counting this node. A voter of the applied configuration keeps
                // hosting the group however long the transition takes (its replacement may still be catching up): leaving
                // would silently reduce the group's failure tolerance while the quorum denominator stays the same (RCV-01).
                // A voter removed while it was cut off may never learn of its removal; it then keeps an idle group, which is
                // harmless (members ignore a non-member's messages while they have a leader, and its log is behind theirs).
                // A learner stops participating after a grace period, but it keeps its log then: a member the configuration
                // may still count must never lose its log (re-listed later, it would rejoin with an empty log and could vote
                // for a candidate lacking committed entries). The data is deleted only once the applied (committed)
                // configuration no longer contains this node.
                RaftConfState applied = host.Group.AppliedConf;
                if (!_unlistedSince.TryGetValue(host.Id, out long since))
                {
                    since = now;
                    _unlistedSince[host.Id] = since;
                }

                bool counted = applied.Contains(_s.NodeId);
                close = !counted || (!applied.IsVoter(_s.NodeId) && _s.Time.GetElapsedTime(since, now) > grace);
                deleteData = !counted;
            }

            if (close)
            {
                _unlistedSince.Remove(host.Id);
                _hosts.Remove(host.Id);
                host.NotifyClosing();
                if (!deleteData)
                {
                    _s.Warn("volume", NodeServices.Inv(
                        $"Volume {host.Name}: no longer listed but still a member of the applied configuration; leaving the group and keeping its data."));
                }

                await _s.Raft.CloseAsync(host.Group.Name, deleteData);
            }
        }
    }

    private async Task OpenAsync(VolumeRecord record)
    {
        IVolumeStateMachineFactory? factory = _s.VolumeStateMachines;
        if (factory is null)
        {
            if (!_warnedNoFactory)
            {
                _warnedNoFactory = true;
                _s.Error("volume", "No volume state machine is available in this build; volume groups are not hosted.");
            }

            return;
        }

        _opening.Add(record.Id);
        try
        {
            var context = new VolumeHostContext { Services = _s, Record = record, GroupName = record.GroupName };
            IReplicatedStateMachine sm = factory.Create(context);
            IReadOnlyList<RaftEntry>? bootstrap = record.Creation.InitialVoters.Contains(_s.NodeId)
                ? RaftBootstrap.BuildEntries(record.Creation.InitialVoters, [], [])
                : null;
            string directory = _s.DataPath(ControlHost.StoragePathOf("vol-" + record.Id.Value.ToString("N")));
            var spec = new GroupOpenSpec(record.GroupName, record.Name, record.Name, directory, sm, bootstrap);
            GroupRuntime group = await _s.Raft.OpenAsync(spec);
            var host = new VolumeHost(_s, record, group, sm);
            _hosts[record.Id] = host;
            host.NotifyOpened();
        }
        catch (Exception ex) when (ex is DfsException or IOException or InvalidOperationException or ArgumentException)
        {
            _s.Error("volume", NodeServices.Inv($"Opening volume {record.Name} failed: {ex.GetType().Name}: {ex.Message}"));
        }
        finally
        {
            _opening.Remove(record.Id);
        }
    }
}
