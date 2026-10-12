using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server;

/// <summary>A cluster member as applied by one node's control state (test harness view).</summary>
/// <param name="Id">Node id.</param>
/// <param name="Name">Node name.</param>
/// <param name="Labels">Topology labels.</param>
/// <param name="Endpoints">Registered endpoints.</param>
/// <param name="Status">Membership status name (Active, Draining, Drained, Removed).</param>
/// <param name="Incarnation">Registered incarnation.</param>
/// <param name="IsLive">False once removed.</param>
/// <param name="IsPlacementEligible">True when new data and voter roles may be placed on the node.</param>
internal sealed record HarnessMemberSummary(NodeId Id, string Name, DfsTopologyLabels Labels, IReadOnlyList<string> Endpoints, string Status,
    ulong Incarnation, bool IsLive, bool IsPlacementEligible);

/// <summary>A volume as applied by one node's control state (test harness view).</summary>
/// <param name="Id">Volume id.</param>
/// <param name="Name">Volume name.</param>
/// <param name="Epoch">Volume epoch.</param>
/// <param name="Group">Wire group name.</param>
/// <param name="State">Lifecycle state name.</param>
/// <param name="StoragePool">Storage pool.</param>
/// <param name="Voters">Desired voters (node names).</param>
/// <param name="Learners">Desired learners (node names).</param>
/// <param name="DefaultPolicy">Current default storage policy.</param>
/// <param name="NamedPolicies">Current named policies.</param>
internal sealed record HarnessVolumeSummary(VolumeId Id, string Name, ulong Epoch, string Group, string State, string StoragePool,
    IReadOnlyList<string> Voters, IReadOnlyList<string> Learners, DfsStoragePolicy DefaultPolicy, IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies);

/// <summary>A storage pool as applied by one node's control state.</summary>
/// <param name="Name">Pool name.</param>
/// <param name="Nodes">Member node names (ordinal).</param>
internal sealed record HarnessPoolSummary(string Name, IReadOnlyList<string> Nodes);

/// <summary>One node's applied control state, as the harness needs it (stability, placement oracle, pools).</summary>
/// <param name="ClusterId">Cluster id.</param>
/// <param name="AppliedIndex">Applied control index.</param>
/// <param name="PlacementEpoch">Placement epoch.</param>
/// <param name="ControlVoterTarget">Desired number of control voters.</param>
/// <param name="Members">Members in node-id order.</param>
/// <param name="Volumes">Volumes in volume-id order.</param>
/// <param name="Pools">Explicit pools by name.</param>
/// <param name="Features">Active features.</param>
internal sealed record HarnessControlSummary(Guid ClusterId, ulong AppliedIndex, ulong PlacementEpoch, int ControlVoterTarget,
    IReadOnlyList<HarnessMemberSummary> Members, IReadOnlyList<HarnessVolumeSummary> Volumes, IReadOnlyList<HarnessPoolSummary> Pools,
    IReadOnlyList<string> Features)
{
    /// <summary>The member with <paramref name="name"/>, or null.</summary>
    public HarnessMemberSummary? FindMember(string name) => Members.FirstOrDefault(m => string.Equals(m.Name, name, StringComparison.Ordinal));

    /// <summary>The volume with <paramref name="name"/> (not deleted), or null.</summary>
    public HarnessVolumeSummary? FindVolume(string name) => Volumes.FirstOrDefault(v => string.Equals(v.Name, name, StringComparison.Ordinal));
}

/// <summary>Process-level status of a node (test harness view).</summary>
/// <param name="NodeId">Node id.</param>
/// <param name="Incarnation">Incarnation of this process.</param>
/// <param name="IsServing">True once the node serves requests.</param>
/// <param name="IsStopping">True while stopping.</param>
/// <param name="IsSelfRegistered">True once this incarnation is registered with the control group (receipts allowed).</param>
internal sealed record HarnessNodeStatus(NodeId NodeId, ulong Incarnation, bool IsServing, bool IsStopping, bool IsSelfRegistered);

// Internal inspection members used by the test harness (tests/Tedd.Tdfs.Testing). Everything runs on the node loop.
public sealed partial class NodeInspector
{
    /// <summary>This node's applied control state (members, volumes, pools, features).</summary>
    internal Task<HarnessControlSummary> GetControlSummaryAsync() => _loop.InvokeAsync(() =>
    {
        ControlState state = S.Control.State;
        var members = new List<HarnessMemberSummary>(state.Members.Count);
        foreach (MemberRecord m in state.Members.Values)
        {
            members.Add(new HarnessMemberSummary(m.Id, m.Name, m.Labels, [.. m.Endpoints], m.Status.ToString(), m.Incarnation, m.IsLive,
                m.IsPlacementEligible));
        }

        var volumes = new List<HarnessVolumeSummary>(state.Volumes.Count);
        foreach (VolumeRecord v in state.Volumes.Values)
        {
            volumes.Add(new HarnessVolumeSummary(v.Id, v.Name, v.Epoch, v.GroupName, v.State.ToString(), v.StoragePool,
                [.. v.Voters.Select(S.Control.NameOf)], [.. v.Learners.Select(S.Control.NameOf)], v.DefaultPolicy,
                new SortedDictionary<string, DfsStoragePolicy>(v.NamedPolicies.ToDictionary(p => p.Key, p => p.Value, StringComparer.Ordinal), StringComparer.Ordinal)));
        }

        var pools = new List<HarnessPoolSummary>(state.Pools.Count);
        foreach (PoolRecord p in state.Pools.Values)
            pools.Add(new HarnessPoolSummary(p.Name, [.. p.Nodes.Select(S.Control.NameOf).Order(StringComparer.Ordinal)]));
        return new HarnessControlSummary(state.ClusterId, state.AppliedIndex, state.PlacementEpoch, state.ControlVoterTarget, members, volumes, pools,
            [.. state.Features]);
    });

    /// <summary>Process-level status (serving, registered incarnation).</summary>
    internal Task<HarnessNodeStatus> GetNodeStatusAsync() => _loop.InvokeAsync(() =>
        new HarnessNodeStatus(S.NodeId, S.Incarnation, S.IsServing, S.IsStopping, S.Control.IsSelfRegistered));

    /// <summary>
    /// Reports every leadership this node assumes from now on: <paramref name="observer"/> is called on the node loop with
    /// the group's wire name (unique per group instance, for example <c>vol:{id}</c>), its display name ("control" or the
    /// volume name), the term and this node's name whenever a group's role or term changes and this node leads it (and once
    /// immediately for every group it already leads). Dispose the result to stop.
    /// </summary>
    internal Task<IDisposable> ObserveLeadershipAsync(Action<string, string, ulong, string> observer)
    {
        ArgumentNullException.ThrowIfNull(observer);
        return _loop.InvokeAsync<IDisposable>(() =>
        {
            var subscription = new LeadershipSubscription(_loop, S.Raft, S.NodeName, observer);
            foreach (GroupRuntime group in S.Raft.Groups)
                subscription.OnChanged(group);
            return subscription;
        });
    }

    /// <summary>
    /// Re-reads and verifies (checksum, identity) the whole object from the local store. False when the store does not hold
    /// it or the stored copy fails verification.
    /// </summary>
    internal Task<bool> VerifyObjectAsync(ObjectId id) => _loop.InvokeAsync(async () =>
    {
        ObjectStore store = S.ObjectStore;
        if (store.TryGetInfo(id) is not { } info)
            return false;
        try
        {
            var buffer = new byte[checked((int)info.Length)];
            int total = 0;
            while (total < buffer.Length)
            {
                int read = await store.ReadAsync(id, total, buffer.AsMemory(total));
                if (read <= 0)
                    return false;
                total += read;
            }

            return true;
        }
        catch (Exception ex) when (ex is DfsException or IOException or ObjectDisposedException)
        {
            return false;
        }
    });

    /// <summary>
    /// Re-reads and verifies every listed object the local store holds (one node-loop invocation); returns the ids whose
    /// stored copy verified. Objects not held, unreadable or failing verification are omitted.
    /// </summary>
    internal Task<IReadOnlySet<ObjectId>> VerifyObjectsAsync(IReadOnlyCollection<ObjectId> ids)
    {
        ArgumentNullException.ThrowIfNull(ids);
        ObjectId[] wanted = [.. ids];
        return _loop.InvokeAsync<IReadOnlySet<ObjectId>>(async () =>
        {
            ObjectStore store = S.ObjectStore;
            var verified = new HashSet<ObjectId>();
            byte[] buffer = [];
            foreach (ObjectId id in wanted)
            {
                if (store.TryGetInfo(id) is not { } info)
                    continue;
                try
                {
                    if (buffer.Length < info.Length)
                        buffer = new byte[checked((int)info.Length)];
                    int total = 0;
                    bool complete = true;
                    while (total < info.Length)
                    {
                        int read = await store.ReadAsync(id, total, buffer.AsMemory(total, checked((int)info.Length) - total));
                        if (read <= 0)
                        {
                            complete = false;
                            break;
                        }

                        total += read;
                    }

                    if (complete)
                        verified.Add(id);
                }
                catch (Exception ex) when (ex is DfsException or IOException or ObjectDisposedException)
                {
                    // A copy that cannot be read and verified is not a durable copy.
                }
            }

            return verified;
        });
    }

    /// <summary>Where the local store keeps an object's record (for corruption tests), or null when it does not hold it.</summary>
    internal Task<ObjectPhysicalLocation?> GetObjectLocationAsync(ObjectId id) => _loop.InvokeAsync(() => S.ObjectStore.GetPhysicalLocation(id));

    /// <summary>Deletes the local copy of an object without informing anyone (silent loss); false when it was not held.</summary>
    internal Task<bool> DeleteObjectLocallyAsync(ObjectId id) => _loop.InvokeAsync(async () =>
    {
        if (!await S.ObjectStore.ContainsAsync(id))
            return false;
        await S.ObjectStore.DeleteAsync(id);
        return true;
    });

    /// <summary>Runs one compaction pass of the local object store on the node loop.</summary>
    internal Task CompactObjectStoreAsync() => _loop.InvokeAsync(async () => await S.ObjectStore.CompactAsync());

    /// <summary>
    /// Asks this node to transfer leadership of a group it leads to <paramref name="target"/> through the production path
    /// (<c>TimeoutNow</c> once the target is caught up). Returns false when this node does not lead the group.
    /// </summary>
    internal Task<bool> TransferLeadershipAsync(string group, NodeId target)
    {
        ArgumentNullException.ThrowIfNull(group);
        return _loop.InvokeAsync(() =>
        {
            GroupRuntime? runtime = FindGroup(group);
            if (runtime is null || runtime.IsClosed || !runtime.IsLeader)
                return false;
            runtime.TransferLeadership(target);
            return true;
        });
    }

    /// <summary>Forwards leadership changes of one node to an observer (attached and detached on the node loop).</summary>
    private sealed class LeadershipSubscription : IDisposable
    {
        private readonly INodeLoop _loop;
        private readonly RaftGroupHost _raft;
        private readonly string _node;
        private readonly Action<string, string, ulong, string> _observer;
        private bool _disposed;

        public LeadershipSubscription(INodeLoop loop, RaftGroupHost raft, string node, Action<string, string, ulong, string> observer)
        {
            _loop = loop;
            _raft = raft;
            _node = node;
            _observer = observer;
            _raft.LeadershipChanged += OnChanged;
        }

        public void OnChanged(GroupRuntime group)
        {
            if (!_disposed && group.IsLeader && !group.IsClosed)
                _observer(group.Name, group.DisplayName, group.Term, _node);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            if (_loop.IsStopped)
                return;
            _loop.Post(() => _raft.LeadershipChanged -= OnChanged);
        }
    }
}
