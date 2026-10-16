using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Raft;

/// <summary>
/// Spreads the leadership of volume groups over their voters (FSD §3.2: different volumes distribute their groups and
/// leadership across the cluster; ARCHITECTURE §2.4: leader balancing by <c>TimeoutNow</c> transfers). Elections pick
/// leaders at random, so volumes that share voters can all end up led by one node; a leader that leads at least two
/// volume groups more than another voter of one of its groups hands that group to the least loaded such voter.
/// </summary>
/// <remarks>
/// <para>
/// Counts come from the leaders this node knows for the volume groups it hosts (a node hosting a group learns its leader
/// from the leader's heartbeats). A transfer needs a settled group (this node led it for a full election window, applied
/// its commit index, no membership change or other transfer pending) and a target that is an active member, a voter, and
/// caught up and recently active per the leader's replication progress; the Raft transfer itself (<c>TimeoutNow</c>) then
/// only elects the target once it holds the whole log (ARCHITECTURE §4). One transfer per round and a cool-down of two
/// election windows keep the cluster from moving several groups at once.
/// </para>
/// <para>
/// The two-group threshold makes balancing converge: a transfer lowers the sender's count by one and raises a count that
/// was at least two lower, so the sum of squared counts strictly decreases and leadership never moves back and forth.
/// </para>
/// </remarks>
internal sealed class LeaderBalancer
{
    private readonly NodeServices _s;
    private readonly Dictionary<string, (ulong Term, long Since)> _ledSince = new(StringComparer.Ordinal);
    private long _coolDownUntil = long.MinValue;

    public LeaderBalancer(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    private TimeSpan ElectionWindow => _s.Timings.Tick * _s.Timings.ElectionTicksMax;

    /// <summary>One balancing round (a periodic node job).</summary>
    public Task RunOnceAsync()
    {
        if (!_s.IsServing || _s.IsStopping || _s.Raft is not { } raft || _s.Control is not { } control)
            return Task.CompletedTask;

        long now = _s.Time.GetTimestamp();
        GroupRuntime[] volumeGroups = raft.Groups.Where(g => g.VolumeName is not null && !g.IsClosed && g.Failure is null).ToArray();
        TrackLeadership(volumeGroups, now);
        if (now < _coolDownUntil || !control.IsSelfRegistered)
            return Task.CompletedTask;

        var counts = new SortedDictionary<NodeId, int>(NodeIdComparer.Instance);
        foreach (GroupRuntime group in volumeGroups)
        {
            if (group.Leader is { } leader && !leader.IsEmpty)
                counts[leader] = counts.GetValueOrDefault(leader) + 1;
        }

        int mine = counts.GetValueOrDefault(_s.NodeId);
        if (mine < 2)
            return Task.CompletedTask;

        foreach (GroupRuntime group in volumeGroups)
        {
            if (!IsSettled(group, now))
                continue;
            NodeId? target = null;
            int targetCount = int.MaxValue;
            foreach (RaftPeerProgress peer in group.GetProgress())
            {
                if (peer.IsLearner || peer.Peer == _s.NodeId || !peer.RecentActive || peer.Match < group.LastIndex)
                    continue;
                if (!group.Conf.Voters.Contains(peer.Peer) || control.State.FindMember(peer.Peer) is not { Status: WireNodeStatus.Active })
                    continue;
                int count = counts.GetValueOrDefault(peer.Peer);
                if (count < targetCount || (count == targetCount && target is { } t && NodeIdComparer.Instance.Compare(peer.Peer, t) < 0))
                {
                    target = peer.Peer;
                    targetCount = count;
                }
            }

            if (target is not { } chosen || mine - targetCount < 2)
                continue;
            _s.Info("raft", NodeServices.Inv(
                $"Balancing volume leadership: {_s.NodeName} leads {mine} volume groups; handing {group.DisplayName} to {chosen}, which leads {targetCount}."));
            group.TransferLeadership(chosen);
            _coolDownUntil = now + (long)(2 * ElectionWindow.TotalSeconds * _s.Time.TimestampFrequency);
            break;
        }

        return Task.CompletedTask;
    }

    private void TrackLeadership(GroupRuntime[] groups, long now)
    {
        foreach (string name in _ledSince.Keys.ToArray())
        {
            if (!groups.Any(g => g.IsLeader && string.Equals(g.Name, name, StringComparison.Ordinal)))
                _ledSince.Remove(name);
        }

        foreach (GroupRuntime group in groups)
        {
            if (group.IsLeader && (!_ledSince.TryGetValue(group.Name, out (ulong Term, long Since) led) || led.Term != group.Term))
                _ledSince[group.Name] = (group.Term, now);
        }
    }

    private bool IsSettled(GroupRuntime group, long now) =>
        group.IsLeader
        && group.Node.LeadTransferee is null
        && group.AppliedIndex >= group.CommitIndex
        && group.Conf.Voters.SequenceEqual(group.AppliedConf.Voters) && group.Conf.Learners.SequenceEqual(group.AppliedConf.Learners)
        && _ledSince.TryGetValue(group.Name, out (ulong Term, long Since) led) && led.Term == group.Term
        && _s.Time.GetElapsedTime(led.Since, now) >= ElectionWindow;
}
