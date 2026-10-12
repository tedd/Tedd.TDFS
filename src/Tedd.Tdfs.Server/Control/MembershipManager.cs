using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// Membership decisions of the control leader (ARCHITECTURE §2.4/2.5): every live member is a learner of the control group;
/// caught-up learners are promoted until <see cref="ControlState.ControlVoterTarget"/> voters exist (one per zone first,
/// then by name); removed members leave the group and draining voters are replaced, one change at a time. Volume voters
/// are chosen at creation from the volume's pool, and voters of a volume that are removed or draining are replaced in the
/// volume record (the volume leader then reconciles its group, <see cref="Volumes.VolumeHost"/>).
/// </summary>
internal sealed class MembershipManager
{
    private readonly NodeServices _s;

    public MembershipManager(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>Entries a learner may lag behind the commit index and still be promoted.</summary>
    public ulong CatchUpLag => (ulong)Math.Max(8, Math.Min(256, _s.Timings.RaftSnapshotThreshold / 4));

    /// <summary>One reconciliation round (no-op unless this node leads the control group).</summary>
    public async Task RunOnceAsync()
    {
        ControlHost control = _s.Control;
        if (!control.IsLeader || control.Group is not { } group || !control.State.IsBootstrapped)
            return;
        if (await ReconcileControlGroupAsync(group))
            return;
        await ReconcileVolumeRecordsAsync();
    }

    /// <summary>
    /// Chooses the initial voters of a new volume (Metadata policy: OnePerZone, Explicit or Any), restricted to the
    /// eligible members of the volume's pool except for an explicit list. Spreads volumes with rendezvous hashing on the id.
    /// </summary>
    /// <exception cref="DfsException">InvalidPolicy/PlacementUnavailable when the policy cannot be met.</exception>
    public static IReadOnlyList<NodeId> ChooseVolumeVoters(ControlState state, VolumeId volume, DfsMetadataPolicy metadata, string pool)
    {
        if (metadata.GetValidationError() is { } error)
            throw new DfsException(DfsErrorCode.InvalidPolicy, error);
        if (metadata.Placement == DfsVoterPlacement.Explicit)
        {
            var explicitVoters = new List<NodeId>();
            foreach (string name in metadata.ExplicitVoterNodes!)
            {
                MemberRecord member = state.FindMemberByName(name) is { IsLive: true } m
                    ? m
                    : throw new DfsException(DfsErrorCode.InvalidPolicy, $"Explicit voter '{name}' is not a live member.");
                explicitVoters.Add(member.Id);
            }

            return explicitVoters;
        }

        IReadOnlyList<MemberRecord> candidates = state.PoolMembers(pool).Where(m => m.IsPlacementEligible).ToArray();
        if (candidates.Count < metadata.Voters)
        {
            throw new DfsException(DfsErrorCode.InvalidPolicy,
                $"Volume metadata needs {metadata.Voters} voters but pool '{pool}' has only {candidates.Count} eligible nodes.");
        }

        IReadOnlyList<MemberRecord> ordered = HrwOrder(candidates, volume.Value);
        var chosen = new List<MemberRecord>();
        if (metadata.Placement == DfsVoterPlacement.OnePerZone)
        {
            var zones = new HashSet<(string, string)>();
            foreach (MemberRecord m in ordered)
            {
                if (chosen.Count < metadata.Voters && zones.Add(m.Labels.ZoneKey))
                    chosen.Add(m);
            }

            var hosts = new HashSet<string>(chosen.Select(m => m.Labels.Host), StringComparer.Ordinal);
            foreach (MemberRecord m in ordered)
            {
                if (chosen.Count < metadata.Voters && !chosen.Contains(m) && hosts.Add(m.Labels.Host))
                    chosen.Add(m);
            }
        }

        foreach (MemberRecord m in ordered)
        {
            if (chosen.Count < metadata.Voters && !chosen.Contains(m))
                chosen.Add(m);
        }

        return chosen.Select(m => m.Id).ToArray();
    }

    /// <summary>Orders members by rendezvous hash on <paramref name="key"/> (deterministic spread across volumes).</summary>
    public static IReadOnlyList<MemberRecord> HrwOrder(IReadOnlyList<MemberRecord> members, Guid key)
    {
        var nodes = members.Select(m => new PlacementNode(m.Id, m.Name, m.Labels, true, 1)).ToArray();
        byte[] keyBytes = key.ToByteArray();
        var objectKey = new ObjectId(System.Buffers.Binary.BinaryPrimitives.ReadUInt128LittleEndian(keyBytes));
        var byId = members.ToDictionary(m => m.Id);
        return Placement.FallbackOrder(objectKey, 0, nodes).Select(n => byId[n.Id]).ToArray();
    }

    private async Task<bool> ReconcileControlGroupAsync(GroupRuntime group)
    {
        if (group.Node.HasPendingConfChange)
            return true;
        ControlState state = _s.Control.State;
        RaftConfState conf = group.Conf;

        // 1. Removed members leave the control group.
        foreach (NodeId id in conf.Voters.Concat(conf.Learners))
        {
            if (state.FindMember(id) is { IsLive: false } removed && !(conf.IsVoter(id) && conf.Voters.Count == 1))
            {
                _s.Info("membership", NodeServices.Inv($"Removing {removed.Name} from the control group (removed member)."));
                await group.ProposeConfChangeAsync(new RaftConfChange(RaftConfChangeKind.RemoveNode, id));
                return true;
            }
        }

        // 2. Every live member receives the log as a learner.
        foreach (MemberRecord m in state.Members.Values)
        {
            if (m.IsLive && !m.PendingAdmission && !conf.Contains(m.Id))
            {
                _s.Info("membership", NodeServices.Inv($"Adding {m.Name} to the control group as a learner."));
                await group.ProposeConfChangeAsync(new RaftConfChange(RaftConfChangeKind.AddLearner, m.Id));
                return true;
            }
        }

        // 3. Promote caught-up learners until the voter target is reached (one per zone first, then by name).
        int activeVoters = conf.Voters.Count(v => state.FindMember(v) is { Status: WireNodeStatus.Active });
        int eligible = state.Members.Values.Count(m => m.IsLive && m.Status == WireNodeStatus.Active && !m.PendingAdmission);
        int target = Math.Min(state.ControlVoterTarget, eligible);
        if (activeVoters < target && PickPromotion(group, conf, state) is { } promote)
        {
            _s.Info("membership", NodeServices.Inv($"Promoting {_s.Control.NameOf(promote)} to control voter."));
            await group.ProposeConfChangeAsync(new RaftConfChange(RaftConfChangeKind.PromoteLearner, promote));
            return true;
        }

        // 4. Replace draining voters once enough active voters exist.
        if (activeVoters >= target)
        {
            foreach (NodeId voter in conf.Voters)
            {
                if (state.FindMember(voter) is { Status: WireNodeStatus.Draining or WireNodeStatus.Drained } draining && conf.Voters.Count > 1)
                {
                    _s.Info("membership", NodeServices.Inv($"Removing draining {draining.Name} from the control voters."));
                    await group.ProposeConfChangeAsync(new RaftConfChange(RaftConfChangeKind.RemoveNode, voter));
                    return true;
                }
            }
        }

        return false;
    }

    private NodeId? PickPromotion(GroupRuntime group, RaftConfState conf, ControlState state)
    {
        ulong commit = group.CommitIndex;
        var caughtUp = new List<MemberRecord>();
        foreach (RaftPeerProgress progress in group.GetProgress())
        {
            if (!progress.IsLearner || !progress.RecentActive || progress.Match + CatchUpLag < commit)
                continue;
            if (state.FindMember(progress.Peer) is { IsLive: true, Status: WireNodeStatus.Active, PendingAdmission: false } m)
                caughtUp.Add(m);
        }

        if (caughtUp.Count == 0)
            return null;
        var voterZones = new HashSet<(string, string)>(conf.Voters.Select(v => state.FindMember(v)).OfType<MemberRecord>().Select(m => m.Labels.ZoneKey));
        MemberRecord? newZone = caughtUp.Where(m => !voterZones.Contains(m.Labels.ZoneKey)).OrderBy(m => m.Name, StringComparer.Ordinal).FirstOrDefault();
        return (newZone ?? caughtUp.OrderBy(m => m.Name, StringComparer.Ordinal).First()).Id;
    }

    private async Task ReconcileVolumeRecordsAsync()
    {
        ControlState state = _s.Control.State;
        foreach (VolumeRecord volume in state.Volumes.Values.ToArray())
        {
            bool NeedsReplacement(NodeId id) => state.FindMember(id) is not { IsLive: true, Status: WireNodeStatus.Active };
            NodeId[] bad = volume.Voters.Where(NeedsReplacement).ToArray();
            NodeId[] badLearners = volume.Learners.Where(id => state.FindMember(id) is not { IsLive: true }).ToArray();
            if (bad.Length == 0 && badLearners.Length == 0)
                continue;

            var voters = volume.Voters.Where(v => !bad.Contains(v)).ToList();
            IReadOnlyList<MemberRecord> candidates = HrwOrder(state.PoolMembers(volume.StoragePool)
                .Where(m => m.IsPlacementEligible && !volume.Voters.Contains(m.Id)).ToArray(), volume.Id.Value);
            var zones = new HashSet<(string, string)>(voters.Select(v => state.FindMember(v)).OfType<MemberRecord>().Select(m => m.Labels.ZoneKey));
            foreach (NodeId _ in bad)
            {
                MemberRecord? replacement = candidates.FirstOrDefault(c => !voters.Contains(c.Id) && !zones.Contains(c.Labels.ZoneKey))
                    ?? candidates.FirstOrDefault(c => !voters.Contains(c.Id));
                if (replacement is null)
                    break;
                voters.Add(replacement.Id);
                zones.Add(replacement.Labels.ZoneKey);
            }

            // A draining voter without replacement keeps its seat; a removed one is dropped (it can never vote again).
            foreach (NodeId id in bad)
            {
                if (voters.Count < volume.Voters.Count && state.FindMember(id) is { IsLive: true })
                    voters.Add(id);
            }

            if (voters.Count == 0)
                continue;
            NodeId[] learners = volume.Learners.Where(l => !badLearners.Contains(l) && !voters.Contains(l)).ToArray();
            if (voters.SequenceEqual(volume.Voters) && learners.SequenceEqual(volume.Learners))
                continue;
            _s.Info("membership", NodeServices.Inv(
                $"Volume {volume.Name}: replacing voters [{string.Join(", ", bad.Select(_s.Control.NameOf))}] → [{string.Join(", ", voters.Select(_s.Control.NameOf))}]."));
            ApplyOutcome outcome = await _s.Control.ProposeAsync(new SetVolumeVotersCommand(_s.Control.NewHeader(), volume.Id, voters, learners));
            if (!outcome.IsSuccess)
                _s.Warn("membership", NodeServices.Inv($"Volume {volume.Name}: updating voters failed: {outcome.Code} {outcome.Message}"));
            return;
        }
    }
}
