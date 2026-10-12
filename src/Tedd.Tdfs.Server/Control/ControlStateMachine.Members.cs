using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;

namespace Tedd.Tdfs.Server.Control;

internal sealed partial class ControlStateMachine
{
    private ApplyOutcome ApplyBootstrap(BootstrapClusterCommand c)
    {
        if (State.IsBootstrapped)
        {
            return State.ClusterId == c.ClusterId
                ? Fail(DfsErrorCode.AlreadyExists, "The cluster is already bootstrapped.")
                : Fail(DfsErrorCode.ClusterMismatch, "The control state belongs to another cluster.");
        }

        if (c.ClusterId == Guid.Empty || c.Members.Count == 0)
            return Fail(DfsErrorCode.InvalidArgument, "A bootstrap needs a cluster id and at least one member.");

        State.ClusterId = c.ClusterId;
        State.ClusterName = c.ClusterName;
        State.ControlVoterTarget = Math.Max(1, c.ControlVoterTarget);
        string[] features = c.MemberFeatures.Order(StringComparer.Ordinal).ToArray();
        foreach (BootstrapMemberData m in c.Members)
        {
            State.Members[m.Id] = new MemberRecord(m.Id, m.Name, m.Labels, m.Endpoints.ToArray(), WireNodeStatus.Active, 0, UInt128.Zero,
                long.MaxValue, 0, Protocol.ProtocolInfo.CurrentVersion, features, 0, PendingAdmission: false);
        }

        foreach (PrincipalData p in c.Principals)
        {
            State.Principals[p.Name] = new PrincipalRecord(p.Name, p.Salt, p.Iterations, p.StoredKey, p.ServerKey, p.ClusterAdmin,
                new SortedDictionary<VolumeId, DfsPermissions>(VolumeIdComparer.Instance), 0);
        }

        State.PlacementEpoch = 1;
        return ApplyOutcome.Success();
    }

    private ApplyOutcome ApplyAddMember(AddMemberCommand c, ulong index)
    {
        if (!State.IsBootstrapped)
            return Fail(DfsErrorCode.Unavailable, "The cluster is not bootstrapped.");
        if (c.Id.IsEmpty || string.IsNullOrWhiteSpace(c.Name))
            return Fail(DfsErrorCode.InvalidArgument, "A member needs an id and a name.");
        string[] features = c.Features.Order(StringComparer.Ordinal).ToArray();
        MemberRecord? existing = State.FindMember(c.Id);
        if (existing is not null)
        {
            if (!existing.IsLive)
                return Fail(DfsErrorCode.AccessDenied, $"Node {c.Name} was removed from the cluster; re-admission requires an explicit administrative join.");
            if (!string.Equals(existing.Name, c.Name, StringComparison.Ordinal))
                return Fail(DfsErrorCode.AlreadyExists, $"Node id {c.Id} is a member under the name '{existing.Name}'.");

            // A re-join is also an incarnation registration: an older incarnation, or the registered one claimed by another
            // process (cloned disk), must not rewrite the member's endpoints or identity (RSY-021).
            if (c.Incarnation < existing.Incarnation
                || (c.Incarnation == existing.Incarnation && existing.BootId != UInt128.Zero && existing.BootId != c.BootId))
            {
                return Fail(DfsErrorCode.StaleIncarnation,
                    $"Incarnation {c.Incarnation} of {existing.Name} is stale (registered {existing.Incarnation}).");
            }

            MemberRecord updated = existing with { Incarnation = c.Incarnation, BootId = c.BootId };
            updated = updated with { Endpoints = c.Endpoints.ToArray(), CapacityBytes = c.CapacityBytes, ProtocolMax = c.ProtocolMax, Features = features, PendingAdmission = false };
            if (!Equals(existing.Labels, c.Labels))
            {
                updated = updated with { Labels = c.Labels };
                State.PlacementEpoch++;
            }

            State.Members[c.Id] = updated;
            return ApplyOutcome.Success(updated);
        }

        MemberRecord? sameName = State.FindMemberByName(c.Name);
        if (sameName is { IsLive: true } && !sameName.PendingAdmission)
            return Fail(DfsErrorCode.AlreadyExists, $"Another node is a member under the name '{c.Name}'.");
        if (sameName is { IsLive: true, PendingAdmission: true })
            State.Members.Remove(sameName.Id);

        var record = new MemberRecord(c.Id, c.Name, c.Labels, c.Endpoints.ToArray(), WireNodeStatus.Active, c.Incarnation, c.BootId, c.CapacityBytes, 0,
            c.ProtocolMax, features, index, PendingAdmission: false);
        State.Members[c.Id] = record;
        State.PlacementEpoch++;
        return ApplyOutcome.Success(record);
    }

    private ApplyOutcome ApplyRegisterIncarnation(RegisterIncarnationCommand c)
    {
        MemberRecord? m = State.FindMember(c.Id);
        if (m is null)
            return Fail(DfsErrorCode.AccessDenied, $"Node {c.Id} is not a member.");
        if (!m.IsLive)
            return Fail(DfsErrorCode.AccessDenied, $"Node {m.Name} was removed from the cluster.");
        if (c.Incarnation < m.Incarnation)
            return Fail(DfsErrorCode.StaleIncarnation, $"Incarnation {c.Incarnation} of {m.Name} is older than the registered incarnation {m.Incarnation}.");
        if (c.Incarnation == m.Incarnation && m.BootId != UInt128.Zero && c.BootId != m.BootId)
            return Fail(DfsErrorCode.StaleIncarnation, $"Incarnation {c.Incarnation} of {m.Name} is already registered by another process (cloned disk).");
        MemberRecord updated = m with
        {
            Incarnation = c.Incarnation,
            BootId = c.BootId,
            Endpoints = c.Endpoints.Count > 0 ? c.Endpoints.ToArray() : m.Endpoints,
            CapacityBytes = c.CapacityBytes,
            PendingAdmission = false,
        };
        if (!Equals(m.Labels, c.Labels))
        {
            updated = updated with { Labels = c.Labels };
            State.PlacementEpoch++;
        }

        State.Members[c.Id] = updated;
        return ApplyOutcome.Success(updated);
    }

    private ApplyOutcome ApplyAdmitMember(AdmitMemberCommand c, ulong index)
    {
        if (c.Id.IsEmpty || string.IsNullOrWhiteSpace(c.Name))
            return Fail(DfsErrorCode.InvalidArgument, "A member needs an id and a name.");
        if (State.FindMemberByName(c.Name) is { IsLive: true })
            return Fail(DfsErrorCode.AlreadyExists, $"A node named '{c.Name}' is already a member.");
        if (State.Members.ContainsKey(c.Id))
            return Fail(DfsErrorCode.AlreadyExists, $"Node id {c.Id} is already in use.");
        var record = new MemberRecord(c.Id, c.Name, c.Labels, c.Endpoints.ToArray(), WireNodeStatus.Active, 0, UInt128.Zero, long.MaxValue, 0,
            Protocol.ProtocolInfo.CurrentVersion, [], index, PendingAdmission: true);
        State.Members[c.Id] = record;
        State.PlacementEpoch++;
        return ApplyOutcome.Success(record);
    }

    private ApplyOutcome ApplySetMemberStatus(SetMemberStatusCommand c)
    {
        MemberRecord? m = State.FindMember(c.Id);
        if (m is null)
            return Fail(DfsErrorCode.InvalidArgument, $"Node {c.Id} is not a member.");
        if (!m.IsLive)
        {
            return c.Status == WireNodeStatus.Removed
                ? ApplyOutcome.Success(m)
                : Fail(DfsErrorCode.InvalidArgument, $"Node {m.Name} was removed.");
        }

        if (c.Status == WireNodeStatus.Unreachable)
            return Fail(DfsErrorCode.InvalidArgument, "Unreachable is an observation, not a membership status.");
        if (m.Status == c.Status)
            return ApplyOutcome.Success(m);
        MemberRecord updated = m with { Status = c.Status };
        State.Members[c.Id] = updated;
        if (c.Status == WireNodeStatus.Removed)
        {
            foreach (PoolRecord pool in State.Pools.Values.ToArray())
            {
                if (pool.Nodes.Contains(c.Id))
                    State.Pools[pool.Name] = pool with { Nodes = pool.Nodes.Where(n => n != c.Id).ToArray() };
            }
        }

        State.PlacementEpoch++;
        return ApplyOutcome.Success(updated);
    }

    private ApplyOutcome ApplySetMemberLabels(SetMemberLabelsCommand c)
    {
        MemberRecord? m = State.FindMember(c.Id);
        if (m is null || !m.IsLive)
            return Fail(DfsErrorCode.InvalidArgument, $"Node {c.Id} is not a live member.");
        if (Equals(m.Labels, c.Labels))
            return ApplyOutcome.Success(m);
        MemberRecord updated = m with { Labels = c.Labels };
        State.Members[c.Id] = updated;
        State.PlacementEpoch++;
        return ApplyOutcome.Success(updated);
    }
}
