using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Server.Admin;

internal sealed partial class AdminService
{
    /// <summary>Optional provider of per-pool replica health (the volume service aggregates inventories).</summary>
    public Func<string, ReplicaHealthData>? PoolHealthProvider { get; set; }

    private async Task<AdminResultMessage> CreatePrincipalAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        AdminCreatePrincipalBody body = message.ReadBody<AdminCreatePrincipalBody>();
        if (ControlStateMachine.GetPrincipalNameError(body.Principal) is { } error)
            throw new ArgumentException(error);
        if (string.IsNullOrEmpty(body.Secret))
            throw new ArgumentException("A principal secret is required.");
        RequireLeader();
        if (State.FindPrincipal(body.Principal) is not null && !(message.RequestId != UInt128.Zero && State.AdminOutcomes.ContainsKey(message.RequestId)))
            throw new DfsAlreadyExistsException($"Principal '{body.Principal}' already exists.");
        PrincipalVerifier verifier = HandshakeAuth.CreateVerifier(body.Secret, _s.Random.NextBytes(HandshakeAuth.SaltSize), HandshakeAuth.DefaultIterations);
        var data = new PrincipalData(body.Principal, verifier.Salt, verifier.Iterations, verifier.StoredKey, verifier.ServerKey, body.ClusterAdmin);
        return await ProposeEmptyAsync(message, new CreatePrincipalCommand(Header(message, principal), data), ct);
    }

    private async Task<AdminResultMessage> ExportRecoveryKeyAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        AdminExportRecoveryKeyBody body = message.ReadBody<AdminExportRecoveryKeyBody>();
        if (string.IsNullOrEmpty(body.Passphrase))
            throw new ArgumentException("A passphrase is required.");
        RequireLeader();
        byte[] escrow = DataKeys.ExportEscrow(_s.Keys.StorageKek, body.Passphrase, _s.Random);
        await ProposeAsync(message, new AppendAuditCommand(Header(message, principal), "ExportRecoveryKey", "ok", "cluster=" + State.ClusterName), ct);
        return Result(message.Op, new AdminRecoveryKeyBody(escrow));
    }

    private async Task<AdminResultMessage> GroupOperationAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        switch (message.Op)
        {
            case AdminOp.ListGroups:
                return Result(message.Op, new AdminGroupListBody(State.Groups.Values.Select(g => new GroupInfoData(g.Name, g.Members.ToArray())).ToArray()));
            case AdminOp.CreateGroup:
            case AdminOp.DeleteGroup:
            {
                string group = message.ReadBody<AdminGroupNameBody>().Group;
                ControlCommandKind kind = message.Op == AdminOp.CreateGroup ? ControlCommandKind.CreateGroup : ControlCommandKind.DeleteGroup;
                return await ProposeEmptyAsync(message, new GroupCommand(Header(message, principal), kind, group, null), ct);
            }

            default:
            {
                AdminGroupMemberBody body = message.ReadBody<AdminGroupMemberBody>();
                ControlCommandKind kind = message.Op == AdminOp.AddGroupMember ? ControlCommandKind.AddGroupMember : ControlCommandKind.RemoveGroupMember;
                return await ProposeEmptyAsync(message, new GroupCommand(Header(message, principal), kind, body.Group, body.Principal), ct);
            }
        }
    }

    private async Task<AdminResultMessage> PoolOperationAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        switch (message.Op)
        {
            case AdminOp.ListPools:
                return Result(message.Op, new AdminPoolListBody(PoolNames().Select(PoolInfo).ToArray()));
            case AdminOp.PoolStatus:
            {
                string pool = message.ReadBody<AdminPoolNameBody>().Pool;
                if (!State.PoolExists(pool))
                    throw new DfsException(DfsErrorCode.PoolNotFound, $"Pool '{pool}' does not exist.");
                ReplicaHealthData health = PoolHealthProvider?.Invoke(pool) ?? new ReplicaHealthData(0, 0, 0, 0, 0, 0, 0, 0, 0);
                string[] draining = State.PoolMembers(pool)
                    .Where(m => m.Status is WireNodeStatus.Draining or WireNodeStatus.Drained)
                    .Select(m => m.Name).Order(StringComparer.Ordinal).ToArray();
                return Result(message.Op, new AdminPoolStatusBody(PoolInfo(pool), health, draining));
            }

            case AdminOp.DeletePool:
                return await ProposeEmptyAsync(message, new PoolCommand(Header(message, principal), ControlCommandKind.DeletePool,
                    message.ReadBody<AdminPoolNameBody>().Pool, []), ct);
            default:
            {
                AdminPoolMembersBody body = message.ReadBody<AdminPoolMembersBody>();
                var nodes = new List<NodeId>();
                foreach (string name in body.Nodes)
                {
                    MemberRecord member = State.FindMemberByName(name) is { IsLive: true } m
                        ? m
                        : throw new ArgumentException($"Node '{name}' is not a live member.");
                    nodes.Add(member.Id);
                }

                ControlCommandKind kind = message.Op == AdminOp.CreatePool ? ControlCommandKind.CreatePool : ControlCommandKind.SetPoolMembers;
                return await ProposeEmptyAsync(message, new PoolCommand(Header(message, principal), kind, body.Pool, nodes), ct);
            }
        }
    }

    private IEnumerable<string> PoolNames() => new[] { DfsPoolNames.Default }.Concat(State.Pools.Keys).Order(StringComparer.Ordinal);

    private PoolInfoData PoolInfo(string pool)
    {
        IReadOnlyList<MemberRecord> members = State.PoolMembers(pool);
        string[] volumes = State.Volumes.Values.Where(v => string.Equals(v.StoragePool, pool, StringComparison.Ordinal))
            .Select(v => v.Name).Order(StringComparer.Ordinal).ToArray();
        long capacity = 0;
        long used = 0;
        foreach (MemberRecord m in members)
        {
            capacity = SaturatingAdd(capacity, m.CapacityBytes);
            used = SaturatingAdd(used, m.UsedBytes);
        }

        return new PoolInfoData(pool, members.Select(m => m.Name).Order(StringComparer.Ordinal).ToArray(), volumes, capacity, used);
    }

    private static long SaturatingAdd(long a, long b) => b > 0 && a > long.MaxValue - b ? long.MaxValue : a + b;
}
