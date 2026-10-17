using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Admin;

/// <summary>Handler of a volume-scoped administrative operation (registered by the volume service).</summary>
internal delegate ValueTask<AdminResultMessage> AdminOperationHandler(RpcContext context, AdminRequestMessage request);

/// <summary>
/// <c>AdminRequest</c> dispatch (PROTOCOL.md §8.8). Control-plane operations are served here: cluster status and nodes on
/// the control leader, configuration changes proposed to the control group (deduplicated by request id), and local reads
/// (volume list, audit log). Volume-scoped operations served by a volume leader (status, replica health, policy
/// migration, scrub, repair, restore) are dispatched to handlers the volume service registers with
/// <see cref="RegisterVolumeOperation"/>; until then they answer <see cref="DfsErrorCode.Unsupported"/>.
/// </summary>
internal sealed partial class AdminService
{
    private readonly NodeServices _s;
    private readonly SortedDictionary<AdminOp, AdminOperationHandler> _volumeOperations = new();

    public AdminService(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    private ControlHost Control => _s.Control;

    private ControlState State => _s.Control.State;

    /// <summary>Operations dispatched to the volume service.</summary>
    public static IReadOnlyList<AdminOp> VolumeOperations { get; } =
        [AdminOp.VolumeStatus, AdminOp.PolicyMigrationStatus, AdminOp.StartScrub, AdminOp.StartRepair, AdminOp.ReplicaHealth, AdminOp.RestoreFromBackup];

    /// <summary>
    /// Registers the handler of a volume-scoped administrative operation (one of <see cref="VolumeOperations"/>). The
    /// handler authorizes the caller itself (VolumeAdmin or ClusterAdmin per PROTOCOL.md §8.8).
    /// </summary>
    public void RegisterVolumeOperation(AdminOp op, AdminOperationHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!VolumeOperations.Contains(op))
            throw new ArgumentException($"{op} is not a volume-scoped administrative operation.", nameof(op));
        if (!_volumeOperations.TryAdd(op, handler))
            throw new InvalidOperationException($"A handler for {op} is already registered.");
    }

    /// <summary>Registers the <c>AdminRequest</c> handler.</summary>
    public void Register(RpcDispatcher rpc) => rpc.Register<AdminRequestMessage>(HandleAsync, RpcAccess.Client);

    private async ValueTask<IWireMessage> HandleAsync(RpcContext context, IWireMessage request)
    {
        var message = (AdminRequestMessage)request;
        string principal = context.Principal ?? string.Empty;
        try
        {
            return await DispatchAsync(context, message, principal);
        }
        catch (UnauthorizedAccessException)
        {
            await Control.ReportSecurityEventAndWaitAsync("admin:" + message.Op, "denied", DenialScope(message), principal);
            throw;
        }
    }

    /// <summary>The audit scope of a denied request: the operation and, where the body names one, its target object.</summary>
    private static string DenialScope(AdminRequestMessage message)
    {
        string op = "op=" + message.Op;
        try
        {
            return message.Op switch
            {
                AdminOp.CreateGroup or AdminOp.DeleteGroup => op + " group=" + message.ReadBody<AdminGroupNameBody>().Group,
                AdminOp.AddGroupMember or AdminOp.RemoveGroupMember => op + " group=" + message.ReadBody<AdminGroupMemberBody>().Group
                    + " principal=" + message.ReadBody<AdminGroupMemberBody>().Principal,
                AdminOp.ListGroups => op + " group=*",
                AdminOp.PoolStatus or AdminOp.DeletePool => op + " pool=" + message.ReadBody<AdminPoolNameBody>().Pool,
                AdminOp.CreatePool or AdminOp.SetPoolMembers => op + " pool=" + message.ReadBody<AdminPoolMembersBody>().Pool,
                AdminOp.ListPools => op + " pool=*",
                AdminOp.SetDefaultPolicy => op + " volume=" + message.ReadBody<AdminSetDefaultPolicyBody>().Volume,
                AdminOp.DefinePolicy => op + " volume=" + message.ReadBody<AdminDefinePolicyBody>().Volume
                    + " policy=" + message.ReadBody<AdminDefinePolicyBody>().PolicyName,
                AdminOp.Grant or AdminOp.Revoke => op + " volume=" + message.ReadBody<AdminGrantBody>().Volume
                    + " principal=" + message.ReadBody<AdminGrantBody>().Principal,
                AdminOp.DeleteVolume or AdminOp.RotateDataKey => op + " volume=" + message.ReadBody<AdminVolumeNameBody>().Volume,
                _ => op,
            };
        }
        catch (Exception ex) when (ex is DfsProtocolException or ArgumentException or FormatException)
        {
            return op;
        }
    }

    private async ValueTask<IWireMessage> DispatchAsync(RpcContext context, AdminRequestMessage message, string principal)
    {
        if (_volumeOperations.TryGetValue(message.Op, out AdminOperationHandler? volumeHandler))
            return await volumeHandler(context, message);
        CancellationToken ct = context.Cancellation;
        switch (message.Op)
        {
            case AdminOp.ClusterStatus:
                RequireClusterAdmin(principal);
                return Result(message.Op, await ClusterStatusAsync(ct));
            case AdminOp.ListNodes:
                RequireClusterAdmin(principal);
                return Result(message.Op, await ListNodesAsync(ct));
            case AdminOp.ListVolumes:
                RequireClusterAdmin(principal);
                return Result(message.Op, new AdminNameListBody(State.Volumes.Values.Select(v => v.Name).Order(StringComparer.Ordinal).ToArray()));
            case AdminOp.AuditLog:
            {
                RequireClusterAdmin(principal);
                int max = message.ReadBody<AdminAuditLogRequestBody>().Max;

                // On the control leader (where clients send it) the read follows a ReadIndex barrier, so it contains every
                // record committed before it, including the denials answered just before (read-your-writes); another node
                // answers from its applied view.
                if (Control is { IsLeader: true, Group: { } group })
                    await group.ReadIndexAsync(ct);
                IEnumerable<AuditEntry> entries = State.Audit;
                if (max >= 0 && State.Audit.Count > max)
                    entries = State.Audit.Skip(State.Audit.Count - max);
                return Result(message.Op, new AdminNameListBody(entries.Select(e => e.ToString()).ToArray()));
            }

            case AdminOp.AddNode:
                return await AddNodeAsync(message, principal, ct);
            case AdminOp.DrainNode:
                return await SetNodeStatusAsync(message, principal, message.ReadBody<AdminNodeNameBody>().Name, WireNodeStatus.Draining, false, ct);
            case AdminOp.RemoveNode:
            {
                AdminRemoveNodeBody body = message.ReadBody<AdminRemoveNodeBody>();
                return await SetNodeStatusAsync(message, principal, body.Name, WireNodeStatus.Removed, body.Force, ct);
            }

            case AdminOp.SetNodeLabels:
                return await SetNodeLabelsAsync(message, principal, ct);
            case AdminOp.CreateVolume:
                return await CreateVolumeAsync(message, principal, ct);
            case AdminOp.DeleteVolume:
                return await DeleteVolumeAsync(message, principal, ct);
            case AdminOp.SetDefaultPolicy:
                return await SetDefaultPolicyAsync(message, principal, ct);
            case AdminOp.DefinePolicy:
                return await DefinePolicyAsync(message, principal, ct);
            case AdminOp.CreatePrincipal:
                return await CreatePrincipalAsync(message, principal, ct);
            case AdminOp.DeletePrincipal:
                RequireClusterAdmin(principal);
                return await ProposeEmptyAsync(message, new DeletePrincipalCommand(Header(message, principal), message.ReadBody<AdminPrincipalNameBody>().Principal), ct);
            case AdminOp.Grant:
            case AdminOp.Revoke:
                return await GrantAsync(message, principal, ct);
            case AdminOp.RotateDataKey:
                return await RotateDataKeyAsync(message, principal, ct);
            case AdminOp.ExportRecoveryKey:
                return await ExportRecoveryKeyAsync(message, principal, ct);
            case AdminOp.SetFeature:
            {
                RequireClusterAdmin(principal);
                AdminSetFeatureBody body = message.ReadBody<AdminSetFeatureBody>();
                return await ProposeEmptyAsync(message, new SetFeatureCommand(Header(message, principal), body.Feature, body.Enabled), ct);
            }

            case AdminOp.CreateGroup:
            case AdminOp.DeleteGroup:
            case AdminOp.AddGroupMember:
            case AdminOp.RemoveGroupMember:
            case AdminOp.ListGroups:
                return await GroupOperationAsync(message, principal, ct);
            case AdminOp.CreatePool:
            case AdminOp.SetPoolMembers:
            case AdminOp.DeletePool:
            case AdminOp.ListPools:
            case AdminOp.PoolStatus:
                return await PoolOperationAsync(message, principal, ct);
            default:
                if (VolumeOperations.Contains(message.Op))
                    throw new NotSupportedException($"{message.Op} is implemented by the volume service, which is not available on {_s.NodeName}.");
                throw new NotSupportedException($"Administrative operation {(ushort)message.Op} is not supported.");
        }
    }

    // ------------------------------------------------------------------ authorization and helpers

    private void RequireClusterAdmin(string principal)
    {
        if (!State.IsClusterAdmin(principal))
            throw new UnauthorizedAccessException($"Principal '{principal}' is not a cluster administrator.");
    }

    private void RequireVolumeAdmin(string principal, VolumeRecord volume)
    {
        if (State.IsClusterAdmin(principal))
            return;
        if ((State.GetPermissions(principal, volume.Id) & DfsPermissions.VolumeAdmin) == 0)
            throw new UnauthorizedAccessException($"Principal '{principal}' may not administer volume '{volume.Name}'.");
    }

    /// <summary>
    /// The named volume of the applied control state. Only the control leader answers "does not exist": another node's
    /// view may lag the control log, so it redirects (retryable <c>NotLeader</c>) instead of a stale definite answer.
    /// </summary>
    private VolumeRecord RequireVolume(string name)
    {
        if (State.FindVolumeByName(name) is { } volume)
            return volume;
        RequireLeader();
        throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume '{name}' does not exist.");
    }

    private MemberRecord RequireMember(string name)
        => State.FindMemberByName(name) is { IsLive: true } m ? m : throw new ArgumentException($"Node '{name}' is not a live member.");

    private void RequireLeader()
    {
        if (!Control.IsLeader)
            throw Control.NotLeader();
    }

    private static CommandHeader HeaderOf(TimeProvider time, AdminRequestMessage message, string principal) => new(message.RequestId, principal, time.GetUtcNow());

    private CommandHeader Header(AdminRequestMessage message, string principal) => HeaderOf(_s.Time, message, principal);

    private static AdminResultMessage Result<TBody>(AdminOp op, TBody body) where TBody : IWireRecord<TBody> => AdminResultMessage.Create(op, body);

    /// <summary>Proposes a mutating command on the control leader; a retried request id returns the retained outcome.</summary>
    private async Task<ApplyOutcome> ProposeAsync(AdminRequestMessage message, ControlCommand command, CancellationToken ct)
    {
        RequireLeader();
        if (message.RequestId != UInt128.Zero && State.AdminOutcomes.TryGetValue(message.RequestId, out AdminOutcome? prior))
        {
            object? value = prior.Kind switch
            {
                ControlCommandKind.CreateVolume => State.FindVolume(new VolumeId(prior.RefGuid)),
                ControlCommandKind.AdmitMember => State.FindMember(new NodeId(prior.RefGuid)),
                ControlCommandKind.AddDataKey => (uint)prior.RefNumber,
                _ => null,
            };
            return new ApplyOutcome(prior.Code, prior.Message, value) { Index = prior.Index };
        }

        ApplyOutcome outcome = await Control.ProposeAsync(command, ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        return outcome;
    }

    private async Task<AdminResultMessage> ProposeEmptyAsync(AdminRequestMessage message, ControlCommand command, CancellationToken ct)
    {
        ApplyOutcome outcome = await ProposeAsync(message, command, ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        return Result(message.Op, new AdminEmptyBody());
    }

    // ------------------------------------------------------------------ cluster and nodes

    private async Task<AdminClusterStatusBody> ClusterStatusAsync(CancellationToken ct)
    {
        RequireLeader();
        await Control.Group!.ReadIndexAsync(ct);
        RaftConfState conf = Control.Group.AppliedConf;
        NodeEntry[] nodes = NodesByName().Select(m => Control.ToNodeEntry(m, conf)).ToArray();
        return new AdminClusterStatusBody(State.ClusterId, State.ClusterName, ControlGroupStatus(), nodes,
            State.Volumes.Values.Select(v => v.Name).Order(StringComparer.Ordinal).ToArray(), State.PlacementEpoch,
            State.Features.ToHashSet(StringComparer.Ordinal));
    }

    private async Task<AdminNodeListBody> ListNodesAsync(CancellationToken ct)
    {
        RequireLeader();
        await Control.Group!.ReadIndexAsync(ct);
        RaftConfState conf = Control.Group.AppliedConf;
        return new AdminNodeListBody(NodesByName().Select(m => WithReportedUsage(Control.ToNodeEntry(m, conf))).ToArray());
    }

    private IEnumerable<MemberRecord> NodesByName()
        => State.Members.Values.OrderBy(m => m.Name, StringComparer.Ordinal).ThenBy(m => m.Id, NodeIdComparer.Instance);

    /// <summary>Status of the control group as seen by this node.</summary>
    public GroupStatusData ControlGroupStatus() => GroupStatus(Control.Group!, "control");

    /// <summary>Status of a hosted group (voters/learners by name, quorum as seen by the leader).</summary>
    public GroupStatusData GroupStatus(GroupRuntime group, string displayName)
    {
        RaftConfState conf = group.Conf;
        bool hasQuorum;
        if (group.IsLeader)
        {
            int reachable = conf.IsVoter(_s.NodeId) ? 1 : 0;
            foreach (RaftPeerProgress p in group.GetProgress())
            {
                if (!p.IsLearner && p.RecentActive)
                    reachable++;
            }

            hasQuorum = reachable >= conf.Quorum;
        }
        else
        {
            hasQuorum = group.Leader is not null;
        }

        return new GroupStatusData(displayName, conf.Voters.Select(Control.NameOf).ToArray(), conf.Learners.Select(Control.NameOf).ToArray(),
            group.Leader is { } leader ? Control.NameOf(leader) : null, group.Term, group.CommitIndex, group.AppliedIndex, hasQuorum);
    }

    private async Task<AdminResultMessage> AddNodeAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        AdminAddNodeBody body = message.ReadBody<AdminAddNodeBody>();
        if (string.IsNullOrWhiteSpace(body.Name))
            throw new ArgumentException("A node name is required.");
        RequireLeader();
        if (State.FindMemberByName(body.Name) is { IsLive: true })
            throw new DfsAlreadyExistsException($"A node named '{body.Name}' is already a member.");
        var command = new AdmitMemberCommand(Header(message, principal), NodeId.New(_s.Random), body.Name, body.Labels, body.Endpoints);
        ApplyOutcome outcome = await ProposeAsync(message, command, ct);
        var member = outcome.Value as MemberRecord ?? State.FindMemberByName(body.Name)
            ?? throw new DfsException(DfsErrorCode.Internal, "The admitted node is missing from the control state.");
        return Result(message.Op, Control.ToNodeEntry(member, Control.Group!.AppliedConf));
    }

    private async Task<AdminResultMessage> SetNodeStatusAsync(AdminRequestMessage message, string principal, string name, WireNodeStatus status, bool force,
        CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        RequireLeader();
        MemberRecord member = RequireMember(name);
        if (status == WireNodeStatus.Removed && !force && State.Members.Values.Count(m => m.IsLive) <= 1)
            throw new DfsException(DfsErrorCode.InvalidArgument, $"Node '{name}' is the last live member and cannot be removed.");
        if (status == WireNodeStatus.Removed && !force && member.IsLive && RemovalCheck is { } check)
        {
            await check(member, ct);
            RequireLeader();
            member = RequireMember(name);
        }

        return await ProposeEmptyAsync(message, new SetMemberStatusCommand(Header(message, principal), member.Id, status, force), ct);
    }

    private async Task<AdminResultMessage> SetNodeLabelsAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        AdminSetNodeLabelsBody body = message.ReadBody<AdminSetNodeLabelsBody>();
        RequireLeader();
        MemberRecord member = RequireMember(body.Name);
        return await ProposeEmptyAsync(message, new SetMemberLabelsCommand(Header(message, principal), member.Id, body.Labels), ct);
    }
}
