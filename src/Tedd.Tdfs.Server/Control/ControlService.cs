using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// Discovery and membership RPCs served from the control state (PROTOCOL.md §9.2): <c>GetClusterMap</c>,
/// <c>GetVolumeRoute</c>, <c>JoinCluster</c> (seed join; control leader only) and <c>RegisterIncarnation</c>.
/// </summary>
internal sealed class ControlService
{
    private readonly NodeServices _s;

    public ControlService(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    private ControlHost Control => _s.Control;

    /// <summary>Registers the handlers.</summary>
    public void Register(RpcDispatcher rpc)
    {
        rpc.Register<GetClusterMapMessage>(GetClusterMapAsync, RpcAccess.Any | RpcAccess.JoiningNode);
        rpc.Register<GetVolumeRouteMessage>(GetVolumeRouteAsync, RpcAccess.Any | RpcAccess.JoiningNode);
        rpc.Register<JoinClusterMessage>(JoinClusterAsync, RpcAccess.Node | RpcAccess.JoiningNode);
        rpc.Register<RegisterIncarnationMessage>(RegisterIncarnationAsync, RpcAccess.Node);
    }

    private ValueTask<IWireMessage> GetClusterMapAsync(RpcContext context, IWireMessage request)
    {
        var message = (GetClusterMapMessage)request;
        ulong current = Control.State.AppliedIndex;
        ClusterMapData? map = message.KnownConfigIndex != 0 && message.KnownConfigIndex == current ? null : Control.BuildClusterMap();
        return new ValueTask<IWireMessage>(new ClusterMapResultMessage(current, map));
    }

    private async ValueTask<IWireMessage> GetVolumeRouteAsync(RpcContext context, IWireMessage request)
    {
        var message = (GetVolumeRouteMessage)request;
        VolumeRecord volume = Control.State.FindVolumeByName(message.Volume)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume '{message.Volume}' does not exist.");
        GroupRuntime? group = _s.Raft.TryGet(volume.GroupName);
        CommitToken last = CommitToken.None;
        if (message.RequireLeader)
        {
            if (group is null)
                throw new DfsUnavailableException(DfsErrorCode.NotLeader, $"{_s.NodeName} does not host volume '{volume.Name}'.");
            if (!group.IsLeader)
                throw group.NotLeader();
            ulong applied = await group.ReadIndexAsync(context.Cancellation);
            last = new CommitToken(Control.State.ClusterId, volume.Id, volume.Epoch, applied);
        }

        NodeId? leader = group?.Leader;
        return new VolumeRouteResultMessage(Control.ToVolumeEntry(volume), leader, leader is { } id ? Control.ResolveEndpoint(id) : null,
            group?.Term ?? 0, last);
    }

    private async ValueTask<IWireMessage> JoinClusterAsync(RpcContext context, IWireMessage request)
    {
        var message = (JoinClusterMessage)request;
        if (context.PeerNode != message.Node)
            throw new UnauthorizedAccessException("A node can only join as itself.");
        if (!Control.IsLeader)
            throw Control.NotLeader();
        ControlState state = Control.State;
        if (state.FindMember(message.Node) is { IsLive: false })
            throw new UnauthorizedAccessException($"Node {message.Name} was removed from the cluster; re-admission requires an explicit administrative join.");
        if (state.FindMemberByName(message.Name) is { IsLive: true, PendingAdmission: false } other && other.Id != message.Node)
            throw new DfsAlreadyExistsException($"Another node is a member under the name '{message.Name}'.");

        var command = new AddMemberCommand(Control.NewHeader(), message.Node, message.Name, message.Labels, message.Endpoints, message.Incarnation,
            message.BootId, message.CapacityBytes, message.ProtocolMax, message.Features.Order(StringComparer.Ordinal).ToArray());
        ApplyOutcome outcome = await Control.ProposeAsync(command, context.Cancellation);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        _s.Info("control", NodeServices.Inv($"Node {message.Name} ({message.Node}) joined the cluster at index {outcome.Index}."));
        return new JoinClusterResultMessage(true, _s.NodeId, _s.Endpoint, Control.BuildClusterMap());
    }

    private async ValueTask<IWireMessage> RegisterIncarnationAsync(RpcContext context, IWireMessage request)
    {
        var message = (RegisterIncarnationMessage)request;
        if (context.PeerNode != message.Node || context.Identity.Incarnation != message.Incarnation)
            throw new UnauthorizedAccessException("A node can only register its own incarnation.");
        if (!Control.IsLeader)
            throw Control.NotLeader();
        MemberRecord member = await RegisterAsync(message.Node, message.Incarnation, message.BootId, message.Labels, message.Endpoints, message.CapacityBytes,
            context.Cancellation);
        return new RegisterIncarnationResultMessage(true, member.Incarnation, member.Status, Control.State.AppliedIndex, Control.State.PlacementEpoch);
    }

    /// <summary>Proposes an incarnation registration (control leader) and returns the registered member.</summary>
    public async Task<MemberRecord> RegisterAsync(NodeId node, ulong incarnation, UInt128 bootId, DfsTopologyLabels labels, IReadOnlyList<string> endpoints,
        long capacityBytes, CancellationToken ct)
    {
        var command = new RegisterIncarnationCommand(Control.NewHeader(), node, incarnation, bootId, labels, endpoints, capacityBytes);
        ApplyOutcome outcome = await Control.ProposeAsync(command, ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        return (MemberRecord)outcome.Value!;
    }
}
