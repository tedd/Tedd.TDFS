using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// Discovery and membership RPCs served from the control state (PROTOCOL.md §9.2): <c>GetClusterMap</c>,
/// <c>GetVolumeRoute</c>, <c>JoinCluster</c> (seed join; control leader only), <c>RegisterIncarnation</c> and
/// <c>AuditEvent</c> (security events forwarded to the control leader).
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
        rpc.Register<AuditEventMessage>(AuditEventAsync, RpcAccess.Node);
    }

    /// <summary>Longest field of a forwarded audit record (longer values are cut; records never carry payload or secrets).</summary>
    private const int MaxAuditFieldLength = 512;

    /// <summary><c>AuditEvent</c> (member → control leader): records a security event another node observed (SEC-016).</summary>
    private async ValueTask<IWireMessage> AuditEventAsync(RpcContext context, IWireMessage request)
    {
        var message = (AuditEventMessage)request;
        if (!Control.IsLeader)
            throw Control.NotLeader();
        bool accepted = await Control.RecordForwardedSecurityEventAsync(new AuditEventMessage(Cut(message.Operation), Cut(message.Outcome), Cut(message.Scope),
            Cut(message.Principal)));
        return new AuditEventResultMessage(accepted);

        static string Cut(string value) => value.Length <= MaxAuditFieldLength ? value : value[..MaxAuditFieldLength];
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
            // The leader variant backs DfsVolume.GetInfoAsync: it discloses the volume's last commit, so a client principal
            // needs the Read grant on the volume (SEC-003); plain routing stays open to every authenticated peer, as the
            // route carries nothing beyond the cluster map.
            if (!context.IsNode)
            {
                string principal = context.Principal ?? string.Empty;
                if ((Control.State.GetPermissions(principal, volume.Id) & DfsPermissions.Read) == 0 && !Control.State.IsClusterAdmin(principal))
                {
                    Control.ReportSecurityEvent("volume:get info", "denied", "volume=" + volume.Name, principal);
                    throw new UnauthorizedAccessException($"Principal '{principal}' lacks the Read grant on volume '{volume.Name}' (get info).");
                }
            }

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
        RequireOwnIdentity(context, message.Node, message.Incarnation, message.BootId);
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
        RequireOwnIdentity(context, message.Node, message.Incarnation, message.BootId);
        if (!Control.IsLeader)
            throw Control.NotLeader();
        MemberRecord member = await RegisterAsync(message.Node, message.Incarnation, message.BootId, message.Labels, message.Endpoints, message.CapacityBytes,
            context.Cancellation, ReadRelease(message.Extensions));
        return new RegisterIncarnationResultMessage(true, member.Incarnation, member.Status, Control.State.AppliedIndex, Control.State.PlacementEpoch);
    }

    /// <summary>Optional extension of <c>RegisterIncarnation</c> carrying the registering node's release (varint32 protocol max, string list features).</summary>
    public const uint ReleaseExtension = 1;

    /// <summary>The release extension describing <paramref name="release"/>.</summary>
    public static WireExtension EncodeRelease(NodeRelease release)
    {
        ArgumentNullException.ThrowIfNull(release);
        var buffer = new System.Buffers.ArrayBufferWriter<byte>(64);
        var writer = new WireWriter(buffer);
        writer.WriteVarInt32(release.ProtocolMax);
        writer.WriteStringList(release.Features.Order(StringComparer.Ordinal).ToArray());
        return new WireExtension(ReleaseExtension, Required: false, buffer.WrittenSpan.ToArray());
    }

    /// <summary>The release carried by a registration's extensions, or null (older senders omit it).</summary>
    private static (int ProtocolMax, IReadOnlyList<string> Features)? ReadRelease(IReadOnlyList<WireExtension> extensions)
    {
        foreach (WireExtension extension in extensions)
        {
            if (extension.FieldId != ReleaseExtension)
                continue;
            try
            {
                var reader = new WireReader(extension.Value.Span, WireLimits.Default);
                int protocolMax = reader.ReadVarInt32();
                IReadOnlyList<string> features = reader.ReadStringList();
                return (protocolMax, features);
            }
            catch (DfsProtocolException)
            {
                return null; // malformed optional extension: treated as absent
            }
        }

        return null;
    }

    /// <summary>
    /// A join or incarnation registration must describe the authenticated connection itself: its node id, the incarnation
    /// it authenticated with and, on a connection the peer opened (whose Hello announced it), its boot id. Otherwise a
    /// process could register an incarnation it does not run, fencing the real one (RSY-021) or reviving a stale one.
    /// </summary>
    private static void RequireOwnIdentity(RpcContext context, NodeId node, ulong incarnation, UInt128 bootId)
    {
        if (context.PeerNode != node || context.Identity.Incarnation != incarnation
            || (!context.State.Outbound && context.State.BootId != bootId))
        {
            throw new UnauthorizedAccessException("A node can only register its own identity, incarnation and boot id.");
        }
    }

    /// <summary>
    /// Proposes an incarnation registration (control leader) and returns the registered member. When the registering process
    /// runs another software release than the member record lists (<paramref name="release"/>: protocol maximum and
    /// understood features), the release is recorded first, so feature eligibility (FMT-009) reflects the restarted node
    /// as soon as it counts as registered.
    /// </summary>
    public async Task<MemberRecord> RegisterAsync(NodeId node, ulong incarnation, UInt128 bootId, DfsTopologyLabels labels, IReadOnlyList<string> endpoints,
        long capacityBytes, CancellationToken ct, (int ProtocolMax, IReadOnlyList<string> Features)? release = null)
    {
        if (release is { } r && Control.State.FindMember(node) is { IsLive: true } known
            && (known.ProtocolMax != r.ProtocolMax || !known.Features.SequenceEqual(r.Features.Order(StringComparer.Ordinal), StringComparer.Ordinal)))
        {
            ApplyOutcome recorded = await Control.ProposeAsync(new SetMemberReleaseCommand(Control.NewHeader(), node, r.ProtocolMax, r.Features), ct);
            if (!recorded.IsSuccess)
                throw recorded.ToException();
        }

        var command = new RegisterIncarnationCommand(Control.NewHeader(), node, incarnation, bootId, labels, endpoints, capacityBytes);
        ApplyOutcome outcome = await Control.ProposeAsync(command, ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        return (MemberRecord)outcome.Value!;
    }
}
