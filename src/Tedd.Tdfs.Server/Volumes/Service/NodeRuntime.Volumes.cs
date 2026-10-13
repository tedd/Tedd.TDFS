using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Admin;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Service;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>Volume-service plug-in of the node runtime (INTERNALS.md §1.3): request handlers, admin operations and leader jobs.</summary>
internal sealed partial class NodeRuntime
{
    partial void RegisterVolumeHandlers(NodeServices services)
    {
        var router = new VolumeRequestRouter(services);
        router.Register<OpenSessionMessage>((svc, ctx, m) => svc.OpenSessionAsync(ctx, m));
        router.Register<KeepAliveMessage>((svc, ctx, m) => svc.KeepAliveAsync(ctx, m));
        router.Register<QueryOperationMessage>((svc, ctx, m) => svc.QueryOperationAsync(ctx, m));
        router.Register<ReserveOperationIdsMessage>((svc, ctx, m) => svc.ReserveOperationIdsAsync(ctx, m));

        router.Register<OpenHandleMessage>((svc, ctx, m) => svc.OpenHandleAsync(ctx, m));
        router.Register<CloseHandleMessage>((svc, ctx, m) => svc.CloseHandleAsync(ctx, m));
        router.Register<ReadRangeMessage>((svc, ctx, m) => svc.ReadRangeAsync(ctx, m));
        router.Register<GetInfoMessage>((svc, ctx, m) => svc.GetInfoAsync(ctx, m));
        router.Register<ListDirectoryMessage>((svc, ctx, m) => svc.ListDirectoryAsync(ctx, m));
        router.Register<CreateStagingMessage>((svc, ctx, m) => svc.CreateStagingAsync(ctx, m));
        router.Register<CommitFileMessage>((svc, ctx, m) => svc.CommitFileAsync(ctx, m));
        router.Register<WriteAllMessage>((svc, ctx, m) => svc.WriteAllAsync(ctx, m));
        router.Register<AppendMessage>((svc, ctx, m) => svc.AppendAsync(ctx, m));
        router.Register<CopyMessage>((svc, ctx, m) => svc.CopyAsync(ctx, m));
        router.Register<MoveMessage>((svc, ctx, m) => svc.MoveAsync(ctx, m));
        router.Register<DeleteMessage>((svc, ctx, m) => svc.DeleteAsync(ctx, m));
        router.Register<CreateDirectoryMessage>((svc, ctx, m) => svc.CreateDirectoryAsync(ctx, m));
        router.Register<SetPolicyMessage>((svc, ctx, m) => svc.SetPolicyAsync(ctx, m));
        router.Register<SetAccessMessage>((svc, ctx, m) => svc.SetAccessAsync(ctx, m));

        router.Register<GetAccessControlMessage>((svc, ctx, m) => svc.GetAccessControlAsync(ctx, m));
        router.Register<SetAccessControlMessage>((svc, ctx, m) => svc.SetAccessControlAsync(ctx, m));
        router.Register<SetOwnerMessage>((svc, ctx, m) => svc.SetOwnerAsync(ctx, m));
        router.Register<GetEffectiveRightsMessage>((svc, ctx, m) => svc.GetEffectiveRightsAsync(ctx, m));
        router.Register<ListStreamsMessage>((svc, ctx, m) => svc.ListStreamsAsync(ctx, m));

        router.Register<TxBeginMessage>((svc, ctx, m) => svc.TxBeginAsync(ctx, m));
        router.Register<TxOpMessage>((svc, ctx, m) => svc.TxOpAsync(ctx, m));
        router.Register<TxCommitMessage>((svc, ctx, m) => svc.TxCommitAsync(ctx, m));
        router.Register<TxAbortMessage>((svc, ctx, m) => svc.TxAbortAsync(ctx, m));

        router.Register<CreateSnapshotMessage>((svc, ctx, m) => svc.CreateSnapshotAsync(ctx, m));
        router.Register<ListSnapshotsMessage>((svc, ctx, m) => svc.ListSnapshotsAsync(ctx, m));
        router.Register<DeleteSnapshotMessage>((svc, ctx, m) => svc.DeleteSnapshotAsync(ctx, m));
        router.Register<OpenSnapshotMessage>((svc, ctx, m) => svc.OpenSnapshotAsync(ctx, m));
        router.Register<CloseSnapshotMessage>((svc, ctx, m) => svc.CloseSnapshotAsync(ctx, m));
        router.Register<SetRetentionMessage>((svc, ctx, m) => svc.SetRetentionAsync(ctx, m));
        router.Register<RestoreSnapshotMessage>((svc, ctx, m) => svc.RestoreSnapshotAsync(ctx, m));
        router.Register<BackupRegisterMessage>((svc, ctx, m) => svc.BackupRegisterAsync(ctx, m));

        services.Rpc.Register<InventoryReportMessage>(router.InventoryAsync, RpcAccess.Node);
        services.Rpc.Register<RestoreInstallMessage>(router.RestoreInstallAsync, RpcAccess.Node);
        var restoreControl = new RestoreControl(services);
        services.Rpc.Register<RestoreVolumeMessage>(restoreControl.HandleAsync, RpcAccess.Node);

        services.Admin.RegisterVolumeOperation(AdminOp.VolumeStatus, router.Admin((svc, ctx, m) => svc.VolumeStatusAsync(ctx, m)));
        services.Admin.RegisterVolumeOperation(AdminOp.ReplicaHealth, router.Admin((svc, ctx, m) => svc.ReplicaHealthAsync(ctx, m)));
        services.Admin.RegisterVolumeOperation(AdminOp.PolicyMigrationStatus, router.Admin((svc, ctx, m) => svc.PolicyMigrationStatusAsync(ctx, m)));
    }

    partial void StartVolumeJobs(NodeServices services)
    {
        NodeTimings t = services.Timings;
        TimeSpan interval = TimeSpan.FromTicks(Math.Max(t.Tick.Ticks * 10, Math.Min(t.LeaseDuration.Ticks / 4, t.PinDuration.Ticks / 4)));
        services.Jobs.StartPeriodic("volume-maintenance", interval, async () =>
        {
            foreach (VolumeHost host in services.Volumes.Hosts)
            {
                if (host.Service is { IsLeader: true } service && !host.Group.IsClosed)
                    await service.MaintainAsync();
            }
        });
    }
}

/// <summary>
/// Routes volume requests to the volume service of the addressed volume (INTERNALS.md §1.3): resolves the volume by name,
/// checks the header's expected volume id and epoch (EpochMismatch, PATH-014) and redirects to the leader (NotLeader with
/// a hint).
/// </summary>
internal sealed class VolumeRequestRouter(NodeServices services)
{
    /// <summary>Handler of one request type on the volume service.</summary>
    public delegate Task<IWireMessage> ServiceHandler<in T>(VolumeService service, RpcContext context, T request);

    /// <summary>Handler of a volume admin operation on the volume service.</summary>
    public delegate Task<AdminResultMessage> AdminHandler(VolumeService service, RpcContext context, AdminRequestMessage request);

    /// <summary>Registers a client request handler.</summary>
    public void Register<T>(ServiceHandler<T> handler)
        where T : class, IWireMessage<T>, IVolumeRequest
        => services.Rpc.Register<T>(async (context, request) =>
        {
            var typed = (T)request;
            try
            {
                return await handler(Resolve(typed.Header), context, typed);
            }
            catch (ApplyFailure failure)
            {
                // Leader pre-checks reuse the apply validators: their failures carry the same code and detail as apply would.
                var reply = (ErrorReplyMessage)VolumeApplyResult.Fail(failure.Code, failure.Message, failure.Detail).Reply!;
                return typed.Header.Operation.IsEmpty ? reply : reply with { OperationId = typed.Header.Operation };
            }
        }, RpcAccess.Client);

    /// <summary>An admin-operation handler for a volume-scoped operation (body: <see cref="AdminVolumeNameBody"/>).</summary>
    public AdminOperationHandler Admin(AdminHandler handler)
        => async (context, request) =>
        {
            AdminVolumeNameBody body = request.ReadBody<AdminVolumeNameBody>();
            VolumeRecord record = services.Control.State.FindVolumeByName(body.Volume)
                ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume '{body.Volume}' does not exist.");
            RequireServed(record);
            return await handler(ServiceOf(record), context, request);
        };

    /// <summary><c>RestoreInstall</c> (restore coordinator → the restoring volume's leader).</summary>
    public async ValueTask<IWireMessage> RestoreInstallAsync(RpcContext context, IWireMessage request)
    {
        var message = (RestoreInstallMessage)request;
        VolumeRecord record = services.Control.State.FindVolume(message.Volume)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {message.Volume} does not exist.");
        return await ServiceOf(record).RestoreInstallAsync(context, message);
    }

    /// <summary><c>InventoryReport</c> (node → volume leader).</summary>
    public async ValueTask<IWireMessage> InventoryAsync(RpcContext context, IWireMessage request)
    {
        var message = (InventoryReportMessage)request;
        VolumeRecord record = services.Control.State.FindVolume(message.Volume)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {message.Volume} does not exist.");
        return await ServiceOf(record).InventoryReportAsync(context, message);
    }

    private VolumeService Resolve(RequestHeader header)
    {
        VolumeRecord record = services.Control.State.FindVolumeByName(header.Volume)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume '{header.Volume}' does not exist.");
        if (!header.ExpectedVolumeId.Value.Equals(Guid.Empty) && header.ExpectedVolumeId != record.Id)
            throw new DfsEpochMismatchException($"Volume '{header.Volume}' is {record.Id}, not the expected {header.ExpectedVolumeId}.");
        if (header.ExpectedVolumeEpoch != 0 && header.ExpectedVolumeEpoch != record.Epoch)
            throw new DfsEpochMismatchException($"Volume '{header.Volume}' is at epoch {record.Epoch}, not the expected {header.ExpectedVolumeEpoch}.");
        RequireServed(record);
        return ServiceOf(record);
    }

    /// <summary>A volume being restored is not served to clients before the restore completes and activates it (RST-007).</summary>
    private static void RequireServed(VolumeRecord record)
    {
        if (record.State == WireVolumeState.Restoring)
            throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"Volume '{record.Name}' is being restored and is not served yet.");
    }

    private VolumeService ServiceOf(VolumeRecord record)
    {
        VolumeHost? host = services.Volumes.TryGet(record.Id);
        if (host is null)
        {
            string? hint = services.Raft.LeaderOf(record.GroupName) is { } leader ? services.Control.ResolveEndpoint(leader) : null;
            throw new DfsUnavailableException(DfsErrorCode.NotLeader, $"{services.NodeName} does not host volume '{record.Name}'.") { LeaderHint = hint };
        }

        VolumeService service = host.Service
            ?? throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"Volume '{record.Name}' is not served by a volume service on {services.NodeName}.");
        if (!host.IsLeader)
            throw host.Group.NotLeader();
        return service;
    }
}
