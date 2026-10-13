using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Backup;
using Tedd.Tdfs.Server.Volumes.Service;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Backup plug-in of the node runtime (INTERNALS.md §1.3): the volume leader's backup requests (status, sync, barriers), the
/// control leader's registration step, restore from a backup host, and the periodic exporter reconciliation.
/// </summary>
internal sealed partial class NodeRuntime
{
    partial void RegisterBackupHandlers(NodeServices services)
    {
        var exporters = new BackupExporters(services);
        if (_transport is { } transport)
            exporters.Attach(transport);
        services.BackupExports = exporters;

        var router = new VolumeRequestRouter(services);
        router.Register<BackupStatusMessage>((svc, ctx, m) => svc.BackupStatusAsync(ctx, m));
        router.Register<BackupRequestSyncMessage>((svc, ctx, m) => svc.BackupRequestSyncAsync(ctx, m));
        router.Register<BackupAwaitMessage>((svc, ctx, m) => svc.BackupAwaitAsync(ctx, m));

        var control = new BackupTargetControl(services);
        services.Rpc.Register<BackupTargetUpdateMessage>(control.HandleAsync, RpcAccess.Node);

        var restore = new BackupRestoreCoordinator(services);
        services.Admin.RegisterVolumeOperation(AdminOp.RestoreFromBackup, restore.HandleAsync);
    }

    partial void StartBackupJobs(NodeServices services)
    {
        BackupExporters exporters = services.BackupExports ?? throw new InvalidOperationException("The backup exporters were not registered.");
        services.Jobs.StartPeriodic("backup-exporters", services.Timings.Tick * 20, exporters.ReconcileAsync);
    }
}
