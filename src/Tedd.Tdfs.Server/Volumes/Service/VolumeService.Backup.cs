using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Volumes.Service;

internal sealed partial class VolumeService
{
    /// <summary>
    /// <c>BackupRegister</c>: listing the registered late-sync targets of the volume (from its control record). Registering,
    /// configuring and removing targets belong to the backup exporter, which is not part of this build (Unsupported).
    /// </summary>
    public async Task<IWireMessage> BackupRegisterAsync(RpcContext context, BackupRegisterMessage request)
    {
        RejectTransaction(request);
        string principal = context.Principal ?? string.Empty;
        if (request.Action != BackupTargetAction.List)
        {
            return new ErrorReplyMessage(DfsErrorCode.Unsupported,
                $"Backup target {request.Action} is not supported by this build (the backup exporter is not implemented).",
                request.Header.Operation.IsEmpty ? null : request.Header.Operation);
        }

        if (GrantsOf(principal) == DfsPermissions.None)
            throw new UnauthorizedAccessException($"Principal '{principal}' has no grant on volume '{Name}'.");
        ulong applied = await ReadBarrierAsync(context.Cancellation);
        string[] targets = [.. Record.BackupTargets.Keys.Order(StringComparer.Ordinal)];
        return new BackupRegisterResultMessage(targets, Token(applied));
    }
}
