using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>
/// Fault-context identity of a request at the request-path sites (F04 <c>rpc.request.accepted</c>, F13 <c>rpc.reply.send</c>):
/// the operation id it carries (the queried id for <c>QueryOperation</c>) and the normalized path it addresses.
/// </summary>
internal static class RequestFaultInfo
{
    /// <summary>The operation id of a request, or null.</summary>
    public static DfsOperationId? OperationOf(IWireMessage message) => message switch
    {
        QueryOperationMessage q when !q.Target.IsEmpty => q.Target,
        IVolumeRequest { Header.Operation: { IsEmpty: false } op } => op,
        _ => null,
    };

    /// <summary>The (normalized when valid) path a request addresses, or null.</summary>
    public static string? PathOf(IWireMessage message)
    {
        string? raw = message switch
        {
            OpenHandleMessage m => m.Path,
            GetInfoMessage m when m.Handle.IsEmpty => m.Path,
            ListDirectoryMessage m => m.Path,
            WriteAllMessage m => m.Path,
            AppendMessage m when m.Handle.IsEmpty => m.Path,
            CopyMessage m => m.Source,
            MoveMessage m => m.Source,
            DeleteMessage m => m.Path,
            CreateDirectoryMessage m => m.Path,
            SetPolicyMessage m => m.Path,
            SetAccessMessage m => m.Path,
            GetAccessControlMessage m => m.Path,
            SetAccessControlMessage m => m.Path,
            SetOwnerMessage m => m.Path,
            GetEffectiveRightsMessage m => m.Path,
            ListStreamsMessage m => m.Path,
            _ => null,
        };
        if (raw is null)
            return null;
        return DfsPath.TryNormalize(raw, out string normalized, out _) ? normalized : raw;
    }
}
