using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Volumes.Service;

internal sealed partial class VolumeService
{
    /// <summary>Marker of the transaction phase: every transaction request is answered Unsupported until it is implemented.</summary>
    internal const string TransactionsNotImplemented =
        "NOT IMPLEMENTED (next phase): transactions (TxBegin/TxOp/TxCommit/TxAbort, working roots, validation) are not supported by this volume service yet.";

    /// <summary><c>TxBegin</c>/<c>TxOp</c>/<c>TxCommit</c>/<c>TxAbort</c>: next phase (commands and state are defined).</summary>
    public ValueTask<IWireMessage> TransactionAsync(RpcContext context, IVolumeRequest request)
    {
        if (!IsLeader)
            throw Group.NotLeader();
        _ = context;
        return new ValueTask<IWireMessage>(new ErrorReplyMessage(DfsErrorCode.Unsupported, TransactionsNotImplemented,
            request.Header.Operation.IsEmpty ? null : request.Header.Operation, false, null, request.Header.Transaction));
    }
}
