using System.Diagnostics.CodeAnalysis;

namespace Tedd.Tdfs;

/// <summary>
/// The operation may or may not have committed (<see cref="DfsErrorCode.OutcomeUnknown"/>). Always carries the
/// operation id (and the transaction id for transactions) so the outcome can be resolved later with
/// <c>QueryOperationAsync</c>; never presented as a definite abort.
/// </summary>
public class DfsCommitOutcomeUnknownException : DfsException
{
    /// <summary>Creates the exception for <paramref name="operationId"/>.</summary>
    [SetsRequiredMembers]
    public DfsCommitOutcomeUnknownException(DfsOperationId operationId, string message, Exception? inner = null)
        : base(DfsErrorCode.OutcomeUnknown, message, inner)
    {
        OperationId = operationId;
    }

    /// <summary>Creates the exception for a transaction commit with unknown outcome.</summary>
    [SetsRequiredMembers]
    public DfsCommitOutcomeUnknownException(DfsOperationId operationId, DfsTransactionId? transactionId, string message, Exception? inner = null)
        : base(DfsErrorCode.OutcomeUnknown, message, inner)
    {
        OperationId = operationId;
        TransactionId = transactionId;
    }

    /// <summary>Creates the exception; the required <see cref="OperationId"/> must be set with an object initializer.</summary>
    public DfsCommitOutcomeUnknownException(string message, Exception? inner = null)
        : base(DfsErrorCode.OutcomeUnknown, message, inner)
    {
    }

    /// <summary>Operation id to resolve the outcome with (also visible through <see cref="DfsException.OperationId"/>).</summary>
    public new required DfsOperationId OperationId
    {
        get => base.OperationId.GetValueOrDefault();
        init => base.OperationId = value;
    }

    private DfsTransactionId? _transactionId;

    /// <summary>Transaction id, when the unknown outcome is a transaction commit.</summary>
    public DfsTransactionId? TransactionId
    {
        get => _transactionId;
        init => _transactionId = value;
    }

    /// <summary>Sets the transaction id after construction (used when decoding error replies).</summary>
    internal void SetTransactionId(DfsTransactionId? value) => _transactionId = value;
}
