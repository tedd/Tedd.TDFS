namespace Tedd.Tdfs;

/// <summary>
/// Transaction validation failed because something it read changed (<see cref="DfsErrorCode.TransactionConflict"/>).
/// Nothing was applied; the application may retry the whole transaction body.
/// </summary>
public class DfsTransactionConflictException : DfsException
{
    /// <summary>Creates the exception for <paramref name="transactionId"/>.</summary>
    public DfsTransactionConflictException(DfsTransactionId transactionId, string message, Exception? inner = null)
        : this(DfsErrorCode.TransactionConflict, transactionId, message, inner)
    {
    }

    /// <summary>Creates the exception without a known transaction id.</summary>
    public DfsTransactionConflictException(string message, Exception? inner = null)
        : this(DfsErrorCode.TransactionConflict, default, message, inner)
    {
    }

    /// <summary>Constructor for derived transaction failures.</summary>
    protected DfsTransactionConflictException(DfsErrorCode code, DfsTransactionId transactionId, string message, Exception? inner)
        : base(code, message, inner)
    {
        TransactionId = transactionId;
        IsRetryable = true;
    }

    private DfsTransactionId _transactionId;

    /// <summary>The transaction that failed.</summary>
    public DfsTransactionId TransactionId
    {
        get => _transactionId;
        init => _transactionId = value;
    }

    /// <summary>Sets the transaction id after construction (used when decoding error replies).</summary>
    internal void SetTransactionId(DfsTransactionId value) => _transactionId = value;
}
