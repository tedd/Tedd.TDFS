namespace Tedd.Tdfs;

/// <summary>
/// The transaction was aborted by the system (leader change, expiry, limits) — <see cref="DfsErrorCode.TransactionAborted"/>.
/// Nothing was applied; the application may retry the whole transaction body.
/// </summary>
public class DfsTransactionAbortedException : DfsTransactionConflictException
{
    /// <summary>Creates the exception for <paramref name="transactionId"/>.</summary>
    public DfsTransactionAbortedException(DfsTransactionId transactionId, string message, Exception? inner = null)
        : base(DfsErrorCode.TransactionAborted, transactionId, message, inner)
    {
    }

    /// <summary>Creates the exception without a known transaction id.</summary>
    public DfsTransactionAbortedException(string message, Exception? inner = null)
        : base(DfsErrorCode.TransactionAborted, default, message, inner)
    {
    }
}
