namespace Tedd.Tdfs;

/// <summary>An operation id was reused with a different principal, operation kind or payload.</summary>
public class DfsOperationIdMismatchException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.OperationIdMismatch"/>.</summary>
    public DfsOperationIdMismatchException(string message, Exception? inner = null)
        : base(DfsErrorCode.OperationIdMismatch, message, inner)
    {
    }
}
