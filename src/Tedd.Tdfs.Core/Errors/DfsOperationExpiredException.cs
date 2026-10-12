namespace Tedd.Tdfs;

/// <summary>The operation id is outside the retained outcome window; it is rejected, never replayed as a new request.</summary>
public class DfsOperationExpiredException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.OperationExpired"/>.</summary>
    public DfsOperationExpiredException(string message, Exception? inner = null)
        : base(DfsErrorCode.OperationExpired, message, inner)
    {
    }
}
