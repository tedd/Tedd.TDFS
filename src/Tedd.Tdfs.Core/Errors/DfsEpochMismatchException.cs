namespace Tedd.Tdfs;

/// <summary>A token, handle or session belongs to a different volume epoch.</summary>
public class DfsEpochMismatchException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.EpochMismatch"/>.</summary>
    public DfsEpochMismatchException(string message, Exception? inner = null)
        : base(DfsErrorCode.EpochMismatch, message, inner)
    {
    }
}
