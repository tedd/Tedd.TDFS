namespace Tedd.Tdfs;

/// <summary>A non-recursive delete targeted a non-empty directory.</summary>
public class DfsDirectoryNotEmptyException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.DirectoryNotEmpty"/>.</summary>
    public DfsDirectoryNotEmptyException(string message, Exception? inner = null)
        : base(DfsErrorCode.DirectoryNotEmpty, message, inner)
    {
    }
}
