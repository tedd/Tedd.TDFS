namespace Tedd.Tdfs;

/// <summary>The destination entry already exists.</summary>
public class DfsAlreadyExistsException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.AlreadyExists"/>.</summary>
    public DfsAlreadyExistsException(string message, Exception? inner = null)
        : base(DfsErrorCode.AlreadyExists, message, inner)
    {
    }
}
