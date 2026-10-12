namespace Tedd.Tdfs;

/// <summary>The target is read-only (snapshot views and backup hosts).</summary>
public class DfsReadOnlyException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.ReadOnly"/>.</summary>
    public DfsReadOnlyException(string message, Exception? inner = null)
        : base(DfsErrorCode.ReadOnly, message, inner)
    {
    }
}
