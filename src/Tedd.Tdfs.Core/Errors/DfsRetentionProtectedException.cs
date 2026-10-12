namespace Tedd.Tdfs;

/// <summary>Deleting or shortening protected backup history was refused.</summary>
public class DfsRetentionProtectedException : UnauthorizedAccessException
{
    /// <summary>Creates the exception.</summary>
    public DfsRetentionProtectedException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }

    /// <summary>Always <see cref="DfsErrorCode.RetentionProtected"/>.</summary>
    public DfsErrorCode ErrorCode => DfsErrorCode.RetentionProtected;
}
