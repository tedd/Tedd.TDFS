namespace Tedd.Tdfs;

/// <summary>Key material required to decrypt or verify recovery data is missing.</summary>
public class DfsRecoveryKeyMissingException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.RecoveryKeyMissing"/>.</summary>
    public DfsRecoveryKeyMissingException(string message, Exception? inner = null)
        : base(DfsErrorCode.RecoveryKeyMissing, message, inner)
    {
    }
}
