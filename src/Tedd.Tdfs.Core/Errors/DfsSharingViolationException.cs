namespace Tedd.Tdfs;

/// <summary>
/// A share reservation or writer-exclusivity conflict (<see cref="DfsErrorCode.SharingViolation"/> or
/// <see cref="DfsErrorCode.WriterConflict"/>). Retryable once the conflicting handle closes.
/// </summary>
public class DfsSharingViolationException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.SharingViolation"/>.</summary>
    public DfsSharingViolationException(string message, Exception? inner = null)
        : this(DfsErrorCode.SharingViolation, message, inner)
    {
    }

    /// <summary>Creates the exception with <see cref="DfsErrorCode.SharingViolation"/> or <see cref="DfsErrorCode.WriterConflict"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="code"/> is neither of the allowed codes.</exception>
    public DfsSharingViolationException(DfsErrorCode code, string message, Exception? inner = null)
        : base(Check(code), message, inner)
    {
        IsRetryable = true;
    }

    private static DfsErrorCode Check(DfsErrorCode code)
        => code is DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict
            ? code
            : throw new ArgumentOutOfRangeException(nameof(code), code, "Expected SharingViolation or WriterConflict.");
}
