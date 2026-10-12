namespace Tedd.Tdfs;

/// <summary>
/// A read pin or enumeration continuation expired (<see cref="DfsErrorCode.PinExpired"/> or
/// <see cref="DfsErrorCode.ContinuationExpired"/>); the read is not silently restarted against another version.
/// </summary>
public class DfsPinExpiredException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.PinExpired"/>.</summary>
    public DfsPinExpiredException(string message, Exception? inner = null)
        : this(DfsErrorCode.PinExpired, message, inner)
    {
    }

    /// <summary>Creates the exception with <see cref="DfsErrorCode.PinExpired"/> or <see cref="DfsErrorCode.ContinuationExpired"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="code"/> is neither of the allowed codes.</exception>
    public DfsPinExpiredException(DfsErrorCode code, string message, Exception? inner = null)
        : base(Check(code), message, inner)
    {
    }

    private static DfsErrorCode Check(DfsErrorCode code)
        => code is DfsErrorCode.PinExpired or DfsErrorCode.ContinuationExpired
            ? code
            : throw new ArgumentOutOfRangeException(nameof(code), code, "Expected PinExpired or ContinuationExpired.");
}
