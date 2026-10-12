namespace Tedd.Tdfs;

/// <summary>
/// A malformed protocol message (<see cref="DfsErrorCode.ProtocolError"/>) or a message requiring an unsupported
/// feature (<see cref="DfsErrorCode.Unsupported"/>).
/// </summary>
public class DfsProtocolException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.ProtocolError"/>.</summary>
    public DfsProtocolException(string message, Exception? inner = null)
        : this(DfsErrorCode.ProtocolError, message, inner)
    {
    }

    /// <summary>Creates the exception with <see cref="DfsErrorCode.ProtocolError"/> or <see cref="DfsErrorCode.Unsupported"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="code"/> is neither of the allowed codes.</exception>
    public DfsProtocolException(DfsErrorCode code, string message, Exception? inner = null)
        : base(Check(code), message, inner)
    {
    }

    private static DfsErrorCode Check(DfsErrorCode code)
        => code is DfsErrorCode.ProtocolError or DfsErrorCode.Unsupported
            ? code
            : throw new ArgumentOutOfRangeException(nameof(code), code, "Expected ProtocolError or Unsupported.");
}
