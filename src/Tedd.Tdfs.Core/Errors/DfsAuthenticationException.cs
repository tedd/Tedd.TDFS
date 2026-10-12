namespace Tedd.Tdfs;

/// <summary>Authentication failed (wrong principal/secret, wrong cluster secret, removed node).</summary>
public class DfsAuthenticationException : UnauthorizedAccessException
{
    /// <summary>Creates the exception.</summary>
    public DfsAuthenticationException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }

    /// <summary>Always <see cref="DfsErrorCode.AuthenticationFailed"/>.</summary>
    public DfsErrorCode ErrorCode => DfsErrorCode.AuthenticationFailed;
}
