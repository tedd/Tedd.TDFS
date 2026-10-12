namespace Tedd.Tdfs.Transport;

/// <summary>A connection could not be established, failed, or was closed while a call was in flight.</summary>
public sealed class RpcConnectionException : IOException
{
    /// <summary>Creates the exception.</summary>
    public RpcConnectionException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }
}
