namespace Tedd.Tdfs;

/// <summary>A majority of the configured voters is unavailable (<see cref="DfsErrorCode.QuorumUnavailable"/>).</summary>
public class DfsQuorumUnavailableException : DfsUnavailableException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.QuorumUnavailable"/>.</summary>
    public DfsQuorumUnavailableException(string message, Exception? inner = null)
        : base(DfsErrorCode.QuorumUnavailable, message, inner)
    {
    }
}
