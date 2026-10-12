namespace Tedd.Tdfs;

/// <summary>
/// Base type of availability failures (no leader, node down, quorum or placement unavailable, capacity). Retryable:
/// nothing authoritative is known to have happened unless a more specific exception says otherwise.
/// </summary>
public class DfsUnavailableException : DfsException
{
    /// <summary>Creates the exception with error code <see cref="DfsErrorCode.Unavailable"/>.</summary>
    public DfsUnavailableException(string message, Exception? inner = null)
        : this(DfsErrorCode.Unavailable, message, inner)
    {
    }

    /// <summary>
    /// Creates the exception with a specific availability code (typically <see cref="DfsErrorCode.Unavailable"/>,
    /// <see cref="DfsErrorCode.NodeUnavailable"/> or <see cref="DfsErrorCode.NotLeader"/>).
    /// </summary>
    public DfsUnavailableException(DfsErrorCode code, string message, Exception? inner = null)
        : base(code, message, inner)
    {
        IsRetryable = true;
    }

    private string? _leaderHint;

    /// <summary>Leader endpoint hint supplied with <see cref="DfsErrorCode.NotLeader"/>, when known.</summary>
    public string? LeaderHint
    {
        get => _leaderHint;
        init => _leaderHint = value;
    }

    /// <summary>Sets the leader hint after construction (used when decoding error replies).</summary>
    internal void SetLeaderHint(string? value) => _leaderHint = value;
}
