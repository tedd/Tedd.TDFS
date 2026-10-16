namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// Typed availability failures (FSD §13: quorum and placement loss surface as distinct typed exceptions; never a bare
/// timeout, docs/api-additions/mesh.md §10).
/// </summary>
internal static class Availability
{
    /// <summary>
    /// True when <paramref name="failure"/> (or a cause of it) shows that no leader could be found: a node answered that it
    /// knows no leader, or the quorum was reported unavailable.
    /// </summary>
    public static bool IsLeaderless(Exception? failure)
    {
        for (Exception? e = failure; e is not null; e = e.InnerException)
        {
            if (e is DfsQuorumUnavailableException || e is DfsUnavailableException { ErrorCode: DfsErrorCode.NotLeader, LeaderHint: null or "" })
                return true;
        }

        return false;
    }

    /// <summary>
    /// The failure of a request without namespace effect that got no answer: quorum loss when no leader could be found,
    /// otherwise a plain availability failure.
    /// </summary>
    public static DfsUnavailableException Unanswered(string message, Exception cause)
        => IsLeaderless(cause)
            ? new DfsQuorumUnavailableException(message + " No leader could be found: the metadata quorum is unavailable.", cause)
            : new DfsUnavailableException(DfsErrorCode.Unavailable, message, cause);
}
