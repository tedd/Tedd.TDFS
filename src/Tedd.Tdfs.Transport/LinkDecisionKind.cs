namespace Tedd.Tdfs.Transport;

/// <summary>What a link policy does with one message.</summary>
public enum LinkDecisionKind
{
    /// <summary>Deliver normally.</summary>
    Deliver,
    /// <summary>Drop the message (the sender's call times out).</summary>
    Drop,
    /// <summary>Deliver after <see cref="LinkDecision.Delay"/>.</summary>
    Delay,
    /// <summary>Deliver twice (application-level duplication).</summary>
    Duplicate,
    /// <summary>Silently discard while the connection stays open (half-open link).</summary>
    BlackHole,
}
