namespace Tedd.Tdfs.Transport;

/// <summary>A link policy decision for one message.</summary>
/// <param name="Kind">The action.</param>
/// <param name="Delay">Delivery delay, for <see cref="LinkDecisionKind.Delay"/>.</param>
public readonly record struct LinkDecision(LinkDecisionKind Kind, TimeSpan Delay = default)
{
    /// <summary>Deliver normally.</summary>
    public static LinkDecision Deliver => new(LinkDecisionKind.Deliver);

    /// <summary>Drop the message.</summary>
    public static LinkDecision Drop => new(LinkDecisionKind.Drop);

    /// <summary>Deliver the message twice.</summary>
    public static LinkDecision Duplicate => new(LinkDecisionKind.Duplicate);

    /// <summary>Discard the message while keeping the connection open.</summary>
    public static LinkDecision BlackHole => new(LinkDecisionKind.BlackHole);

    /// <summary>Deliver after <paramref name="delay"/>.</summary>
    public static LinkDecision Delayed(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        return new LinkDecision(LinkDecisionKind.Delay, delay);
    }
}
