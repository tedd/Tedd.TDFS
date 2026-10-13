namespace Tedd.Tdfs.Testing;

/// <summary>What a <see cref="MessageRule"/> does to the messages it matches.</summary>
internal enum MessageRuleKind
{
    /// <summary>The message is dropped (the connection stays open).</summary>
    Drop,

    /// <summary>The request is delivered and executed; its reply is discarded.</summary>
    DropReply,

    /// <summary>The message is delivered twice.</summary>
    Duplicate,

    /// <summary>The message is delivered after an extra delay (it may be overtaken).</summary>
    Delay,

    /// <summary>Only a prefix of the frame reaches the receiver, then the connection closes.</summary>
    Truncate,

    /// <summary>The transmitted bytes are altered (below integrity protection where the transport has it).</summary>
    Tamper,
}

/// <summary>An active drop/duplicate/delay rule created by <see cref="TestNetwork"/>.</summary>
/// <remarks>
/// A rule applies to the next <see cref="Count"/> messages matching <see cref="Filter"/>, in the order the network observes
/// them, and then deactivates itself. Rules are evaluated in creation order; the first matching active rule of a kind that
/// applies to a message consumes it.
/// </remarks>
public sealed class MessageRule
{
    private int _matched;
    private int _cancelled;

    internal MessageRule(MessageFilter filter, int count)
        : this(filter, count, MessageRuleKind.Drop)
    {
    }

    internal MessageRule(MessageFilter filter, int count, MessageRuleKind kind, TimeSpan delay = default, int keepBytes = 0,
        TransportTamper tamper = TransportTamper.FlipBit)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        Filter = filter;
        Count = count;
        Kind = kind;
        Delay = delay;
        KeepBytes = keepBytes;
        Tamper = tamper;
    }

    /// <summary>The filter selecting affected messages.</summary>
    public MessageFilter Filter { get; }

    /// <summary>Number of messages the rule applies to in total.</summary>
    public int Count { get; }

    /// <summary>Messages still to be affected.</summary>
    public int Remaining => IsCancelled ? 0 : Math.Max(0, Count - Volatile.Read(ref _matched));

    /// <summary>Messages affected so far.</summary>
    public int Matched => Volatile.Read(ref _matched);

    internal MessageRuleKind Kind { get; }

    internal TimeSpan Delay { get; }

    internal int KeepBytes { get; }

    internal TransportTamper Tamper { get; }

    internal bool IsCancelled => Volatile.Read(ref _cancelled) != 0;

    /// <summary>True while the rule can still affect messages.</summary>
    internal bool IsActive => !IsCancelled && Volatile.Read(ref _matched) < Count;

    /// <summary>Deactivates the rule.</summary>
    public void Cancel() => Interlocked.Exchange(ref _cancelled, 1);

    /// <inheritdoc />
    public override string ToString() => $"{Kind} rule (matched {Matched}/{Count}{(IsCancelled ? ", cancelled" : string.Empty)})";

    /// <summary>Claims one application of the rule; false when it is exhausted or cancelled.</summary>
    internal bool TryConsume()
    {
        while (true)
        {
            if (IsCancelled)
                return false;
            int matched = Volatile.Read(ref _matched);
            if (matched >= Count)
                return false;
            if (Interlocked.CompareExchange(ref _matched, matched + 1, matched) == matched)
                return true;
        }
    }
}
