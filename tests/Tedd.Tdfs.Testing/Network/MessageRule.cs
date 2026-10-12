namespace Tedd.Tdfs.Testing;

/// <summary>An active drop/duplicate/delay rule created by <see cref="TestNetwork"/>.</summary>
public sealed class MessageRule
{
    internal MessageRule(MessageFilter filter, int count)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        Filter = filter;
        Count = count;
    }

    /// <summary>The filter selecting affected messages.</summary>
    public MessageFilter Filter { get; }

    /// <summary>Number of messages the rule applies to in total.</summary>
    public int Count { get; }

    /// <summary>Messages still to be affected.</summary>
    public int Remaining => throw Stub.NotYet("MessageRule.Remaining");

    /// <summary>Messages affected so far.</summary>
    public int Matched => throw Stub.NotYet("MessageRule.Matched");

    /// <summary>Deactivates the rule.</summary>
    public void Cancel() => throw Stub.NotYet("MessageRule.Cancel");
}
