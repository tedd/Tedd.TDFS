namespace Tedd.Tdfs.Testing;

/// <summary>Captures matching messages until they are released or dropped.</summary>
public sealed class MessageHold : IDisposable
{
    internal MessageHold(MessageFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        Filter = filter;
    }

    /// <summary>The filter selecting held messages.</summary>
    public MessageFilter Filter { get; }

    /// <summary>Messages currently held.</summary>
    public int Count => throw Stub.NotYet("MessageHold.Count");

    /// <summary>All frames captured by this hold (for replay tests).</summary>
    public IReadOnlyList<CapturedMessage> Captured => throw Stub.NotYet("MessageHold.Captured");

    /// <summary>Waits until at least <paramref name="count"/> messages are held.</summary>
    public Task WaitForAsync(int count, TimeSpan timeout) => throw Stub.NotYet("MessageHold.WaitForAsync");

    /// <summary>Delivers all held messages (optionally in reverse order) and stops holding.</summary>
    public void ReleaseAll(bool reverseOrder = false) => throw Stub.NotYet("MessageHold.ReleaseAll");

    /// <summary>Discards all held messages.</summary>
    public void DropAll() => throw Stub.NotYet("MessageHold.DropAll");

    /// <summary>Releases remaining messages and stops holding.</summary>
    public void Dispose() => throw Stub.NotYet("MessageHold.Dispose");
}
