namespace Tedd.Tdfs.Testing;

/// <summary>Counts delivered messages and bytes per filter.</summary>
public sealed class MessageLog
{
    internal MessageLog()
    {
    }

    /// <summary>Number of logged messages matching <paramref name="filter"/>.</summary>
    public long CountMessages(MessageFilter filter) => throw Stub.NotYet("MessageLog.CountMessages");

    /// <summary>Number of logged frame bytes matching <paramref name="filter"/>.</summary>
    public long CountBytes(MessageFilter filter) => throw Stub.NotYet("MessageLog.CountBytes");

    /// <summary>Clears the log.</summary>
    public void Reset() => throw Stub.NotYet("MessageLog.Reset");
}
