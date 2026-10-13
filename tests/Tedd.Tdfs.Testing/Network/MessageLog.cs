namespace Tedd.Tdfs.Testing;

/// <summary>Counts delivered messages and bytes per filter.</summary>
/// <remarks>
/// The log is fed by the harness link policy, which observes every application message once (requests, replies and one-way
/// messages, between nodes, backup nodes and clients). A message counts as delivered when the network decided to deliver it
/// (possibly delayed); a duplicated message counts once per delivered copy; dropped and black-holed messages do not count.
/// <see cref="TestNetwork.GetMessageAttempts"/> lists every observation, delivered or not.
/// </remarks>
public sealed class MessageLog
{
    private readonly HarnessLinkPolicy? _policy;

    internal MessageLog()
    {
    }

    internal MessageLog(HarnessLinkPolicy policy)
    {
        _policy = policy;
    }

    /// <summary>Number of logged messages matching <paramref name="filter"/>.</summary>
    public long CountMessages(MessageFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return Policy.CountDelivered(filter, out _);
    }

    /// <summary>Number of logged frame bytes matching <paramref name="filter"/>.</summary>
    public long CountBytes(MessageFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        Policy.CountDelivered(filter, out long bytes);
        return bytes;
    }

    /// <summary>Clears the log.</summary>
    public void Reset() => Policy.ResetLog();

    private HarnessLinkPolicy Policy => _policy ?? throw new InvalidOperationException("The log is not attached to a network.");
}
