using System.Globalization;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Network control for a test cluster. Names: "n0".."n9", backups "b0".., clients "client:c0". Operations affect both
/// directions unless stated otherwise.
/// </summary>
/// <remarks>
/// <para>Every transport of the cluster (nodes, backup nodes, clients, raw clients, recovery clusters sharing the fabric)
/// consults the same link policy for every message and connection attempt, so rules apply uniformly to the simulated,
/// in-memory and QUIC transports. Partition groups and cuts may name any endpoint, including clients; names a partition
/// does not list are unaffected by it.</para>
/// <para>Latency and bandwidth are applied by the simulated network itself in deterministic mode (FIFO per connection
/// direction) and by delay decisions in realtime mode.</para>
/// </remarks>
public sealed partial class TestNetwork
{
    private readonly NetworkFabric _fabric;
    private readonly Action<string> _trace;

    internal TestNetwork(NetworkFabric fabric, Action<string> trace)
    {
        _fabric = fabric;
        _trace = trace;
    }

    /// <summary>Message statistics.</summary>
    public MessageLog Log => Fabric.Log;

    internal NetworkFabric Fabric => _fabric;

    /// <summary>Nodes not in the same group cannot exchange messages.</summary>
    public void Partition(params string[][] groups)
    {
        ArgumentNullException.ThrowIfNull(groups);
        foreach (string[] group in groups)
        {
            ArgumentNullException.ThrowIfNull(group);
            foreach (string name in group)
                ArgumentException.ThrowIfNullOrWhiteSpace(name);
        }

        Fabric.Policy.SetPartition(groups);
        Trace("partition " + string.Join(" | ", groups.Select(g => string.Join(",", g))));
    }

    /// <summary>Cuts every link of <paramref name="name"/>.</summary>
    public void Isolate(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Fabric.Policy.Isolate(name);
        Trace("isolate " + name);
    }

    /// <summary>Cuts one direction only.</summary>
    public void Cut(string from, string to)
    {
        ValidatePair(from, to);
        Fabric.Policy.SetBlock(from, to, LinkBlock.Cut);
        Trace($"cut {from}->{to}");
    }

    /// <summary>Cuts both directions between two names.</summary>
    public void CutBoth(string a, string b)
    {
        ValidatePair(a, b);
        Fabric.Policy.SetBlock(a, b, LinkBlock.Cut);
        Fabric.Policy.SetBlock(b, a, LinkBlock.Cut);
        Trace($"cut {a}<->{b}");
    }

    /// <summary>Connection stays open but messages vanish (half-open).</summary>
    public void BlackHole(string from, string to)
    {
        ValidatePair(from, to);
        Fabric.Policy.SetBlock(from, to, LinkBlock.BlackHole);
        Trace($"black hole {from}->{to}");
    }

    /// <summary>Restores one direction.</summary>
    public void Restore(string from, string to)
    {
        ValidatePair(from, to);
        Fabric.Policy.RemoveBlock(from, to);
        Trace($"restore {from}->{to}");
    }

    /// <summary>Removes all cuts, black holes and mesh restrictions.</summary>
    public void Heal()
    {
        Fabric.Policy.Heal();
        Trace("heal");
    }

    /// <summary>Adds latency to one direction.</summary>
    public void SetLatency(string from, string to, TimeSpan latency)
    {
        ValidatePair(from, to);
        ArgumentOutOfRangeException.ThrowIfLessThan(latency, TimeSpan.Zero);
        if (Fabric.Sim is { } sim)
            sim.SetLatency(from, to, latency);
        else
            Fabric.Policy.SetLatency(from, to, latency);
        Trace(string.Create(CultureInfo.InvariantCulture, $"latency {from}->{to} = {latency.TotalMilliseconds} ms"));
    }

    /// <summary>Limits bandwidth of one direction.</summary>
    public void SetBandwidth(string from, string to, long bytesPerSecond)
    {
        ValidatePair(from, to);
        ArgumentOutOfRangeException.ThrowIfNegative(bytesPerSecond);
        if (Fabric.Sim is { } sim)
            sim.SetBandwidth(from, to, bytesPerSecond);
        else
            Fabric.Policy.SetBandwidth(from, to, bytesPerSecond);
        Trace(string.Create(CultureInfo.InvariantCulture, $"bandwidth {from}->{to} = {bytesPerSecond} B/s"));
    }

    /// <summary>Closes all connections of <paramref name="name"/> (they reconnect).</summary>
    public void ResetConnections(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Fabric.ResetConnections(name);
        Trace("reset connections of " + name);
    }

    /// <summary>Drops the next matching messages.</summary>
    public MessageRule DropNext(MessageFilter filter, int count = 1) => AddRule(new MessageRule(filter, count, MessageRuleKind.Drop));

    /// <summary>Delivers and executes matching requests but loses their replies.</summary>
    /// <remarks>The filter selects the <b>request</b> (for example <c>From</c> = client, <c>Type</c> = request type).</remarks>
    public MessageRule DropReplies(MessageFilter filter, int count = 1) => AddRule(new MessageRule(filter, count, MessageRuleKind.DropReply));

    /// <summary>Duplicates the next matching messages.</summary>
    public MessageRule DuplicateNext(MessageFilter filter, int count = 1) => AddRule(new MessageRule(filter, count, MessageRuleKind.Duplicate));

    /// <summary>Delays the next matching messages.</summary>
    public MessageRule DelayNext(MessageFilter filter, TimeSpan delay, int count = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        return AddRule(new MessageRule(filter, count, MessageRuleKind.Delay, delay));
    }

    /// <summary>Captures matching messages until released.</summary>
    public MessageHold Hold(MessageFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        var hold = new MessageHold(filter, Fabric);
        Fabric.AddHold(hold);
        Trace("hold " + Describe(filter));
        return hold;
    }

    /// <summary>Re-delivers a previously captured frame from its original sender identity; returns the reply error code.</summary>
    /// <remarks>
    /// The frame is sent on an open authenticated connection of the original sender to the original receiver (preferably the
    /// connection it was captured on), or on a fresh connection authenticated with the same principal when the harness knows
    /// its credentials. A request returns its reply's error code (<see cref="DfsErrorCode.None"/> for a success reply,
    /// <see cref="DfsErrorCode.Timeout"/> when no reply arrives); a one-way message returns <see cref="DfsErrorCode.None"/>
    /// once sent; a reply is discarded by the receiver as a late reply (<see cref="DfsErrorCode.None"/>);
    /// <see cref="DfsErrorCode.Unavailable"/> when no connection of the sender can be used.
    /// </remarks>
    public Task<DfsErrorCode> ReplayAsync(CapturedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return Fabric.ReplayAsync(message, TimeSpan.FromSeconds(10));
    }

    internal void Trace(string message) => _trace("net: " + message);

    internal static string Describe(MessageFilter filter)
    {
        var parts = new List<string>();
        if (filter.From is not null)
            parts.Add("from=" + filter.From);
        if (filter.To is not null)
            parts.Add("to=" + filter.To);
        if (filter.Type is not null)
            parts.Add("type=" + filter.Type.Value);
        if (filter.Operation is not null)
            parts.Add("op=" + filter.Operation.Value);
        if (filter.Direction is not null)
            parts.Add("dir=" + filter.Direction.Value);
        if (filter.Predicate is not null)
            parts.Add("predicate");
        return parts.Count == 0 ? "(any)" : string.Join(' ', parts);
    }

    private MessageRule AddRule(MessageRule rule)
    {
        Fabric.Policy.AddRule(rule);
        string extra = rule.Kind switch
        {
            MessageRuleKind.Delay => " by " + rule.Delay.ToString("c", CultureInfo.InvariantCulture),
            MessageRuleKind.Truncate => " to " + rule.KeepBytes.ToString(CultureInfo.InvariantCulture) + " byte(s)",
            MessageRuleKind.Tamper => " (" + rule.Tamper + ")",
            _ => string.Empty,
        };
        Trace($"{rule.Kind} next {rule.Count.ToString(CultureInfo.InvariantCulture)} {Describe(rule.Filter)}{extra}");
        return rule;
    }

    private static void ValidatePair(string from, string to)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(from);
        ArgumentException.ThrowIfNullOrWhiteSpace(to);
    }
}
