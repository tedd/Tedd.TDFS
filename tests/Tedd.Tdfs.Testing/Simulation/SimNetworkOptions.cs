using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>Configuration of a <see cref="SimNetwork"/>.</summary>
public sealed record SimNetworkOptions
{
    /// <summary>Minimum one-way latency (default 100 µs). Each message samples uniformly from [min, max] (seeded).</summary>
    public TimeSpan MinLatency { get; init; } = TimeSpan.FromMicroseconds(100);

    /// <summary>Maximum one-way latency (default 1 ms).</summary>
    public TimeSpan MaxLatency { get; init; } = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// How long a connection attempt over a link the policy blocks (<see cref="ILinkPolicy.CanConnect"/> false) takes to
    /// fail with <see cref="RpcConnectionException"/> (default 1 s of virtual time).
    /// </summary>
    public TimeSpan BlockedConnectTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// When true (default) the peers of a killed process observe a connection reset (after one latency, if the link
    /// allows it): their pending calls fail with <see cref="RpcConnectionException"/>. When false they observe nothing and
    /// their calls time out.
    /// </summary>
    public bool ResetPeersOnProcessDeath { get; init; } = true;

    /// <summary>Validates the options.</summary>
    /// <exception cref="ArgumentException">A value is out of range.</exception>
    public void Validate()
    {
        if (MinLatency < TimeSpan.Zero)
            throw new ArgumentException("MinLatency must not be negative.", nameof(MinLatency));
        if (MaxLatency < MinLatency)
            throw new ArgumentException("MaxLatency must not be less than MinLatency.", nameof(MaxLatency));
        if (BlockedConnectTimeout <= TimeSpan.Zero)
            throw new ArgumentException("BlockedConnectTimeout must be positive.", nameof(BlockedConnectTimeout));
    }
}

/// <summary>Per-direction link characteristics overriding <see cref="SimNetworkOptions"/>.</summary>
/// <param name="MinLatency">Minimum one-way latency.</param>
/// <param name="MaxLatency">Maximum one-way latency.</param>
/// <param name="BytesPerSecond">Bandwidth (0 = unlimited). Messages on the direction queue behind each other.</param>
public sealed record SimLinkSettings(TimeSpan MinLatency, TimeSpan MaxLatency, long BytesPerSecond = 0)
{
    /// <summary>Validates the settings.</summary>
    /// <exception cref="ArgumentException">A value is out of range.</exception>
    public void Validate()
    {
        if (MinLatency < TimeSpan.Zero)
            throw new ArgumentException("MinLatency must not be negative.", nameof(MinLatency));
        if (MaxLatency < MinLatency)
            throw new ArgumentException("MaxLatency must not be less than MinLatency.", nameof(MaxLatency));
        if (BytesPerSecond < 0)
            throw new ArgumentException("BytesPerSecond must not be negative.", nameof(BytesPerSecond));
    }
}

/// <summary>One application message handed to the simulated network, with the link policy's decision.</summary>
/// <param name="ConnectionId">Connection the message travels on.</param>
/// <param name="From">Sender network identity.</param>
/// <param name="To">Receiver network identity.</param>
/// <param name="Direction">Request, reply or one-way.</param>
/// <param name="Payload">The message bytes (a private copy).</param>
/// <param name="Decision">What the link policy decided.</param>
/// <param name="VirtualTime">Virtual time of the send.</param>
public readonly record struct SimMessageEvent(
    long ConnectionId,
    string From,
    string To,
    LinkDirection Direction,
    ReadOnlyMemory<byte> Payload,
    LinkDecision Decision,
    TimeSpan VirtualTime);
