using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server;

/// <summary>
/// Everything a node takes from its environment. Server code never reads the system clock, system randomness, the
/// file system or the network directly; it uses these (ARCHITECTURE.md §3.1).
/// </summary>
public sealed class NodeEnvironment
{
    /// <summary>Local storage device.</summary>
    public required IStorageMedium Medium { get; init; }

    /// <summary>Creates the node's RPC transport.</summary>
    public required IRpcTransportFactory TransportFactory { get; init; }

    /// <summary>Clock (wall and monotonic) and timers.</summary>
    public required TimeProvider Time { get; init; }

    /// <summary>Randomness (deterministic under simulation).</summary>
    public required IRandomSource Random { get; init; }

    /// <summary>Fault injector (production: <see cref="NoFaults.Instance"/>).</summary>
    public IFaultInjector Faults { get; init; } = NoFaults.Instance;

    /// <summary>Log sink.</summary>
    public INodeLog Log { get; init; } = NullNodeLog.Instance;

    /// <summary>Event-loop factory (the simulation supplies its own).</summary>
    public INodeLoopFactory LoopFactory { get; init; } = ThreadNodeLoopFactory.Instance;

    /// <summary>
    /// Volume state machine factory override (tests); null uses the build's production factory
    /// (<c>VolumeHost.ResolveDefaultFactory</c>).
    /// </summary>
    internal Volumes.IVolumeStateMachineFactory? VolumeStateMachines { get; init; }
}
