namespace Tedd.Tdfs.Testing;

/// <summary>Transport used between nodes and clients of a test cluster.</summary>
public enum TransportKind
{
    /// <summary>In-process message hub (or the simulated network in deterministic mode).</summary>
    InMemory,

    /// <summary>Real QUIC over loopback (Tedd.Quicly).</summary>
    Quic,
}
