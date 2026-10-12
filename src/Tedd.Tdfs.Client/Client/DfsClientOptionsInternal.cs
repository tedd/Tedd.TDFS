using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs;

/// <summary>
/// Advanced client settings that are not part of the public surface. Injected by the test harness (and by hosts that
/// embed the SDK) through <see cref="DfsClient.ConnectAsync(DfsClientOptions, DfsClientOptionsInternal, CancellationToken)"/>.
/// A null member selects the production default.
/// </summary>
internal sealed record DfsClientOptionsInternal
{
    /// <summary>All production defaults (QUIC transport, system randomness, no faults, no mutations).</summary>
    public static DfsClientOptionsInternal None { get; } = new();

    /// <summary>Transport factory; null selects the QUIC transport configured from <see cref="DfsClientOptions.Security"/>.</summary>
    public IRpcTransportFactory? TransportFactory { get; init; }

    /// <summary>Random source (retry jitter, nonces, object ids); null selects <see cref="SystemRandomSource.Instance"/>.</summary>
    public IRandomSource? Random { get; init; }

    /// <summary>Fault injector consulted at client fault sites (F00-F03, F13-F14); null selects <see cref="NoFaults.Instance"/>.</summary>
    public IFaultInjector? Faults { get; init; }

    /// <summary>SDK-side safety mutations for negative-control tests; null selects <see cref="SafetyMutations.None"/>.</summary>
    public SafetyMutations? Mutations { get; init; }

    /// <summary>Network identity of the client (e.g. <c>client:c0</c>); null derives one from <see cref="DfsClientOptions.ClientName"/>.</summary>
    public string? NetworkName { get; init; }
}
