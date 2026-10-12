using Tedd.Tdfs.Transport.Internal;

namespace Tedd.Tdfs.Transport;

/// <summary>
/// In-process network hub of named <c>mem://</c> endpoints with asynchronous delivery and link-policy fault
/// interposition (ARCHITECTURE §10.3). This is the realtime in-memory transport; deterministic simulation uses its own
/// transport on the simulation scheduler.
/// </summary>
/// <remarks>
/// <para><b>Endpoints.</b> <see cref="Factory"/> creates one <see cref="IRpcTransport"/> per network identity. A transport
/// listening on <c>mem://name</c> registers <c>name</c> when it starts and unregisters it when disposed; connecting to an
/// unregistered (or stopped) name fails with <see cref="RpcConnectionException"/>, as does a connection the policy's
/// <see cref="ILinkPolicy.CanConnect"/> refuses.</para>
/// <para><b>Delivery.</b> Connections are bidirectional. Every message is copied, then delivered on the thread pool (never on
/// the sender's stack) in send order per connection and direction. The <see cref="Policy"/> is consulted once per message
/// with the sender's and receiver's network identities, the direction and the complete payload; a delayed message may be
/// overtaken by later ones.</para>
/// </remarks>
public sealed class InMemoryNetwork
{
    private readonly object _gate = new();
    private readonly Dictionary<string, InMemoryRpcTransport> _endpoints = new(StringComparer.Ordinal);
    private readonly HashSet<InMemoryRpcTransport> _transports = new(ReferenceEqualityComparer.Instance);
    private ILinkPolicy _policy;

    /// <summary>Creates a network; a null policy allows everything.</summary>
    public InMemoryNetwork(ILinkPolicy? policy = null)
        : this(policy, TimeProvider.System)
    {
    }

    /// <summary>Creates a network with an explicit time source for call timeouts and link delays.</summary>
    /// <param name="policy">The link policy; null allows everything.</param>
    /// <param name="timeProvider">Time source for call timeouts and <see cref="LinkDecisionKind.Delay"/> decisions.</param>
    public InMemoryNetwork(ILinkPolicy? policy, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _policy = policy ?? AllowAllLinkPolicy.Instance;
        TimeProvider = timeProvider;
        Factory = new InMemoryTransportFactory(this);
    }

    /// <summary>Factory creating transports attached to this network.</summary>
    public IRpcTransportFactory Factory { get; }

    /// <summary>The link policy consulted for every connection and message; setting null restores allow-all.</summary>
    public ILinkPolicy Policy
    {
        get => Volatile.Read(ref _policy);
        set => Volatile.Write(ref _policy, value ?? AllowAllLinkPolicy.Instance);
    }

    /// <summary>Time source for call timeouts and link delays.</summary>
    public TimeProvider TimeProvider { get; }

    /// <summary>The names of the endpoints currently listening, in ordinal order.</summary>
    public IReadOnlyList<string> Endpoints
    {
        get
        {
            lock (_gate)
            {
                string[] names = [.. _endpoints.Keys];
                Array.Sort(names, StringComparer.Ordinal);
                return names;
            }
        }
    }

    /// <summary>
    /// Abortively closes every connection that has the network identity <paramref name="name"/> at either end (both ends
    /// observe <see cref="IRpcConnection.Closed"/> with an exception; pending calls fail with
    /// <see cref="RpcConnectionException"/>). Listening transports keep listening, so peers can reconnect.
    /// </summary>
    public void ResetConnections(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        InMemoryRpcTransport[] transports;
        lock (_gate)
        {
            transports = [.. _transports];
        }

        foreach (InMemoryRpcTransport transport in transports)
            transport.ResetConnections(string.Equals(transport.LocalName, name, StringComparison.Ordinal) ? null : name);
    }

    /// <summary>Tracks a transport created by <see cref="Factory"/> until it is disposed.</summary>
    internal void Track(InMemoryRpcTransport transport)
    {
        lock (_gate)
        {
            _transports.Add(transport);
        }
    }

    /// <summary>Stops tracking a disposed transport.</summary>
    internal void Untrack(InMemoryRpcTransport transport)
    {
        lock (_gate)
        {
            _transports.Remove(transport);
        }
    }

    /// <summary>Registers a listening transport under <paramref name="name"/>; false when the name is taken.</summary>
    internal bool TryRegister(string name, InMemoryRpcTransport transport)
    {
        lock (_gate)
        {
            return _endpoints.TryAdd(name, transport);
        }
    }

    /// <summary>Removes the registration of <paramref name="transport"/> under <paramref name="name"/>, if it is the registered one.</summary>
    internal void Unregister(string name, InMemoryRpcTransport transport)
    {
        lock (_gate)
        {
            if (_endpoints.TryGetValue(name, out InMemoryRpcTransport? registered) && ReferenceEquals(registered, transport))
                _endpoints.Remove(name);
        }
    }

    /// <summary>Finds the transport listening under <paramref name="name"/>.</summary>
    internal bool TryResolve(string name, out InMemoryRpcTransport? transport)
    {
        lock (_gate)
        {
            return _endpoints.TryGetValue(name, out transport);
        }
    }
}
