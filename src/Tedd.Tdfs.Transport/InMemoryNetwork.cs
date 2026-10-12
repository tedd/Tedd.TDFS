namespace Tedd.Tdfs.Transport;

/// <summary>
/// In-process network hub of named <c>mem://</c> endpoints with asynchronous delivery and link-policy fault
/// interposition (ARCHITECTURE §10.3).
/// </summary>
public sealed class InMemoryNetwork
{
    private ILinkPolicy _policy;

    /// <summary>Creates a network; a null policy allows everything.</summary>
    public InMemoryNetwork(ILinkPolicy? policy = null)
    {
        _policy = policy ?? AllowAllLinkPolicy.Instance;
    }

    /// <summary>Factory creating transports attached to this network.</summary>
    public IRpcTransportFactory Factory => throw new NotImplementedException("InMemoryNetwork.Factory: implemented in a later phase");

    /// <summary>The link policy consulted for every connection and message; setting null restores allow-all.</summary>
    public ILinkPolicy Policy
    {
        get => Volatile.Read(ref _policy);
        set => Volatile.Write(ref _policy, value ?? AllowAllLinkPolicy.Instance);
    }
}
