namespace Tedd.Tdfs.Transport;

/// <summary>Production link policy: every connection is allowed and every message delivered.</summary>
public sealed class AllowAllLinkPolicy : ILinkPolicy
{
    /// <summary>The shared instance.</summary>
    public static readonly AllowAllLinkPolicy Instance = new();

    private AllowAllLinkPolicy()
    {
    }

    /// <summary>Always <see cref="LinkDecision.Deliver"/>.</summary>
    public LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix) => LinkDecision.Deliver;

    /// <summary>Always true.</summary>
    public bool CanConnect(string fromName, string toName) => true;
}
