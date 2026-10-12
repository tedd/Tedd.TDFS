namespace Tedd.Tdfs.Transport;

/// <summary>
/// Per-message fault interposition consulted by every transport (partitions, asymmetric cuts, black holes, drops,
/// delays, duplication). Production uses <see cref="AllowAllLinkPolicy"/>.
/// </summary>
public interface ILinkPolicy
{
    /// <summary>Decides what happens to one message.</summary>
    /// <param name="fromName">Sender network identity.</param>
    /// <param name="toName">Receiver network identity.</param>
    /// <param name="direction">Request, reply or one-way.</param>
    /// <param name="payloadPrefix">Leading bytes of the payload (at least the frame header when available).</param>
    LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix);

    /// <summary>True when <paramref name="fromName"/> may open a connection to <paramref name="toName"/>.</summary>
    bool CanConnect(string fromName, string toName);
}
