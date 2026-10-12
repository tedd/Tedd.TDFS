namespace Tedd.Tdfs.Testing;

// Harness additions requested by the RST and SEC suites (see docs/api-additions/rst-sec.md).
public sealed partial class TestNetwork
{
    /// <summary>
    /// Number of contact attempts addressed to the endpoint named <paramref name="name"/> ("n0", "b0", "client:c0", ...)
    /// since the fabric was created: every connection attempt and every message (request, reply or one-way) whose
    /// destination is <paramref name="name"/>, counted when the sender hands it to the transport — whether it was
    /// delivered, dropped, delayed, black-holed, refused by a cut/partition/isolation, or addressed to a crashed,
    /// stopped or destroyed endpoint. Senders on a recovery cluster sharing the fabric
    /// (<see cref="TestCluster.StartRecoveryClusterAsync"/>) are counted too, and so are backup nodes and every client a
    /// test created. Excluded is only the harness's own traffic towards endpoints of the cluster that owns them (that
    /// cluster's <see cref="TestCluster.Admin"/> client, inspectors and stability/invariant checks), which is not part of any
    /// recovery environment and may keep reconnecting to a destroyed node. Used to prove that a recovery procedure made
    /// no undeclared call to a source service (test plan §9.8).
    /// </summary>
    public long CountAttemptsTo(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return Fabric.AttemptsTo(name);
    }

    /// <summary>
    /// Tampers with the next <paramref name="count"/> transmissions of messages matching <paramref name="filter"/>
    /// <b>below</b> the transport's integrity protection (SEC-011).
    /// </summary>
    /// <remarks>
    /// For <see cref="TransportKind.Quic"/> the harness's datagram interposer (between the sender's and receiver's UDP
    /// sockets) applies <paramref name="mode"/> to the encrypted QUIC packet(s) carrying the matched frame, after
    /// encryption; the receiver's TLS 1.3 AEAD/QUIC packet protection must detect it. For
    /// <see cref="TransportKind.InMemory"/> (and the deterministic simulated network) the harness applies the same
    /// mutation to the frame's protected envelope after the sender's integrity protection (MAC) is applied, so the
    /// receiver's integrity check must detect it. The application-level frame is never mutated before protection, so a
    /// receiver that accepts the tampered bytes as a valid message is a defect. <see cref="MessageRule.Matched"/> counts
    /// tampered transmissions (proof the tamper point was reached). Retransmissions by the transport itself are not
    /// tampered unless <paramref name="count"/> covers them.
    /// <para>Implementation status: the in-memory and simulated transports carry frames without any integrity protection,
    /// so the harness alters the transmitted frame itself (the receiver's own validation is what is exercised; a receiver
    /// that accepts an altered frame as a different valid message is reported by the test). QUIC tampering needs a datagram
    /// interposer between MsQuic sockets, which this harness does not provide: the call throws
    /// <see cref="NotSupportedException"/> for <see cref="TransportKind.Quic"/> rather than fabricating evidence.</para>
    /// </remarks>
    public MessageRule TamperNext(MessageFilter filter, TransportTamper mode, int count = 1)
    {
        ArgumentNullException.ThrowIfNull(filter);
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown tamper mode.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        if (Fabric.Kind == TransportKind.Quic)
        {
            throw new NotSupportedException(
                "Tampering below QUIC packet protection requires a UDP datagram interposer between the MsQuic sockets, which the harness does not provide.");
        }

        return AddRule(new MessageRule(filter, count, MessageRuleKind.Tamper, tamper: mode));
    }
}
