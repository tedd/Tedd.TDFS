namespace Tedd.Tdfs.Testing;

// Additions for the OPS suite (docs/api-additions/fmt-ops-perf.md): per-transmission view of the network log.
public sealed partial class TestNetwork
{
    /// <summary>
    /// Every message transmission matching <paramref name="filter"/> that the harness link policy observed since the cluster
    /// started (or since the last <see cref="MessageLog.Reset"/>), in observation order, including messages that were
    /// dropped, delayed, duplicated, held or black-holed (OPS-007).
    /// </summary>
    /// <remarks>
    /// Duplicates created by <see cref="DuplicateNext"/> appear once per delivered copy with the same correlation id. The
    /// returned list is a snapshot; later traffic does not change it.
    /// </remarks>
    public IReadOnlyList<MessageAttempt> GetMessageAttempts(MessageFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return Fabric.Policy.GetAttempts(filter);
    }
}
