namespace Tedd.Tdfs.Testing;

// Harness additions requested by the STR suite (see docs/api-additions/str.md).
public sealed partial class TestFaults
{
    /// <summary>
    /// Forces legal, verified <b>short read results</b> from the backend (STR-004, STR-006).
    /// </summary>
    /// <remarks>
    /// <para>
    /// While the returned rule is active, every read result produced by a matching node carries at most
    /// <paramref name="maxBytesPerResult"/> payload bytes even when the request asked for more. This covers a volume
    /// leader's <c>ReadRange</c> result when it carries inline file bytes and a storage node's <c>GetObject</c> range
    /// result (and any other server reply that returns file/object payload to a client).
    /// </para>
    /// <para>
    /// The shortened payload is always a correct prefix of the requested range of the selected revision/object and its
    /// integrity information covers exactly the bytes returned (it is a verifiable short result, not corruption). The
    /// reply must not claim end-of-file or end-of-object for the shortened range: the requested range still exists.
    /// </para>
    /// <para>
    /// The SDK must treat such a reply as an ordinary short backend result: it may surface a short count to the caller
    /// (FSD §6.2: a nonempty read returns the number of bytes obtained, possibly fewer than requested) or issue further
    /// requests to fill the caller's buffer, but it must never pad, zero-fill, report EOF, fault the stream or fail the
    /// call because of the short reply.
    /// </para>
    /// <para>
    /// The rule is cluster-wide unless <paramref name="node"/> (a live node name such as <c>"n1"</c>) or
    /// <paramref name="volume"/> restricts it. It applies to requests that arrive after this call returns, from any
    /// client, until <see cref="ReadResultLimitRule.Cancel"/> is called or the cluster is disposed. Rules never affect
    /// node-to-node replication, repair or backup transfer traffic. Every reply that was actually shortened increments
    /// <see cref="ReadResultLimitRule.Applied"/>; replies whose payload already fits are not counted.
    /// </para>
    /// </remarks>
    /// <param name="maxBytesPerResult">Largest payload per read result; must be at least 1.</param>
    /// <param name="node">Restrict to results served by this node; null = every live node.</param>
    /// <param name="volume">Restrict to reads of this volume; null = every volume.</param>
    /// <returns>The active rule.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxBytesPerResult"/> is less than 1.</exception>
    public ReadResultLimitRule LimitReadResultBytes(int maxBytesPerResult, string? node = null, string? volume = null) =>
        throw Stub.NotYet("TestFaults.LimitReadResultBytes");
}
