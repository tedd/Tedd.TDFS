using System.Security.Cryptography;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Canonical payload identity of a mutating request, used to bind an operation id to its arguments (FSD §13, OUT-003):
/// a retry with the same id and the same identity returns the stored outcome; the same id with a different identity is
/// rejected with <see cref="DfsErrorCode.OperationIdMismatch"/>.
/// </summary>
/// <remarks>
/// <para>
/// The identity is the first <see cref="HashSize"/> bytes of SHA-256 over the encoded request frame in identity form
/// (<see cref="NormalizeRequest"/>). The header fields that legitimately differ between retries are cleared:
/// <see cref="RequestHeader.Session"/> (a retry may come from another process/session of the same principal),
/// <see cref="RequestHeader.AckWatermark"/>, <see cref="RequestHeader.ExpectedVolumeId"/> and
/// <see cref="RequestHeader.ExpectedVolumeEpoch"/> (routing assertions, checked separately) and the header's extension
/// block.
/// </para>
/// <para>
/// Publications (<see cref="CommitFileMessage"/>, <see cref="WriteAllMessage"/>, <see cref="AppendMessage"/>) also drop
/// their per-attempt upload artifacts: the receipts, the staging session and the uploader-chosen random object ids of
/// <see cref="RangeChangeKind.Data"/> changes. A client that lost its process (or its staged uploads) and retries with a
/// persisted operation id has to upload the content again — under a new staging session, with new random object ids
/// and new receipts — and must still receive the stored outcome instead of an id mismatch (OUT-002, OUT-006). The
/// content itself stays covered: every change's range, kind, object offset and length, checksum and encoding, and the
/// inline bytes. (SDKs therefore split content into objects deterministically, at <c>ChunkSize</c> boundaries.)
/// </para>
/// <para>
/// The volume name, operation id, transaction id, every other request field and the request's own extensions are part
/// of the identity.
/// </para>
/// </remarks>
public static class OperationPayload
{
    /// <summary>Size of the payload identity in bytes.</summary>
    public const int HashSize = 16;

    /// <summary>Returns the header used for payload identity.</summary>
    public static RequestHeader Normalize(RequestHeader header)
    {
        ArgumentNullException.ThrowIfNull(header);
        return header with
        {
            Session = 0,
            AckWatermark = 0,
            ExpectedVolumeId = default,
            ExpectedVolumeEpoch = 0,
            Extensions = [],
        };
    }

    /// <summary>
    /// Returns <paramref name="request"/> in identity form: the header normalized (<see cref="Normalize"/>) and, for
    /// publications, the receipts, the staging session and the object ids of data changes cleared.
    /// </summary>
    public static IVolumeRequest NormalizeRequest(IVolumeRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        IVolumeRequest normalized = request.WithHeader(Normalize(request.Header));
        return normalized switch
        {
            CommitFileMessage commit => commit with { Changes = NormalizeChanges(commit.Changes), Receipts = [], Staging = default },
            WriteAllMessage writeAll => writeAll with { Changes = NormalizeChanges(writeAll.Changes), Receipts = [], Staging = default },
            AppendMessage append => append with { Changes = NormalizeChanges(append.Changes), Receipts = [], Staging = default },
            _ => normalized,
        };
    }

    /// <summary>Computes the payload identity of <paramref name="request"/>.</summary>
    public static byte[] ComputeHash(IVolumeRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        byte[] frame = MessageCodec.Encode(NormalizeRequest(request));
        return SHA256.HashData(frame).AsSpan(0, HashSize).ToArray();
    }

    private static IReadOnlyList<RangeChange> NormalizeChanges(IReadOnlyList<RangeChange>? changes)
    {
        if (changes is null || changes.Count == 0)
            return [];
        var result = new RangeChange[changes.Count];
        for (int i = 0; i < result.Length; i++)
        {
            RangeChange change = changes[i] ?? throw new ArgumentException("A change list must not contain null elements.", nameof(changes));
            result[i] = change.Object.IsEmpty ? change : change with { Object = default };
        }

        return result;
    }
}
