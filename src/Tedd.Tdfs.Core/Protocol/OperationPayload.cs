using System.Security.Cryptography;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Canonical payload identity of a mutating request, used to bind an operation id to its arguments (FSD §13, OUT-003):
/// a retry with the same id and the same identity returns the stored outcome; the same id with a different identity is
/// rejected with <see cref="DfsErrorCode.OperationIdMismatch"/>.
/// </summary>
/// <remarks>
/// The identity is the first <see cref="HashSize"/> bytes of SHA-256 over the encoded request frame after normalizing
/// the header fields that legitimately differ between retries: <see cref="RequestHeader.Session"/> (a retry may come
/// from another process/session of the same principal), <see cref="RequestHeader.AckWatermark"/>,
/// <see cref="RequestHeader.ExpectedVolumeId"/> and <see cref="RequestHeader.ExpectedVolumeEpoch"/> (routing assertions,
/// checked separately) and the header's extension block are cleared. The volume name, operation id, transaction id,
/// every request field and the request's own extension block are part of the identity.
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

    /// <summary>Computes the payload identity of <paramref name="request"/>.</summary>
    public static byte[] ComputeHash(IVolumeRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        byte[] frame = MessageCodec.Encode(request.WithHeader(Normalize(request.Header)));
        return SHA256.HashData(frame).AsSpan(0, HashSize).ToArray();
    }
}
