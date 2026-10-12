using System.Buffers.Binary;
using System.Text;

namespace Tedd.Tdfs.Transport.Internal;

/// <summary>Parsed header of one envelope frame.</summary>
/// <param name="Kind">Message kind.</param>
/// <param name="Correlation">Correlation id (requests and one-way messages: the sender's counter; replies: the request's id).</param>
/// <param name="FragmentIndex">Index of this fragment (0-based).</param>
/// <param name="FragmentCount">Number of fragments of the message (at least 1).</param>
internal readonly record struct EnvelopeHeader(RpcMessageKind Kind, ulong Correlation, uint FragmentIndex, uint FragmentCount);

/// <summary>A peer sent a frame that violates the envelope protocol; the connection is closed.</summary>
internal sealed class QuiclyProtocolException : Exception
{
    public QuiclyProtocolException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// The RPC envelope carried in Quicly messages (ARCHITECTURE §10.2): <c>{u8 kind, u64 correlation, u32 fragmentIndex,
/// u32 fragmentCount, bytes}</c>, all integers little-endian. Frames are at most <see cref="MaxFrameSize"/> bytes so a
/// Quicly reliable message never approaches its 64 KiB limit; larger payloads are fragmented. Single-frame messages
/// travel on the ordered control channel, fragmented ones on the unordered data channel.
/// </summary>
internal static class QuiclyEnvelope
{
    /// <summary>Envelope header bytes.</summary>
    public const int HeaderSize = 1 + 8 + 4 + 4;

    /// <summary>Largest frame (header plus chunk) handed to Quicly.</summary>
    public const int MaxFrameSize = 60 * 1024;

    /// <summary>Largest payload chunk of one frame.</summary>
    public const int MaxChunkSize = MaxFrameSize - HeaderSize;

    /// <summary>Quicly channel of single-frame messages (ReliableOrdered, LZ4).</summary>
    public const ushort ControlChannel = 2;

    /// <summary>Quicly channel of fragmented messages (ReliableUnordered).</summary>
    public const ushort DataChannel = 3;

    /// <summary>Largest network identity in UTF-8 bytes (preamble and auth token).</summary>
    public const int MaxNameBytes = 1024;

    private static readonly byte[] AuthTokenMagic = "TDFS"u8.ToArray();
    private const byte AuthTokenVersion = 1;
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Number of frames a payload of <paramref name="payloadLength"/> bytes needs.</summary>
    public static int FragmentCount(int payloadLength)
        => payloadLength <= MaxChunkSize ? 1 : (int)(((long)payloadLength + MaxChunkSize - 1) / MaxChunkSize);

    /// <summary>Writes one frame into <paramref name="destination"/> and returns its length.</summary>
    public static int WriteFrame(Span<byte> destination, RpcMessageKind kind, ulong correlation, uint fragmentIndex, uint fragmentCount, ReadOnlySpan<byte> chunk)
    {
        if (chunk.Length > MaxChunkSize)
            throw new ArgumentOutOfRangeException(nameof(chunk), chunk.Length, "The chunk exceeds the frame size.");
        destination[0] = (byte)kind;
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(1, 8), correlation);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(9, 4), fragmentIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(13, 4), fragmentCount);
        chunk.CopyTo(destination.Slice(HeaderSize));
        return HeaderSize + chunk.Length;
    }

    /// <summary>Parses and validates a frame.</summary>
    /// <returns>False with <paramref name="error"/> set when the frame is malformed.</returns>
    public static bool TryParse(ReadOnlySpan<byte> frame, out EnvelopeHeader header, out ReadOnlySpan<byte> chunk, out string? error)
    {
        header = default;
        chunk = default;
        if (frame.Length < HeaderSize)
        {
            error = $"The frame is {frame.Length} bytes, shorter than the {HeaderSize}-byte envelope header.";
            return false;
        }

        if (frame.Length > MaxFrameSize)
        {
            error = $"The frame is {frame.Length} bytes, longer than the {MaxFrameSize}-byte limit.";
            return false;
        }

        var kind = (RpcMessageKind)frame[0];
        if (!RpcMessageKinds.IsApplication(kind) && kind != RpcMessageKind.Preamble)
        {
            error = $"Unknown envelope kind {frame[0]}.";
            return false;
        }

        ulong correlation = BinaryPrimitives.ReadUInt64LittleEndian(frame.Slice(1, 8));
        uint index = BinaryPrimitives.ReadUInt32LittleEndian(frame.Slice(9, 4));
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(frame.Slice(13, 4));
        if (count == 0 || index >= count)
        {
            error = $"Invalid fragment {index} of {count}.";
            return false;
        }

        chunk = frame.Slice(HeaderSize);
        if (kind == RpcMessageKind.Preamble && (count != 1 || correlation != 0))
        {
            error = "A preamble must be a single frame with correlation 0.";
            return false;
        }

        if (count > 1 && chunk.IsEmpty)
        {
            error = "A fragment of a multi-frame message must not be empty.";
            return false;
        }

        header = new EnvelopeHeader(kind, correlation, index, count);
        error = null;
        return true;
    }

    /// <summary>Validates a network identity for the wire (non-empty, at most <see cref="MaxNameBytes"/> UTF-8 bytes, no control characters).</summary>
    /// <exception cref="ArgumentException">The name is not usable.</exception>
    public static void ValidateName(string name, string paramName)
    {
        ArgumentException.ThrowIfNullOrEmpty(name, paramName);
        if (Encoding.UTF8.GetByteCount(name) > MaxNameBytes)
            throw new ArgumentException($"A network identity is at most {MaxNameBytes} UTF-8 bytes.", paramName);
        foreach (char c in name)
        {
            if (char.IsControl(c))
                throw new ArgumentException("A network identity must not contain control characters.", paramName);
        }

        if (!IsWellFormed(name))
            throw new ArgumentException("A network identity must be well-formed UTF-16 (no unpaired surrogates).", paramName);
    }

    /// <summary>Encodes a network identity as UTF-8.</summary>
    public static byte[] EncodeName(string name) => StrictUtf8.GetBytes(name);

    /// <summary>Decodes a network identity received from a peer.</summary>
    public static bool TryDecodeName(ReadOnlySpan<byte> utf8, out string? name)
    {
        name = null;
        if (utf8.IsEmpty || utf8.Length > MaxNameBytes)
            return false;
        string decoded;
        try
        {
            decoded = StrictUtf8.GetString(utf8);
        }
        catch (DecoderFallbackException)
        {
            return false;
        }

        foreach (char c in decoded)
        {
            if (char.IsControl(c))
                return false;
        }

        name = decoded;
        return true;
    }

    /// <summary>The Quicly auth token a client presents: <c>"TDFS" u8 version utf8 name</c>.</summary>
    public static byte[] EncodeAuthToken(string name)
    {
        byte[] utf8 = EncodeName(name);
        byte[] token = new byte[AuthTokenMagic.Length + 1 + utf8.Length];
        AuthTokenMagic.CopyTo(token, 0);
        token[AuthTokenMagic.Length] = AuthTokenVersion;
        utf8.CopyTo(token, AuthTokenMagic.Length + 1);
        return token;
    }

    /// <summary>Decodes a client's auth token into its network identity.</summary>
    public static bool TryDecodeAuthToken(ReadOnlySpan<byte> token, out string? name)
    {
        name = null;
        if (token.Length <= AuthTokenMagic.Length + 1 || !token.StartsWith(AuthTokenMagic) || token[AuthTokenMagic.Length] != AuthTokenVersion)
            return false;
        return TryDecodeName(token.Slice(AuthTokenMagic.Length + 1), out name);
    }

    private static bool IsWellFormed(string text)
    {
        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            if (char.IsHighSurrogate(c))
            {
                if (i + 1 >= text.Length || !char.IsLowSurrogate(text[i + 1]))
                    return false;
                i++;
            }
            else if (char.IsLowSurrogate(c))
            {
                return false;
            }
        }

        return true;
    }
}
