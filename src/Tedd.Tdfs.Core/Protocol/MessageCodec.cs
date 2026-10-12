using System.Buffers;
using System.Buffers.Binary;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Frame codec: <c>[u16 MessageType little-endian][body]</c>, with a registry of message decoders keyed by type.
/// Decoding is strict: unknown types, malformed bodies and trailing bytes throw <see cref="DfsProtocolException"/>.
/// </summary>
public static partial class MessageCodec
{
    /// <summary>Size of the frame header.</summary>
    public const int HeaderSize = sizeof(ushort);

    private static readonly Lock RegistrationLock = new();
    private static volatile Dictionary<MessageType, Registration> _registry = new();

    static MessageCodec()
    {
        RegisterBuiltIns();
    }

    private delegate IWireMessage Decoder(ref WireReader reader);

    /// <summary>Encodes a message as a complete frame.</summary>
    public static byte[] Encode(IWireMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var buffer = new ArrayBufferWriter<byte>(256);
        Encode(message, buffer);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Encodes a message as a complete frame into <paramref name="output"/>.</summary>
    public static void Encode(IWireMessage message, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(output);
        var writer = new WireWriter(output);
        writer.WriteUInt16((ushort)message.Type);
        message.Write(ref writer);
    }

    /// <summary>Decodes a complete frame.</summary>
    /// <exception cref="DfsProtocolException">The frame is truncated, has an unknown type, a malformed body or trailing bytes
    /// (<see cref="DfsErrorCode.ProtocolError"/>), or requires an unsupported extension (<see cref="DfsErrorCode.Unsupported"/>).</exception>
    public static IWireMessage Decode(ReadOnlySpan<byte> frame, WireLimits? limits = null)
    {
        if (!TryReadType(frame, out MessageType type))
            throw WireReader.Malformed("Frame is shorter than the message type header.");
        if (!_registry.TryGetValue(type, out Registration? registration))
            throw WireReader.Malformed($"Unknown message type {(ushort)type}.");

        var reader = new WireReader(frame[HeaderSize..], limits ?? WireLimits.Default);
        IWireMessage message;
        try
        {
            message = registration.Decoder(ref reader);
            reader.EnsureEnd();
        }
        catch (DfsProtocolException)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            throw WireReader.Malformed($"Malformed {type} message body: {ex.Message}", ex);
        }

        if (message is null || message.Type != type)
            throw WireReader.Malformed($"Decoder for {type} produced an inconsistent message.");
        return message;
    }

    /// <summary>Decodes a complete frame and requires it to be a <typeparamref name="T"/>.</summary>
    /// <exception cref="DfsProtocolException">The frame is malformed or of another message type.</exception>
    public static T Decode<T>(ReadOnlySpan<byte> frame, WireLimits? limits = null) where T : IWireMessage
    {
        IWireMessage message = Decode(frame, limits);
        return message is T typed
            ? typed
            : throw WireReader.Malformed($"Expected a {typeof(T).Name} message but received {message.Type}.");
    }

    /// <summary>True when a decoder is registered for <paramref name="type"/>.</summary>
    public static bool IsKnown(MessageType type) => _registry.ContainsKey(type);

    /// <summary>Reads the message type from a frame header without decoding the body.</summary>
    public static bool TryReadType(ReadOnlySpan<byte> frame, out MessageType type)
    {
        if (frame.Length < HeaderSize)
        {
            type = default;
            return false;
        }

        type = (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(frame);
        return true;
    }

    /// <summary>All message types with a registered decoder, in ascending order.</summary>
    public static IReadOnlyList<MessageType> RegisteredTypes => _registry.Keys.Order().ToArray();

    /// <summary>The CLR type registered for a message type, if any.</summary>
    public static Type? GetMessageClrType(MessageType type) => _registry.TryGetValue(type, out Registration? r) ? r.ClrType : null;

    /// <summary>Registers the decoder of <typeparamref name="T"/>. Idempotent for the same type; conflicting registrations throw.</summary>
    /// <exception cref="InvalidOperationException">Another CLR type is already registered for the same message type.</exception>
    internal static void Register<T>() where T : IWireMessage<T>
    {
        MessageType type = T.WireType;
        lock (RegistrationLock)
        {
            Dictionary<MessageType, Registration> current = _registry;
            if (current.TryGetValue(type, out Registration? existing))
            {
                if (existing.ClrType == typeof(T))
                    return;
                throw new InvalidOperationException(
                    $"Message type {type} is already registered to {existing.ClrType.FullName}; cannot register {typeof(T).FullName}.");
            }

            var next = new Dictionary<MessageType, Registration>(current)
            {
                [type] = new Registration(typeof(T), static (ref WireReader reader) => T.Read(ref reader)),
            };
            _registry = next;
        }
    }

    private sealed record Registration(Type ClrType, Decoder Decoder);
}
