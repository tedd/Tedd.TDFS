namespace Tedd.Tdfs.Protocol;

/// <summary>A protocol message that can be encoded as <c>[u16 MessageType][body]</c>.</summary>
public interface IWireMessage
{
    /// <summary>The message type written in the frame header.</summary>
    MessageType Type { get; }

    /// <summary>Writes the message body (without the type header), ending with its extension block.</summary>
    void Write(ref WireWriter writer);
}

/// <summary>A protocol message record that can also decode itself; register it with <see cref="MessageCodec"/>.</summary>
/// <typeparam name="TSelf">The implementing record.</typeparam>
public interface IWireMessage<TSelf> : IWireMessage
    where TSelf : IWireMessage<TSelf>
{
    /// <summary>The message type handled by <typeparamref name="TSelf"/>.</summary>
    static abstract MessageType WireType { get; }

    /// <summary>Reads a message body (without the type header). Must consume the terminal extension block.</summary>
    static abstract TSelf Read(ref WireReader reader);
}
