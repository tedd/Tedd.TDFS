namespace Tedd.Tdfs.Protocol;

/// <summary>
/// A record embedded inside protocol messages (headers, list elements, administrative bodies). Like a message body, the
/// encoding of every protocol record ends with its own extension block, so fields can be added to list elements
/// without breaking older readers (FMT-003).
/// </summary>
/// <typeparam name="TSelf">The implementing record.</typeparam>
public interface IWireRecord<TSelf>
    where TSelf : IWireRecord<TSelf>
{
    /// <summary>
    /// Smallest possible encoding of the record in bytes (at least 1). Decoders use it to reject collection counts that
    /// cannot fit in the remaining input before allocating (SEC-014/015).
    /// </summary>
    static abstract int MinEncodedSize { get; }

    /// <summary>Writes the record, ending with its extension block.</summary>
    void Write(ref WireWriter writer);

    /// <summary>Reads the record, including its extension block.</summary>
    static abstract TSelf Read(ref WireReader reader);
}
