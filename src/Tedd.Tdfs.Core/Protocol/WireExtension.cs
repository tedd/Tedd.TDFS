namespace Tedd.Tdfs.Protocol;

/// <summary>
/// One entry of a message's terminal extension block. Readers ignore unknown optional extensions and reject unknown
/// required ones with <see cref="DfsErrorCode.Unsupported"/> (FMT-002/003).
/// </summary>
/// <param name="FieldId">Extension field id (unique within a message).</param>
/// <param name="Required">True when a reader that does not understand the field must reject the message.</param>
/// <param name="Value">Encoded field value.</param>
public readonly record struct WireExtension(uint FieldId, bool Required, ReadOnlyMemory<byte> Value);
