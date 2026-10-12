using System.Buffers;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Encodes and decodes standalone protocol records (administrative bodies, capability tokens, embedded descriptors)
/// with the same strictness as <see cref="MessageCodec"/>: malformed input and trailing bytes throw
/// <see cref="DfsProtocolException"/>.
/// </summary>
public static class WireRecordCodec
{
    /// <summary>Encodes <paramref name="record"/>.</summary>
    public static byte[] Encode<T>(T record) where T : IWireRecord<T>
    {
        if (record is null)
            throw new ArgumentNullException(nameof(record));
        var buffer = new ArrayBufferWriter<byte>(128);
        var writer = new WireWriter(buffer);
        record.Write(ref writer);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Decodes a complete <typeparamref name="T"/> encoding.</summary>
    /// <exception cref="DfsProtocolException">The input is malformed (<see cref="DfsErrorCode.ProtocolError"/>) or requires an
    /// unsupported extension (<see cref="DfsErrorCode.Unsupported"/>).</exception>
    public static T Decode<T>(ReadOnlySpan<byte> data, WireLimits? limits = null) where T : IWireRecord<T>
    {
        var reader = new WireReader(data, limits ?? WireLimits.Default);
        T record;
        try
        {
            record = T.Read(ref reader);
            reader.EnsureEnd();
        }
        catch (DfsProtocolException)
        {
            throw;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            throw WireReader.Malformed($"Malformed {typeof(T).Name}: {ex.Message}", ex);
        }

        return record;
    }
}
