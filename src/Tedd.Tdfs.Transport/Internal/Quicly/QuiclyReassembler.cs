namespace Tedd.Tdfs.Transport.Internal;

/// <summary>
/// Reassembles fragmented envelope messages of one connection and direction. Fragments may arrive in any order (the data
/// channel is unordered). Memory grows with the bytes actually received, never with a declared size, and is bounded per
/// message (<see cref="MaxMessageBytes"/>), per connection (<see cref="MaxBufferedBytes"/>) and in the number of messages
/// in progress (<see cref="MaxConcurrentMessages"/>). Every violation throws <see cref="QuiclyProtocolException"/>.
/// Not thread-safe: used by the connection's Quicly game thread only.
/// </summary>
internal sealed class QuiclyReassembler
{
    /// <summary>Default bound on messages being reassembled at once.</summary>
    public const int DefaultMaxConcurrentMessages = 256;

    private readonly Dictionary<(RpcMessageKind Kind, ulong Correlation), Partial> _partials = new();
    private long _buffered;

    /// <summary>Creates a reassembler.</summary>
    /// <param name="maxMessageBytes">Largest reassembled message.</param>
    /// <param name="maxBufferedBytes">Largest total of buffered fragment bytes.</param>
    /// <param name="maxConcurrentMessages">Most messages in progress at once.</param>
    public QuiclyReassembler(int maxMessageBytes, long maxBufferedBytes, int maxConcurrentMessages = DefaultMaxConcurrentMessages)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxMessageBytes, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBufferedBytes, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrentMessages, 1);
        MaxMessageBytes = maxMessageBytes;
        MaxBufferedBytes = maxBufferedBytes;
        MaxConcurrentMessages = maxConcurrentMessages;
        MaxFragments = QuiclyEnvelope.FragmentCount(maxMessageBytes);
    }

    /// <summary>Largest reassembled message in bytes.</summary>
    public int MaxMessageBytes { get; }

    /// <summary>Largest total of buffered fragment bytes.</summary>
    public long MaxBufferedBytes { get; }

    /// <summary>Most messages in progress at once.</summary>
    public int MaxConcurrentMessages { get; }

    /// <summary>Largest fragment count a message may declare.</summary>
    public int MaxFragments { get; }

    /// <summary>Messages currently in progress.</summary>
    public int PendingMessages => _partials.Count;

    /// <summary>Fragment bytes currently buffered.</summary>
    public long BufferedBytes => _buffered;

    /// <summary>Adds one fragment of a multi-frame message.</summary>
    /// <param name="header">The validated envelope header (<see cref="EnvelopeHeader.FragmentCount"/> &gt; 1).</param>
    /// <param name="chunk">The fragment's bytes (copied).</param>
    /// <returns>The complete message when this was its last missing fragment; otherwise null.</returns>
    /// <exception cref="QuiclyProtocolException">The fragment is inconsistent, duplicated or exceeds a limit.</exception>
    public byte[]? Add(in EnvelopeHeader header, ReadOnlySpan<byte> chunk)
    {
        if (header.FragmentCount < 2)
            throw new QuiclyProtocolException("A single-frame message was handed to the reassembler.");
        if (header.FragmentCount > (uint)MaxFragments)
            throw new QuiclyProtocolException($"A message declares {header.FragmentCount} fragments; at most {MaxFragments} are allowed ({MaxMessageBytes} bytes).");
        if (chunk.Length is 0 or > QuiclyEnvelope.MaxChunkSize)
            throw new QuiclyProtocolException($"A fragment of {chunk.Length} bytes is outside 1..{QuiclyEnvelope.MaxChunkSize}.");

        var key = (header.Kind, header.Correlation);
        if (!_partials.TryGetValue(key, out Partial? partial))
        {
            if (_partials.Count >= MaxConcurrentMessages)
                throw new QuiclyProtocolException($"More than {MaxConcurrentMessages} fragmented messages are in progress.");
            partial = new Partial((int)header.FragmentCount);
            _partials.Add(key, partial);
        }
        else if (partial.Chunks.Length != header.FragmentCount)
        {
            throw new QuiclyProtocolException($"Fragment {header.FragmentIndex} of {header.Kind} {header.Correlation} declares {header.FragmentCount} fragments; earlier fragments declared {partial.Chunks.Length}.");
        }

        if (partial.Chunks[header.FragmentIndex] is not null)
            throw new QuiclyProtocolException($"Fragment {header.FragmentIndex} of {header.Kind} {header.Correlation} arrived twice.");
        if ((long)partial.Total + chunk.Length > MaxMessageBytes) // long: MaxMessageBytes may be close to int.MaxValue
            throw new QuiclyProtocolException($"A fragmented message exceeds {MaxMessageBytes} bytes.");
        if (_buffered + chunk.Length > MaxBufferedBytes)
            throw new QuiclyProtocolException($"Fragmented messages in progress exceed {MaxBufferedBytes} buffered bytes.");

        partial.Chunks[header.FragmentIndex] = chunk.ToArray();
        partial.Received++;
        partial.Total += chunk.Length;
        _buffered += chunk.Length;
        if (partial.Received < partial.Chunks.Length)
            return null;

        _partials.Remove(key);
        _buffered -= partial.Total;
        byte[] message = new byte[partial.Total];
        int offset = 0;
        foreach (byte[]? part in partial.Chunks)
        {
            part!.CopyTo(message, offset);
            offset += part.Length;
        }

        return message;
    }

    /// <summary>Discards every message in progress.</summary>
    public void Clear()
    {
        _partials.Clear();
        _buffered = 0;
    }

    private sealed class Partial
    {
        public Partial(int count)
        {
            Chunks = new byte[]?[count];
        }

        public byte[]?[] Chunks { get; }

        public int Received { get; set; }

        public int Total { get; set; }
    }
}
