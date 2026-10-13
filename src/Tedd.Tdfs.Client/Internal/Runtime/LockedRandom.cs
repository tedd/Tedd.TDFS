namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// Serializes access to an injected <see cref="IRandomSource"/>: <see cref="DfsClient"/> is thread-safe, while seeded
/// sources are not. Draws stay deterministic for a deterministic call order (the simulation runs one thread).
/// </summary>
internal sealed class LockedRandom : IRandomSource
{
    private readonly Lock _sync = new();
    private readonly IRandomSource _inner;

    public LockedRandom(IRandomSource inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
    }

    /// <inheritdoc />
    public ulong NextUInt64()
    {
        lock (_sync)
            return _inner.NextUInt64();
    }

    /// <inheritdoc />
    public int Next(int maxExclusive)
    {
        lock (_sync)
            return _inner.Next(maxExclusive);
    }

    /// <inheritdoc />
    public void Fill(Span<byte> destination)
    {
        lock (_sync)
            _inner.Fill(destination);
    }

    /// <inheritdoc />
    public IRandomSource Fork(string purpose)
    {
        lock (_sync)
            return new LockedRandom(_inner.Fork(purpose));
    }

    /// <summary>A random non-zero 128-bit value (nonces, request ids).</summary>
    public UInt128 NextUInt128()
    {
        while (true)
        {
            UInt128 value;
            lock (_sync)
                value = new UInt128(_inner.NextUInt64(), _inner.NextUInt64());
            if (value != UInt128.Zero)
                return value;
        }
    }

    /// <summary>A random non-zero object id chosen by the uploader (ARCHITECTURE §2.2).</summary>
    public ObjectId NextObjectId()
    {
        while (true)
        {
            ObjectId id;
            lock (_sync)
                id = ObjectId.New(_inner);
            if (!id.IsEmpty)
                return id;
        }
    }
}
