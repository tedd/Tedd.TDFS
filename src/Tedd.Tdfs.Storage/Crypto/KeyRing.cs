using System.Collections.Immutable;
using System.Security.Cryptography;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// Versioned set of 256-bit record keys (key version → key). New records are encrypted with
/// <see cref="CurrentVersion"/>; older versions are retained for decryption until every record using them is gone.
/// Thread-safe; updates replace an immutable snapshot.
/// </summary>
public sealed class KeyRing
{
    /// <summary>Required key length in bytes (AES-256).</summary>
    public const int KeySize = 32;

    private volatile State _state = new(ImmutableSortedDictionary<uint, byte[]>.Empty, 0);

    /// <summary>Creates an empty ring (no current version until a key is added).</summary>
    public KeyRing()
    {
    }

    /// <summary>Creates a ring holding one key, which becomes current.</summary>
    public KeyRing(uint version, ReadOnlySpan<byte> key)
    {
        Add(version, key);
    }

    /// <summary>Version used for new records (the highest added version unless changed with <see cref="SetCurrent"/>); 0 when empty.</summary>
    public uint CurrentVersion => _state.Current;

    /// <summary>All key versions held, ascending.</summary>
    public IReadOnlyList<uint> Versions => [.. _state.Keys.Keys];

    /// <summary>True when the ring holds no keys.</summary>
    public bool IsEmpty => _state.Keys.IsEmpty;

    /// <summary>
    /// Derives a ring holding the keys for versions 1..<paramref name="currentVersion"/> from a root secret with HKDF-SHA256
    /// (see <see cref="StorageKeys.DeriveKey"/>); <paramref name="currentVersion"/> becomes current.
    /// </summary>
    public static KeyRing Derive(ReadOnlySpan<byte> secret, string purpose, uint currentVersion = 1)
    {
        ArgumentOutOfRangeException.ThrowIfZero(currentVersion);
        var ring = new KeyRing();
        for (uint v = 1; v <= currentVersion; v++)
            ring.Add(v, StorageKeys.DeriveKey(secret, purpose, v));
        return ring;
    }

    /// <summary>
    /// Adds a key version. The highest version added so far becomes current. Adding an identical key again is a no-op.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="version"/> is 0 (reserved for "not encrypted").</exception>
    /// <exception cref="ArgumentException">The key is not 32 bytes, or a different key is already registered for the version.</exception>
    public void Add(uint version, ReadOnlySpan<byte> key)
    {
        ArgumentOutOfRangeException.ThrowIfZero(version);
        if (key.Length != KeySize)
            throw new ArgumentException($"Record keys must be {KeySize} bytes; got {key.Length}.", nameof(key));
        byte[] copy = key.ToArray();
        while (true)
        {
            State current = _state;
            if (current.Keys.TryGetValue(version, out byte[]? existing))
            {
                if (CryptographicOperations.FixedTimeEquals(existing, copy))
                    return;
                throw new ArgumentException($"A different key is already registered for version {version}.", nameof(key));
            }

            var next = new State(current.Keys.Add(version, copy), Math.Max(current.Current, version));
            if (Interlocked.CompareExchange(ref _state, next, current) == current)
                return;
        }
    }

    /// <summary>Makes an existing version current (for example to roll back a rotation that was never used).</summary>
    /// <exception cref="KeyNotFoundException">The version is not in the ring.</exception>
    public void SetCurrent(uint version)
    {
        while (true)
        {
            State current = _state;
            if (!current.Keys.ContainsKey(version))
                throw new KeyNotFoundException($"Key version {version} is not in the ring.");
            var next = current with { Current = version };
            if (Interlocked.CompareExchange(ref _state, next, current) == current)
                return;
        }
    }

    /// <summary>Removes a retired key version. The current version cannot be removed.</summary>
    /// <returns>True when the version was present.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="version"/> is the current version.</exception>
    public bool Remove(uint version)
    {
        while (true)
        {
            State current = _state;
            if (!current.Keys.ContainsKey(version))
                return false;
            if (current.Current == version)
                throw new InvalidOperationException($"Key version {version} is current and cannot be removed.");
            var next = current with { Keys = current.Keys.Remove(version) };
            if (Interlocked.CompareExchange(ref _state, next, current) == current)
                return true;
        }
    }

    /// <summary>True when the ring holds key material for <paramref name="version"/>.</summary>
    public bool Contains(uint version) => _state.Keys.ContainsKey(version);

    /// <summary>Gets the key for <paramref name="version"/> (the returned array must not be modified).</summary>
    internal bool TryGetKey(uint version, out byte[] key)
    {
        if (_state.Keys.TryGetValue(version, out byte[]? k))
        {
            key = k;
            return true;
        }

        key = [];
        return false;
    }

    private sealed record State(ImmutableSortedDictionary<uint, byte[]> Keys, uint Current);
}
