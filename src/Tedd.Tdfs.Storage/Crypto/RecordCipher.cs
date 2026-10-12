using System.Security.Cryptography;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// AES-256-GCM <see cref="IRecordCipher"/> over a <see cref="KeyRing"/>. Sealed layout:
/// <c>nonce (12 bytes, from the injected <see cref="IRandomSource"/>) | ciphertext | tag (16 bytes)</c>.
/// </summary>
/// <remarks>
/// Nonces are random 96-bit values; callers must keep the number of records per key well below 2^32 (rotate keys).
/// Instances are thread-safe.
/// </remarks>
public sealed class RecordCipher : IRecordCipher, IDisposable
{
    /// <summary>Nonce size in bytes.</summary>
    public const int NonceSize = 12;

    /// <summary>Authentication tag size in bytes.</summary>
    public const int TagSize = 16;

    private readonly KeyRing _keys;
    private readonly IRandomSource _random;
    private readonly Lock _gate = new();
    private readonly Dictionary<uint, (byte[] Key, AesGcm Aes)> _instances = [];
    private bool _disposed;

    /// <summary>Creates a cipher over <paramref name="keys"/>, drawing nonces from <paramref name="random"/>.</summary>
    /// <exception cref="PlatformNotSupportedException">AES-GCM is not available on this platform.</exception>
    public RecordCipher(KeyRing keys, IRandomSource random)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(random);
        if (!AesGcm.IsSupported)
            throw new PlatformNotSupportedException("AES-GCM is not supported on this platform.");
        _keys = keys;
        _random = random;
    }

    /// <summary>The key ring (rotation adds versions to it).</summary>
    public KeyRing Keys => _keys;

    /// <inheritdoc />
    public int Overhead => NonceSize + TagSize;

    /// <inheritdoc />
    public uint CurrentKeyVersion => _keys.CurrentVersion;

    /// <inheritdoc />
    public bool CanDecrypt(uint keyVersion) => _keys.Contains(keyVersion);

    /// <inheritdoc />
    /// <exception cref="ArgumentException"><paramref name="output"/> is not exactly plaintext length + <see cref="Overhead"/> bytes.</exception>
    /// <exception cref="DfsRecoveryKeyMissingException">The ring holds no key for <paramref name="keyVersion"/>.</exception>
    public void Encrypt(ReadOnlySpan<byte> aad, ReadOnlySpan<byte> plain, Span<byte> output, uint keyVersion)
    {
        if (output.Length != plain.Length + Overhead)
            throw new ArgumentException($"Output must be exactly {plain.Length + Overhead} bytes; got {output.Length}.", nameof(output));
        Span<byte> nonce = output[..NonceSize];
        Span<byte> cipherText = output.Slice(NonceSize, plain.Length);
        Span<byte> tag = output[(NonceSize + plain.Length)..];
        _random.Fill(nonce);
        lock (_gate)
        {
            AesGcm aes = GetLocked(keyVersion)
                ?? throw new DfsRecoveryKeyMissingException($"No record key is available for key version {keyVersion}.");
            aes.Encrypt(nonce, plain, cipherText, tag, aad);
        }
    }

    /// <inheritdoc />
    public bool TryDecrypt(ReadOnlySpan<byte> aad, ReadOnlySpan<byte> sealedData, Span<byte> plain, uint keyVersion)
    {
        if (sealedData.Length < Overhead)
            return false;
        int length = sealedData.Length - Overhead;
        if (plain.Length < length)
            throw new ArgumentException($"Plaintext buffer must hold at least {length} bytes; got {plain.Length}.", nameof(plain));
        ReadOnlySpan<byte> nonce = sealedData[..NonceSize];
        ReadOnlySpan<byte> cipherText = sealedData.Slice(NonceSize, length);
        ReadOnlySpan<byte> tag = sealedData[(NonceSize + length)..];
        lock (_gate)
        {
            AesGcm? aes = GetLocked(keyVersion);
            if (aes is null)
                return false;
            try
            {
                aes.Decrypt(nonce, cipherText, tag, plain[..length], aad);
                return true;
            }
            catch (AuthenticationTagMismatchException)
            {
                CryptographicOperations.ZeroMemory(plain[..length]);
                return false;
            }
        }
    }

    /// <summary>Releases the cached AES instances.</summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            foreach ((byte[] _, AesGcm aes) in _instances.Values)
                aes.Dispose();
            _instances.Clear();
        }
    }

    private AesGcm? GetLocked(uint keyVersion)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_keys.TryGetKey(keyVersion, out byte[] key))
            return null;
        if (_instances.TryGetValue(keyVersion, out (byte[] Key, AesGcm Aes) cached) && ReferenceEquals(cached.Key, key))
            return cached.Aes;
        cached.Aes?.Dispose();
        var aes = new AesGcm(key, TagSize);
        _instances[keyVersion] = (key, aes);
        return aes;
    }
}
