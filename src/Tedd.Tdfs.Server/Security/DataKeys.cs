using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Crypto;

namespace Tedd.Tdfs.Server.Security;

/// <summary>
/// Key material handling (ARCHITECTURE §11): per-volume data-encryption keys wrapped by the cluster KEK for the control
/// state, and the KEK recovery escrow wrapped under an administrator passphrase. AES-256-GCM throughout; nonces come from
/// the node's random source (the wrapped bytes travel inside log entries, so replicas never re-wrap).
/// </summary>
internal static class DataKeys
{
    /// <summary>Size of data-encryption keys.</summary>
    public const int KeySize = 32;

    private const int NonceSize = 12;
    private const int TagSize = 16;
    private const uint EscrowMagic = 0x454B5254; // "TRKE"
    private const byte EscrowVersion = 1;
    private const int EscrowSaltSize = 16;

    /// <summary>PBKDF2-SHA256 iterations protecting the recovery escrow.</summary>
    public const int EscrowIterations = 210_000;

    /// <summary>Generates a fresh data-encryption key.</summary>
    public static byte[] NewDataKey(IRandomSource random) => random.NextBytes(KeySize);

    /// <summary>Wraps a data key for <paramref name="volume"/> and <paramref name="version"/>: <c>nonce | ciphertext | tag</c>.</summary>
    public static byte[] Wrap(byte[] kek, VolumeId volume, uint version, ReadOnlySpan<byte> dataKey, IRandomSource random)
    {
        byte[] wrappingKey = KeyDerivation.DeriveKey(kek, "dek-wrap");
        byte[] output = new byte[NonceSize + dataKey.Length + TagSize];
        Span<byte> nonce = output.AsSpan(0, NonceSize);
        random.Fill(nonce);
        using var aes = new AesGcm(wrappingKey, TagSize);
        aes.Encrypt(nonce, dataKey, output.AsSpan(NonceSize, dataKey.Length), output.AsSpan(NonceSize + dataKey.Length), Aad(volume, version));
        CryptographicOperations.ZeroMemory(wrappingKey);
        return output;
    }

    /// <summary>Unwraps a data key; tampering or a wrong KEK throws <see cref="DfsRecoveryKeyMissingException"/>.</summary>
    public static byte[] Unwrap(byte[] kek, VolumeId volume, uint version, ReadOnlySpan<byte> wrapped)
    {
        if (wrapped.Length < NonceSize + TagSize)
            throw new DfsIntegrityException("The wrapped data key is truncated.");
        byte[] wrappingKey = KeyDerivation.DeriveKey(kek, "dek-wrap");
        int length = wrapped.Length - NonceSize - TagSize;
        byte[] key = new byte[length];
        try
        {
            using var aes = new AesGcm(wrappingKey, TagSize);
            aes.Decrypt(wrapped[..NonceSize], wrapped.Slice(NonceSize, length), wrapped[(NonceSize + length)..], key, Aad(volume, version));
            return key;
        }
        catch (AuthenticationTagMismatchException ex)
        {
            throw new DfsRecoveryKeyMissingException($"Data key version {version} of volume {volume} cannot be unwrapped with the available key material.", ex);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(wrappingKey);
        }
    }

    /// <summary>
    /// The KEK escrow wrapped under <paramref name="passphrase"/>:
    /// <c>u32 magic | u8 version | u32 iterations | salt(16) | nonce(12) | ciphertext | tag(16)</c>.
    /// </summary>
    public static byte[] ExportEscrow(byte[] kek, string passphrase, IRandomSource random)
    {
        ArgumentException.ThrowIfNullOrEmpty(passphrase);
        byte[] salt = random.NextBytes(EscrowSaltSize);
        byte[] key = Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(passphrase), salt, EscrowIterations, HashAlgorithmName.SHA256, 32);
        int header = 4 + 1 + 4 + EscrowSaltSize;
        byte[] output = new byte[header + NonceSize + kek.Length + TagSize];
        BinaryPrimitives.WriteUInt32LittleEndian(output, EscrowMagic);
        output[4] = EscrowVersion;
        BinaryPrimitives.WriteInt32LittleEndian(output.AsSpan(5), EscrowIterations);
        salt.CopyTo(output.AsSpan(9));
        Span<byte> nonce = output.AsSpan(header, NonceSize);
        random.Fill(nonce);
        using (var aes = new AesGcm(key, TagSize))
            aes.Encrypt(nonce, kek, output.AsSpan(header + NonceSize, kek.Length), output.AsSpan(header + NonceSize + kek.Length), output.AsSpan(0, header));
        CryptographicOperations.ZeroMemory(key);
        return output;
    }

    /// <summary>Recovers the KEK from an escrow; a wrong passphrase or tampering throws <see cref="DfsRecoveryKeyMissingException"/>.</summary>
    public static byte[] ImportEscrow(ReadOnlySpan<byte> escrow, string passphrase)
    {
        int header = 4 + 1 + 4 + EscrowSaltSize;
        if (escrow.Length < header + NonceSize + TagSize || BinaryPrimitives.ReadUInt32LittleEndian(escrow) != EscrowMagic || escrow[4] != EscrowVersion)
            throw new DfsRecoveryKeyMissingException("The recovery key escrow is malformed.");
        int iterations = BinaryPrimitives.ReadInt32LittleEndian(escrow[5..]);
        if (iterations < 1 || iterations > Protocol.HandshakeAuth.MaxIterations)
            throw new DfsRecoveryKeyMissingException("The recovery key escrow is malformed."); // bounded PBKDF2 work for hostile input
        byte[] key = Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(passphrase), escrow.Slice(9, EscrowSaltSize).ToArray(), iterations, HashAlgorithmName.SHA256, 32);
        int length = escrow.Length - header - NonceSize - TagSize;
        byte[] kek = new byte[length];
        try
        {
            using var aes = new AesGcm(key, TagSize);
            aes.Decrypt(escrow.Slice(header, NonceSize), escrow.Slice(header + NonceSize, length), escrow[(header + NonceSize + length)..], kek, escrow[..header]);
            return kek;
        }
        catch (AuthenticationTagMismatchException ex)
        {
            throw new DfsRecoveryKeyMissingException("The recovery key escrow cannot be opened with this passphrase.", ex);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    private static byte[] Aad(VolumeId volume, uint version)
    {
        byte[] aad = new byte[12 + 16 + 4];
        "tdfs/dek/v1\0"u8.CopyTo(aad);
        volume.Value.TryWriteBytes(aad.AsSpan(12));
        BinaryPrimitives.WriteUInt32LittleEndian(aad.AsSpan(28), version);
        return aad;
    }
}
