using System.Security.Cryptography;
using System.Text;

namespace Tedd.Tdfs.Crypto;

/// <summary>HKDF-SHA256 key derivation from a root secret (cluster secret, backup secret).</summary>
internal static class KeyDerivation
{
    /// <summary>Purpose label for node-to-node authentication keys.</summary>
    public const string NodeAuthPurpose = "node-auth";

    /// <summary>Purpose label for object receipt MAC keys.</summary>
    public const string ReceiptPurpose = "receipt";

    /// <summary>Purpose label for staging/read capability MAC keys.</summary>
    public const string CapabilityPurpose = "capability";

    /// <summary>Purpose label for the storage key-encryption key.</summary>
    public const string StorageKekPurpose = "storage-kek";

    /// <summary>Derives <paramref name="length"/> bytes for <paramref name="purpose"/> from <paramref name="secret"/>.</summary>
    /// <param name="secret">Input keying material.</param>
    /// <param name="purpose">Domain-separation label (encoded into the HKDF info as <c>tdfs/v1/{purpose}</c>).</param>
    /// <param name="length">Output length in bytes (default 32).</param>
    /// <param name="salt">Optional salt (for example the cluster id bytes).</param>
    public static byte[] DeriveKey(ReadOnlySpan<byte> secret, string purpose, int length = 32, ReadOnlySpan<byte> salt = default)
    {
        ArgumentNullException.ThrowIfNull(purpose);
        ArgumentOutOfRangeException.ThrowIfLessThan(length, 1);
        if (secret.IsEmpty)
            throw new ArgumentException("The secret must not be empty.", nameof(secret));
        byte[] info = Encoding.UTF8.GetBytes("tdfs/v1/" + purpose);
        var output = new byte[length];
        HKDF.DeriveKey(HashAlgorithmName.SHA256, secret, output, salt, info);
        return output;
    }
}
