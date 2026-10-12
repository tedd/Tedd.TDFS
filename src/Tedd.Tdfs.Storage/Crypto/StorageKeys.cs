using System.Globalization;
using Tedd.Tdfs.Crypto;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// HKDF-SHA256 derivation of storage keys from a root secret (for example the node storage KEK or a backup secret)
/// and a purpose label. Different purposes and versions always yield independent keys.
/// </summary>
public static class StorageKeys
{
    /// <summary>Purpose label for Raft journal records.</summary>
    public const string JournalPurpose = "storage/journal";

    /// <summary>Purpose label for state-machine checkpoints.</summary>
    public const string CheckpointPurpose = "storage/checkpoint";

    /// <summary>Purpose label for object payloads.</summary>
    public const string ObjectPurpose = "storage/object";

    /// <summary>Purpose label for backup manifests.</summary>
    public const string BackupPurpose = "storage/backup";

    /// <summary>
    /// Derives a 32-byte key for <paramref name="purpose"/> and key <paramref name="version"/> from
    /// <paramref name="secret"/> (HKDF-SHA256, info = <c>tdfs/v1/{purpose}/key-v{version}</c>).
    /// </summary>
    /// <exception cref="ArgumentException">The secret is empty or the purpose is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="version"/> is 0.</exception>
    public static byte[] DeriveKey(ReadOnlySpan<byte> secret, string purpose, uint version = 1, ReadOnlySpan<byte> salt = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(purpose);
        ArgumentOutOfRangeException.ThrowIfZero(version);
        string label = string.Create(CultureInfo.InvariantCulture, $"{purpose}/key-v{version}");
        return KeyDerivation.DeriveKey(secret, label, KeyRing.KeySize, salt);
    }
}
