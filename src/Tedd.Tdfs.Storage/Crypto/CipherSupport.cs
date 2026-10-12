namespace Tedd.Tdfs.Storage;

/// <summary>Shared encryption helpers for stores that use an <see cref="IRecordCipher"/>.</summary>
internal static class CipherSupport
{
    /// <summary>Upper bound accepted for <see cref="IRecordCipher.Overhead"/>.</summary>
    public const int MaxOverhead = 4096;

    /// <summary>Validates a cipher's overhead and returns it.</summary>
    public static int ValidateOverhead(IRecordCipher cipher)
    {
        int overhead = cipher.Overhead;
        if (overhead is < 0 or > MaxOverhead)
            throw new ArgumentException($"Record cipher overhead {overhead} is outside [0, {MaxOverhead}].", nameof(cipher));
        return overhead;
    }

    /// <summary>Returns the cipher's current key version, rejecting 0 (reserved for "not encrypted").</summary>
    public static uint CurrentVersion(IRecordCipher cipher)
    {
        uint version = cipher.CurrentKeyVersion;
        if (version == 0)
            throw new DfsRecoveryKeyMissingException("The record cipher has no current key version (key ring is empty).");
        return version;
    }

    /// <summary>
    /// Decrypts <paramref name="sealedData"/> into a new array. Missing key material throws
    /// <see cref="DfsRecoveryKeyMissingException"/>; a failed authentication returns null.
    /// </summary>
    public static byte[]? TryDecrypt(IRecordCipher? cipher, ReadOnlySpan<byte> aad, ReadOnlySpan<byte> sealedData, uint keyVersion, string what)
    {
        if (cipher is null)
            throw new DfsRecoveryKeyMissingException($"{what} is encrypted (key version {keyVersion}) but no record cipher is configured.");
        if (!cipher.CanDecrypt(keyVersion))
            throw new DfsRecoveryKeyMissingException($"{what} is encrypted with key version {keyVersion}, which is not available.");
        int overhead = ValidateOverhead(cipher);
        if (sealedData.Length < overhead)
            return null;
        var plain = new byte[sealedData.Length - overhead];
        return cipher.TryDecrypt(aad, sealedData, plain, keyVersion) ? plain : null;
    }

    /// <summary>
    /// Decrypts into <paramref name="plain"/> (exactly sealed length − overhead bytes). Same error contract as
    /// <see cref="TryDecrypt(IRecordCipher?, ReadOnlySpan{byte}, ReadOnlySpan{byte}, uint, string)"/>.
    /// </summary>
    public static bool TryDecryptInto(IRecordCipher? cipher, ReadOnlySpan<byte> aad, ReadOnlySpan<byte> sealedData, Span<byte> plain, uint keyVersion, string what)
    {
        if (cipher is null)
            throw new DfsRecoveryKeyMissingException($"{what} is encrypted (key version {keyVersion}) but no record cipher is configured.");
        if (!cipher.CanDecrypt(keyVersion))
            throw new DfsRecoveryKeyMissingException($"{what} is encrypted with key version {keyVersion}, which is not available.");
        int overhead = ValidateOverhead(cipher);
        if (sealedData.Length - overhead != plain.Length)
            return false;
        return cipher.TryDecrypt(aad, sealedData, plain, keyVersion);
    }
}
