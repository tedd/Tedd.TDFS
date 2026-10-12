namespace Tedd.Tdfs.Storage;

/// <summary>Authenticated encryption of stored records (encryption at rest).</summary>
public interface IRecordCipher
{
    /// <summary>Bytes added by <see cref="Encrypt"/> (output length = plaintext length + overhead).</summary>
    int Overhead { get; }

    /// <summary>Encrypts <paramref name="plain"/> bound to <paramref name="aad"/> into <paramref name="output"/> using key version <paramref name="keyVersion"/>.</summary>
    void Encrypt(ReadOnlySpan<byte> aad, ReadOnlySpan<byte> plain, Span<byte> output, uint keyVersion);

    /// <summary>Decrypts and authenticates; returns false when the data or the associated data was tampered with or the key is unknown.</summary>
    bool TryDecrypt(ReadOnlySpan<byte> aad, ReadOnlySpan<byte> sealedData, Span<byte> plain, uint keyVersion);
}
