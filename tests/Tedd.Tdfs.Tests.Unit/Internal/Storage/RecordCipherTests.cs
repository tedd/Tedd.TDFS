using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

public sealed class RecordCipherTests
{
    private static readonly byte[] Secret = TestData.Bytes(77, 32);

    private static RecordCipher NewCipher(uint versions = 1, ulong seed = 5)
        => new(KeyRing.Derive(Secret, StorageKeys.ObjectPurpose, versions), new SeededRandom(seed));

    [Fact]
    public void EncryptDecrypt_RoundTrips_WithOverheadOfNonceAndTag()
    {
        using RecordCipher cipher = NewCipher();
        byte[] plain = TestData.Bytes(1, 1000);
        byte[] aad = [1, 2, 3];
        var sealedData = new byte[plain.Length + cipher.Overhead];
        cipher.Encrypt(aad, plain, sealedData, 1);
        Assert.Equal(28, cipher.Overhead);
        Assert.False(sealedData.AsSpan(RecordCipher.NonceSize, plain.Length).SequenceEqual(plain));
        var decrypted = new byte[plain.Length];
        Assert.True(cipher.TryDecrypt(aad, sealedData, decrypted, 1));
        Assert.Equal(plain, decrypted);
    }

    [Fact]
    public void EmptyPlaintext_RoundTrips()
    {
        using RecordCipher cipher = NewCipher();
        var sealedData = new byte[cipher.Overhead];
        cipher.Encrypt([], [], sealedData, 1);
        Assert.True(cipher.TryDecrypt([], sealedData, [], 1));
    }

    [Fact]
    public void TamperedCiphertextTagNonceOrAad_FailsAuthentication()
    {
        using RecordCipher cipher = NewCipher();
        byte[] plain = TestData.Bytes(2, 64);
        byte[] aad = [9, 9];
        var sealedData = new byte[plain.Length + cipher.Overhead];
        cipher.Encrypt(aad, plain, sealedData, 1);
        var output = new byte[plain.Length];
        foreach (int offset in new[] { 0, 5, RecordCipher.NonceSize, RecordCipher.NonceSize + 10, sealedData.Length - 1 })
        {
            byte[] copy = (byte[])sealedData.Clone();
            copy[offset] ^= 0x01;
            Assert.False(cipher.TryDecrypt(aad, copy, output, 1));
        }

        Assert.False(cipher.TryDecrypt([9, 8], sealedData, output, 1));
        Assert.False(cipher.TryDecrypt(aad, sealedData.AsSpan(0, 10), output, 1));
    }

    [Fact]
    public void NoncesAreFresh_SoEqualPlaintextsDiffer()
    {
        using RecordCipher cipher = NewCipher();
        byte[] plain = TestData.Bytes(3, 32);
        var a = new byte[plain.Length + cipher.Overhead];
        var b = new byte[plain.Length + cipher.Overhead];
        cipher.Encrypt([], plain, a, 1);
        cipher.Encrypt([], plain, b, 1);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void KeyRotation_UsesNewestVersion_AndKeepsOldReadable()
    {
        var ring = new KeyRing(1, StorageKeys.DeriveKey(Secret, StorageKeys.JournalPurpose, 1));
        using var cipher = new RecordCipher(ring, new SeededRandom(1));
        byte[] plain = TestData.Bytes(4, 40);
        var v1 = new byte[plain.Length + cipher.Overhead];
        cipher.Encrypt([], plain, v1, cipher.CurrentKeyVersion);
        Assert.Equal(1u, cipher.CurrentKeyVersion);

        ring.Add(2, StorageKeys.DeriveKey(Secret, StorageKeys.JournalPurpose, 2));
        Assert.Equal(2u, cipher.CurrentKeyVersion);
        var output = new byte[plain.Length];
        Assert.True(cipher.TryDecrypt([], v1, output, 1));
        Assert.False(cipher.TryDecrypt([], v1, output, 2));
        Assert.True(cipher.CanDecrypt(1));
        Assert.False(cipher.CanDecrypt(3));
        Assert.False(cipher.TryDecrypt([], v1, output, 3));

        Assert.True(ring.Remove(1));
        Assert.False(cipher.CanDecrypt(1));
        Assert.False(cipher.TryDecrypt([], v1, output, 1));
        Assert.Throws<InvalidOperationException>(() => ring.Remove(2));
    }

    [Fact]
    public void EncryptWithMissingKey_ThrowsRecoveryKeyMissing()
    {
        using var cipher = new RecordCipher(new KeyRing(), new SeededRandom(1));
        Assert.Throws<DfsRecoveryKeyMissingException>(() => cipher.Encrypt([], [1], new byte[1 + cipher.Overhead], 1));
    }

    [Fact]
    public void WrongOutputLength_Throws()
    {
        using RecordCipher cipher = NewCipher();
        Assert.Throws<ArgumentException>(() => cipher.Encrypt([], [1, 2], new byte[2], 1));
    }

    [Fact]
    public void KeyRing_ValidatesKeysAndVersions()
    {
        var ring = new KeyRing();
        Assert.True(ring.IsEmpty);
        Assert.Equal(0u, ring.CurrentVersion);
        Assert.Throws<ArgumentException>(() => ring.Add(1, new byte[16]));
        Assert.Throws<ArgumentOutOfRangeException>(() => ring.Add(0, new byte[32]));
        byte[] key = TestData.Bytes(5, 32);
        ring.Add(3, key);
        ring.Add(3, key); // identical re-add is a no-op
        Assert.Throws<ArgumentException>(() => ring.Add(3, TestData.Bytes(6, 32)));
        ring.Add(1, TestData.Bytes(7, 32));
        Assert.Equal(3u, ring.CurrentVersion);
        Assert.Equal([1u, 3u], ring.Versions);
        ring.SetCurrent(1);
        Assert.Equal(1u, ring.CurrentVersion);
        Assert.Throws<KeyNotFoundException>(() => ring.SetCurrent(9));
    }

    [Fact]
    public void Hkdf_IsDeterministicAndDomainSeparated()
    {
        byte[] a = StorageKeys.DeriveKey(Secret, StorageKeys.JournalPurpose, 1);
        Assert.Equal(32, a.Length);
        Assert.Equal(a, StorageKeys.DeriveKey(Secret, StorageKeys.JournalPurpose, 1));
        Assert.NotEqual(a, StorageKeys.DeriveKey(Secret, StorageKeys.JournalPurpose, 2));
        Assert.NotEqual(a, StorageKeys.DeriveKey(Secret, StorageKeys.CheckpointPurpose, 1));
        Assert.NotEqual(a, StorageKeys.DeriveKey(TestData.Bytes(78, 32), StorageKeys.JournalPurpose, 1));
        Assert.Throws<ArgumentException>(() => StorageKeys.DeriveKey([], "x"));
        Assert.Throws<ArgumentException>(() => StorageKeys.DeriveKey(Secret, ""));
        Assert.Throws<ArgumentOutOfRangeException>(() => StorageKeys.DeriveKey(Secret, "x", 0));
    }

    [Fact]
    public void Derive_BuildsAllVersions()
    {
        KeyRing ring = KeyRing.Derive(Secret, "p", 3);
        Assert.Equal([1u, 2u, 3u], ring.Versions);
        Assert.Equal(3u, ring.CurrentVersion);
    }
}
