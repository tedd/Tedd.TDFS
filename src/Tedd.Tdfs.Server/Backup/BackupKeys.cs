using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Crypto;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// Key material of a backup node (ARCHITECTURE §11, FSD §12.4 "independently recoverable encryption-key path"). The node's
/// identity file holds its node id, a seed for unknown-principal fake salts and its data key wrapped (AES-256-GCM) by a
/// key-encryption key derived from the configured backup secret. The data key encrypts objects and manifests at rest. Without
/// the secret the node still starts and serves its catalog, but refuses to present snapshot contents
/// (<see cref="DfsRecoveryKeyMissingException"/>) until the secret is supplied as recovery key material (RST-005).
/// </summary>
internal sealed class BackupKeys : IDisposable
{
    /// <summary>Name of the identity file inside the data directory.</summary>
    public const string IdentityFile = "backup.identity";

    private const uint Magic = 0x49424454;  // "TDBI"
    private const ushort Version = 1;
    private const int NonceSize = 12;
    private const int TagSize = 16;
    private const int DataKeySize = 32;
    private const string KekPurpose = "backup-kek";
    private const string FakeSaltPurpose = "backup-fake-salt";

    private readonly byte[] _wrappedDataKey;
    private readonly IRandomSource _random;
    private RecordCipher? _objects;
    private RecordCipher? _catalog;

    private BackupKeys(NodeId nodeId, byte[] fakeSaltKey, byte[] wrappedDataKey, IRandomSource random)
    {
        NodeId = nodeId;
        FakeSaltKey = fakeSaltKey;
        _wrappedDataKey = wrappedDataKey;
        _random = random;
    }

    /// <summary>The backup node's stable id (used as the holder of its objects in snapshot views).</summary>
    public NodeId NodeId { get; }

    /// <summary>Key of the deterministic fake salt answered for unknown principals.</summary>
    public byte[] FakeSaltKey { get; }

    /// <summary>True once the data key is available.</summary>
    public bool IsUnlocked => _objects is not null;

    /// <summary>Cipher of objects at rest (null while locked).</summary>
    public RecordCipher? ObjectCipher => _objects;

    /// <summary>Cipher of manifests and staged transfer state at rest (null while locked).</summary>
    public RecordCipher? CatalogCipher => _catalog;

    /// <summary>
    /// Loads the identity file, or creates it (with a new random data key wrapped by the secret's key-encryption key) on an
    /// empty data directory. With a secret the data key is unwrapped; without one the node starts locked.
    /// </summary>
    /// <exception cref="DfsRecoveryKeyMissingException">An empty data directory and no secret, or a secret that does not unwrap the data key.</exception>
    public static async Task<BackupKeys> LoadOrCreateAsync(IStorageMedium medium, string dataDirectory, ReadOnlyMemory<byte> secret, IRandomSource random,
        CancellationToken ct)
    {
        string path = StoragePath.Combine(dataDirectory, IdentityFile);
        byte[]? raw = await AtomicFile.ReadAsync(medium, path, ct);
        BackupKeys keys;
        if (raw is null)
        {
            if (secret.IsEmpty)
                throw new DfsRecoveryKeyMissingException("The backup node has no key material: a backup secret is required to initialize its storage.");
            Span<byte> id = stackalloc byte[16];
            random.Fill(id);
            var nodeId = new NodeId(new Guid(id));
            byte[] fakeSaltKey = new byte[32];
            random.Fill(fakeSaltKey);
            byte[] dataKey = new byte[DataKeySize];
            random.Fill(dataKey);
            byte[] wrapped = Wrap(secret.Span, nodeId, dataKey, random);
            keys = new BackupKeys(nodeId, fakeSaltKey, wrapped, random);
            await AtomicFile.WriteAsync(medium, path, keys.Encode(), ct);
            keys.Install(dataKey);
            CryptographicOperations.ZeroMemory(dataKey);
            return keys;
        }

        keys = Decode(raw, path, random);
        if (!secret.IsEmpty && !keys.TryUnlock(secret.Span))
            throw new DfsRecoveryKeyMissingException("The configured backup secret does not unlock this backup node's data key.");
        return keys;
    }

    /// <summary>Unlocks the data key with recovery key material (the backup secret); false when it does not match.</summary>
    public bool TryUnlock(ReadOnlySpan<byte> secret)
    {
        if (IsUnlocked)
            return true;
        if (secret.IsEmpty)
            return false;
        byte[]? dataKey = Unwrap(secret, NodeId, _wrappedDataKey);
        if (dataKey is null)
            return false;
        Install(dataKey);
        CryptographicOperations.ZeroMemory(dataKey);
        return true;
    }

    /// <summary>Throws <see cref="DfsRecoveryKeyMissingException"/> while locked.</summary>
    public void RequireUnlocked(string what)
    {
        if (!IsUnlocked)
            throw new DfsRecoveryKeyMissingException($"The backup node's key material is not available: {what} needs the recovery key.");
    }

    /// <summary>Encrypts catalog/manifest bytes at rest, bound to <paramref name="context"/>.</summary>
    public byte[] SealAtRest(ReadOnlySpan<byte> plain, string context)
    {
        RecordCipher cipher = _catalog ?? throw new DfsRecoveryKeyMissingException("The backup node's key material is not available.");
        byte[] aad = Encoding.UTF8.GetBytes("tdfs/backup-at-rest/v1\0" + context);
        var output = new byte[4 + plain.Length + cipher.Overhead];
        BinaryPrimitives.WriteUInt32LittleEndian(output, cipher.CurrentKeyVersion);
        cipher.Encrypt(aad, plain, output.AsSpan(4), cipher.CurrentKeyVersion);
        return output;
    }

    /// <summary>Decrypts bytes sealed by <see cref="SealAtRest"/>; tampering or a foreign context fails with <see cref="DfsIntegrityException"/>.</summary>
    public byte[] OpenAtRest(ReadOnlySpan<byte> sealedBytes, string context)
    {
        RecordCipher cipher = _catalog ?? throw new DfsRecoveryKeyMissingException("The backup node's key material is not available.");
        if (sealedBytes.Length < 4 + cipher.Overhead)
            throw new DfsIntegrityException($"Backup catalog record '{context}' is truncated.");
        uint version = BinaryPrimitives.ReadUInt32LittleEndian(sealedBytes);
        byte[] aad = Encoding.UTF8.GetBytes("tdfs/backup-at-rest/v1\0" + context);
        var plain = new byte[sealedBytes.Length - 4 - cipher.Overhead];
        if (!cipher.TryDecrypt(aad, sealedBytes[4..], plain, version))
            throw new DfsIntegrityException($"Backup catalog record '{context}' failed authentication.");
        return plain;
    }

    /// <summary>The key deriving fake salts (derived from the stored seed).</summary>
    public byte[] DeriveFakeSaltKey() => KeyDerivation.DeriveKey(FakeSaltKey, FakeSaltPurpose);

    /// <inheritdoc />
    public void Dispose()
    {
        _objects?.Dispose();
        _catalog?.Dispose();
        _objects = null;
        _catalog = null;
    }

    private void Install(ReadOnlySpan<byte> dataKey)
    {
        _objects = new RecordCipher(KeyRing.Derive(dataKey, StorageKeys.ObjectPurpose), _random.Fork("backup-cipher/objects"));
        _catalog = new RecordCipher(KeyRing.Derive(dataKey, StorageKeys.BackupPurpose), _random.Fork("backup-cipher/catalog"));
    }

    private byte[] Encode()
    {
        var data = new byte[4 + 2 + 16 + 32 + 4 + _wrappedDataKey.Length];
        Span<byte> s = data;
        BinaryPrimitives.WriteUInt32LittleEndian(s, Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(s[4..], Version);
        NodeId.Value.TryWriteBytes(s[6..]);
        FakeSaltKey.CopyTo(s[22..]);
        BinaryPrimitives.WriteInt32LittleEndian(s[54..], _wrappedDataKey.Length);
        _wrappedDataKey.CopyTo(s[58..]);
        return data;
    }

    private static BackupKeys Decode(byte[] data, string path, IRandomSource random)
    {
        if (data.Length < 58 || BinaryPrimitives.ReadUInt32LittleEndian(data) != Magic)
            throw new DfsIntegrityException($"Backup identity file '{path}' is corrupt.");
        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(4));
        if (version != Version)
            throw new DfsProtocolException(DfsErrorCode.Unsupported, $"Backup identity file '{path}' uses format {version}; this build reads {Version}.");
        var nodeId = new NodeId(new Guid(data.AsSpan(6, 16)));
        byte[] fakeSalt = data.AsSpan(22, 32).ToArray();
        int wrappedLength = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(54));
        if (wrappedLength != NonceSize + DataKeySize + TagSize || data.Length != 58 + wrappedLength)
            throw new DfsIntegrityException($"Backup identity file '{path}' is corrupt.");
        return new BackupKeys(nodeId, fakeSalt, data.AsSpan(58, wrappedLength).ToArray(), random);
    }

    private static byte[] Kek(ReadOnlySpan<byte> secret, NodeId nodeId)
    {
        Span<byte> salt = stackalloc byte[16];
        nodeId.Value.TryWriteBytes(salt);
        return KeyDerivation.DeriveKey(secret, KekPurpose, 32, salt);
    }

    private static byte[] Aad(NodeId nodeId) => [.. "tdfs/backup-dek/v1\0"u8, .. nodeId.Value.ToByteArray()];

    private static byte[] Wrap(ReadOnlySpan<byte> secret, NodeId nodeId, ReadOnlySpan<byte> dataKey, IRandomSource random)
    {
        byte[] kek = Kek(secret, nodeId);
        var wrapped = new byte[NonceSize + DataKeySize + TagSize];
        random.Fill(wrapped.AsSpan(0, NonceSize));
        using (var aes = new AesGcm(kek, TagSize))
            aes.Encrypt(wrapped.AsSpan(0, NonceSize), dataKey, wrapped.AsSpan(NonceSize, DataKeySize), wrapped.AsSpan(NonceSize + DataKeySize), Aad(nodeId));
        CryptographicOperations.ZeroMemory(kek);
        return wrapped;
    }

    private static byte[]? Unwrap(ReadOnlySpan<byte> secret, NodeId nodeId, byte[] wrapped)
    {
        byte[] kek = Kek(secret, nodeId);
        var dataKey = new byte[DataKeySize];
        try
        {
            using var aes = new AesGcm(kek, TagSize);
            aes.Decrypt(wrapped.AsSpan(0, NonceSize), wrapped.AsSpan(NonceSize, DataKeySize), wrapped.AsSpan(NonceSize + DataKeySize), dataKey, Aad(nodeId));
            return dataKey;
        }
        catch (AuthenticationTagMismatchException)
        {
            return null;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(kek);
        }
    }
}
