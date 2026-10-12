using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Crypto;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Security;

/// <summary>
/// Keys derived from the shared cluster secret with HKDF-SHA256 (ARCHITECTURE §11, <see cref="KeyDerivation"/>):
/// node authentication, receipt MACs, data capabilities and the storage key-encryption key, plus the derived server-side
/// keys used for deterministic principal salts and the unknown-principal fake salt (PROTOCOL.md §3).
/// </summary>
/// <remarks>Never logged or exposed; instances live only in node memory.</remarks>
internal sealed class KeyHierarchy
{
    private const string PrincipalSaltPurpose = "principal-salt";
    private const string FakeSaltPurpose = "fake-salt";

    /// <summary>Derives every key from <paramref name="clusterSecret"/>.</summary>
    /// <exception cref="ArgumentException">The secret is empty.</exception>
    public KeyHierarchy(ReadOnlySpan<byte> clusterSecret)
    {
        if (clusterSecret.IsEmpty)
            throw new ArgumentException("The cluster secret must not be empty.", nameof(clusterSecret));
        NodeAuthKey = KeyDerivation.DeriveKey(clusterSecret, KeyDerivation.NodeAuthPurpose);
        ReceiptKey = KeyDerivation.DeriveKey(clusterSecret, KeyDerivation.ReceiptPurpose);
        CapabilityKey = KeyDerivation.DeriveKey(clusterSecret, KeyDerivation.CapabilityPurpose);
        StorageKek = KeyDerivation.DeriveKey(clusterSecret, KeyDerivation.StorageKekPurpose);
        PrincipalSaltKey = KeyDerivation.DeriveKey(clusterSecret, PrincipalSaltPurpose);
        FakeSaltKey = KeyDerivation.DeriveKey(clusterSecret, FakeSaltPurpose);
    }

    /// <summary>Key proving cluster membership in node handshakes (HKDF info <c>tdfs/v1/node-auth</c>).</summary>
    public byte[] NodeAuthKey { get; }

    /// <summary>Key of object receipt MACs (<c>tdfs/v1/receipt</c>).</summary>
    public byte[] ReceiptKey { get; }

    /// <summary>Key of data capability MACs (<c>tdfs/v1/capability</c>).</summary>
    public byte[] CapabilityKey { get; }

    /// <summary>Storage key-encryption key (<c>tdfs/v1/storage-kek</c>): root of at-rest keys and DEK wrapping.</summary>
    public byte[] StorageKek { get; }

    /// <summary>Key deriving deterministic salts of bootstrap principals (identical on every initial member).</summary>
    public byte[] PrincipalSaltKey { get; }

    /// <summary>Key of the deterministic fake salt answered for unknown principals.</summary>
    public byte[] FakeSaltKey { get; }

    /// <summary>Record cipher for Raft journals at rest (derived from <see cref="StorageKek"/>).</summary>
    public RecordCipher CreateJournalCipher(IRandomSource random) => new RecordCipher(KeyRing.Derive(StorageKek, StorageKeys.JournalPurpose), random);

    /// <summary>Record cipher for state-machine checkpoints at rest.</summary>
    public RecordCipher CreateCheckpointCipher(IRandomSource random) => new RecordCipher(KeyRing.Derive(StorageKek, StorageKeys.CheckpointPurpose), random);

    /// <summary>Record cipher for object payloads at rest (node level; per-volume DEKs are tracked in the control state).</summary>
    public RecordCipher CreateObjectCipher(IRandomSource random) => new RecordCipher(KeyRing.Derive(StorageKek, StorageKeys.ObjectPurpose), random);

    /// <summary>
    /// Deterministic salt for a principal created at bootstrap: every initial member derives the same verifier from the
    /// same description without coordination.
    /// </summary>
    public byte[] BootstrapSalt(string principal)
    {
        ArgumentNullException.ThrowIfNull(principal);
        byte[] mac = HMACSHA256.HashData(PrincipalSaltKey, Encoding.UTF8.GetBytes("tdfs/bootstrap-salt/v1\0" + principal));
        return mac.AsSpan(0, HandshakeAuth.SaltSize).ToArray();
    }

    /// <summary>The fake salt answered for an unknown principal (existence is not disclosed).</summary>
    public byte[] FakeSalt(string principal) => HandshakeAuth.FakeSalt(FakeSaltKey, principal);
}
