using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Stored verifier of a principal's secret (SCRAM-SHA-256 style, RFC 5802/7677 key schedule): the server never stores
/// the secret or the PBKDF2 output, only <see cref="StoredKey"/> (to check client proofs) and <see cref="ServerKey"/>
/// (to prove its own knowledge of the verifier).
/// </summary>
/// <param name="Salt">PBKDF2 salt.</param>
/// <param name="Iterations">PBKDF2-SHA256 iteration count.</param>
/// <param name="StoredKey">SHA-256(ClientKey).</param>
/// <param name="ServerKey">HMAC(SaltedSecret, "Server Key").</param>
internal sealed record PrincipalVerifier(byte[] Salt, int Iterations, byte[] StoredKey, byte[] ServerKey);

/// <summary>Client-side keys derived from a principal's secret, salt and iteration count.</summary>
/// <param name="ClientKey">HMAC(SaltedSecret, "Client Key").</param>
/// <param name="StoredKey">SHA-256(ClientKey).</param>
/// <param name="ServerKey">HMAC(SaltedSecret, "Server Key").</param>
internal readonly record struct ClientKeys(byte[] ClientKey, byte[] StoredKey, byte[] ServerKey);

/// <summary>
/// Handshake authentication computations shared by the SDK and the server (PROTOCOL.md §3). The transcript binds both
/// frames exactly as they were sent (both nonces, both identities, the protocol range and features), so a proof can
/// neither be replayed on another connection nor reflected to the other side.
/// </summary>
internal static class HandshakeAuth
{
    /// <summary>Default PBKDF2 iteration count for new principal verifiers.</summary>
    public const int DefaultIterations = 100_000;

    /// <summary>Salt size of new principal verifiers.</summary>
    public const int SaltSize = 16;

    /// <summary>Upper bound on the iteration count a client accepts from a challenge (protects clients from CPU exhaustion).</summary>
    public const int MaxIterations = 10_000_000;

    private static ReadOnlySpan<byte> TranscriptLabel => "tdfs/hello/v1\0"u8;
    private static ReadOnlySpan<byte> NodeProofLabel => "tdfs/node-proof/v1\0"u8;
    private static ReadOnlySpan<byte> NodeServerLabel => "tdfs/node-server/v1\0"u8;
    private static ReadOnlySpan<byte> FakeSaltLabel => "tdfs/fake-salt/v1\0"u8;

    /// <summary>
    /// Computes the 32-byte transcript hash: SHA-256 of <c>"tdfs/hello/v1\0" || u32le(len(hello)) || hello ||
    /// u32le(len(challenge)) || challenge</c>, where both frames are the complete encoded frames (type header included)
    /// exactly as transmitted.
    /// </summary>
    public static byte[] ComputeTranscript(ReadOnlySpan<byte> helloFrame, ReadOnlySpan<byte> challengeFrame)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> length = stackalloc byte[4];
        hash.AppendData(TranscriptLabel);
        BinaryPrimitives.WriteUInt32LittleEndian(length, (uint)helloFrame.Length);
        hash.AppendData(length);
        hash.AppendData(helloFrame);
        BinaryPrimitives.WriteUInt32LittleEndian(length, (uint)challengeFrame.Length);
        hash.AppendData(length);
        hash.AppendData(challengeFrame);
        return hash.GetHashAndReset();
    }

    // ------------------------------------------------------------------ node peers

    /// <summary>Node proof (sent in <c>HelloProof</c>): HMAC-SHA256(NodeAuthKey, "tdfs/node-proof/v1\0" || transcript).</summary>
    public static byte[] ComputeNodeProof(ReadOnlySpan<byte> nodeAuthKey, ReadOnlySpan<byte> transcript)
        => LabelledMac(nodeAuthKey, NodeProofLabel, transcript);

    /// <summary>Verifies a node proof in constant time.</summary>
    public static bool VerifyNodeProof(ReadOnlySpan<byte> nodeAuthKey, ReadOnlySpan<byte> transcript, ReadOnlySpan<byte> proof)
        => proof.Length == ProtocolInfo.MacSize && CryptographicOperations.FixedTimeEquals(ComputeNodeProof(nodeAuthKey, transcript), proof);

    /// <summary>Server MAC towards a node peer (sent in <c>HelloResult</c>): HMAC-SHA256(NodeAuthKey, "tdfs/node-server/v1\0" || transcript).</summary>
    public static byte[] ComputeNodeServerMac(ReadOnlySpan<byte> nodeAuthKey, ReadOnlySpan<byte> transcript)
        => LabelledMac(nodeAuthKey, NodeServerLabel, transcript);

    /// <summary>Verifies the server MAC towards a node peer in constant time.</summary>
    public static bool VerifyNodeServerMac(ReadOnlySpan<byte> nodeAuthKey, ReadOnlySpan<byte> transcript, ReadOnlySpan<byte> mac)
        => mac.Length == ProtocolInfo.MacSize && CryptographicOperations.FixedTimeEquals(ComputeNodeServerMac(nodeAuthKey, transcript), mac);

    // ------------------------------------------------------------------ principals (clients, exporters)

    /// <summary>Derives the client keys: SaltedSecret = PBKDF2-SHA256(UTF-8(secret), salt, iterations, 32).</summary>
    /// <exception cref="ArgumentOutOfRangeException">The iteration count is outside 1..<see cref="MaxIterations"/>.</exception>
    public static ClientKeys DeriveClientKeys(string secret, ReadOnlySpan<byte> salt, int iterations)
    {
        ArgumentNullException.ThrowIfNull(secret);
        ArgumentOutOfRangeException.ThrowIfLessThan(iterations, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(iterations, MaxIterations);
        byte[] secretBytes = Encoding.UTF8.GetBytes(secret);
        byte[] salted = Rfc2898DeriveBytes.Pbkdf2(secretBytes, salt, iterations, HashAlgorithmName.SHA256, 32);
        try
        {
            byte[] clientKey = HMACSHA256.HashData(salted, "Client Key"u8);
            byte[] storedKey = SHA256.HashData(clientKey);
            byte[] serverKey = HMACSHA256.HashData(salted, "Server Key"u8);
            return new ClientKeys(clientKey, storedKey, serverKey);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(salted);
            CryptographicOperations.ZeroMemory(secretBytes);
        }
    }

    /// <summary>Creates the stored verifier for a principal secret.</summary>
    public static PrincipalVerifier CreateVerifier(string secret, ReadOnlySpan<byte> salt, int iterations)
    {
        ClientKeys keys = DeriveClientKeys(secret, salt, iterations);
        CryptographicOperations.ZeroMemory(keys.ClientKey);
        return new PrincipalVerifier(salt.ToArray(), iterations, keys.StoredKey, keys.ServerKey);
    }

    /// <summary>Client proof: ClientKey XOR HMAC-SHA256(StoredKey, transcript).</summary>
    public static byte[] ComputeClientProof(ClientKeys keys, ReadOnlySpan<byte> transcript)
    {
        byte[] signature = HMACSHA256.HashData(keys.StoredKey, transcript);
        for (int i = 0; i < signature.Length; i++)
            signature[i] ^= keys.ClientKey[i];
        return signature;
    }

    /// <summary>Server-side verification of a client proof: SHA-256(proof XOR HMAC(StoredKey, transcript)) == StoredKey.</summary>
    public static bool VerifyClientProof(PrincipalVerifier verifier, ReadOnlySpan<byte> transcript, ReadOnlySpan<byte> proof)
    {
        ArgumentNullException.ThrowIfNull(verifier);
        if (proof.Length != ProtocolInfo.MacSize)
            return false;
        byte[] candidate = HMACSHA256.HashData(verifier.StoredKey, transcript);
        for (int i = 0; i < candidate.Length; i++)
            candidate[i] ^= proof[i];
        byte[] candidateStored = SHA256.HashData(candidate);
        CryptographicOperations.ZeroMemory(candidate);
        return CryptographicOperations.FixedTimeEquals(candidateStored, verifier.StoredKey);
    }

    /// <summary>Server MAC towards a principal (sent in <c>HelloResult</c>): HMAC-SHA256(ServerKey, transcript).</summary>
    public static byte[] ComputeServerMac(PrincipalVerifier verifier, ReadOnlySpan<byte> transcript)
    {
        ArgumentNullException.ThrowIfNull(verifier);
        return HMACSHA256.HashData(verifier.ServerKey, transcript);
    }

    /// <summary>Client-side verification of the server MAC in constant time.</summary>
    public static bool VerifyServerMac(ClientKeys keys, ReadOnlySpan<byte> transcript, ReadOnlySpan<byte> mac)
        => mac.Length == ProtocolInfo.MacSize && CryptographicOperations.FixedTimeEquals(HMACSHA256.HashData(keys.ServerKey, transcript), mac);

    /// <summary>
    /// Deterministic salt returned for an unknown principal so that the challenge does not reveal whether a principal
    /// exists: the first <see cref="SaltSize"/> bytes of HMAC-SHA256(serverSecretKey, "tdfs/fake-salt/v1\0" || UTF-8(principal)).
    /// </summary>
    public static byte[] FakeSalt(ReadOnlySpan<byte> serverSecretKey, string principal)
    {
        ArgumentNullException.ThrowIfNull(principal);
        byte[] mac = LabelledMac(serverSecretKey, FakeSaltLabel, Encoding.UTF8.GetBytes(principal));
        return mac.AsSpan(0, SaltSize).ToArray();
    }

    private static byte[] LabelledMac(ReadOnlySpan<byte> key, ReadOnlySpan<byte> label, ReadOnlySpan<byte> data)
    {
        using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, key);
        hmac.AppendData(label);
        hmac.AppendData(data);
        return hmac.GetHashAndReset();
    }
}
