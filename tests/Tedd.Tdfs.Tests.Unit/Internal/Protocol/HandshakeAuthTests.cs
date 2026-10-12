using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>Handshake authentication (SEC-001/002): transcript binding, principal proofs, node proofs, mutual MACs.</summary>
public sealed class HandshakeAuthTests
{
    private const int Iterations = 1000; // keep tests fast; production uses HandshakeAuth.DefaultIterations
    private static readonly byte[] Salt = Enumerable.Range(0, HandshakeAuth.SaltSize).Select(i => (byte)(i * 3)).ToArray();

    private static (byte[] Hello, byte[] Challenge) Frames(byte clientNonce = 1, byte serverNonce = 2, string principal = "alice")
    {
        var hello = new HelloMessage(Guid.Empty, 1, 1, ImmutableSortedSet.Create(StringComparer.Ordinal, "f1"), PeerRole.Client, "c0", null, 0,
            principal, Enumerable.Repeat(clientNonce, 32).ToArray());
        var challenge = new HelloChallengeMessage(Guid.Parse("00000000-0000-0000-0000-000000000001"), "cluster", 1,
            ImmutableSortedSet.Create(StringComparer.Ordinal, "f1"), "n0", PeerRole.Node, new NodeId(Guid.Parse("00000000-0000-0000-0000-0000000000aa")), 4,
            Enumerable.Repeat(serverNonce, 32).ToArray(), Salt, Iterations);
        return (MessageCodec.Encode(hello), MessageCodec.Encode(challenge));
    }

    [Fact]
    public void ClientProofVerifiesAgainstStoredVerifierAndServerMacVerifiesBack()
    {
        PrincipalVerifier verifier = HandshakeAuth.CreateVerifier("s3cret", Salt, Iterations);
        (byte[] hello, byte[] challenge) = Frames();
        byte[] transcript = HandshakeAuth.ComputeTranscript(hello, challenge);
        Assert.Equal(32, transcript.Length);

        ClientKeys keys = HandshakeAuth.DeriveClientKeys("s3cret", Salt, Iterations);
        byte[] proof = HandshakeAuth.ComputeClientProof(keys, transcript);
        Assert.Equal(ProtocolInfo.MacSize, proof.Length);
        Assert.True(HandshakeAuth.VerifyClientProof(verifier, transcript, proof));

        byte[] serverMac = HandshakeAuth.ComputeServerMac(verifier, transcript);
        Assert.True(HandshakeAuth.VerifyServerMac(keys, transcript, serverMac));
    }

    [Fact]
    public void WrongSecretIsRejected()
    {
        PrincipalVerifier verifier = HandshakeAuth.CreateVerifier("s3cret", Salt, Iterations);
        (byte[] hello, byte[] challenge) = Frames();
        byte[] transcript = HandshakeAuth.ComputeTranscript(hello, challenge);
        ClientKeys wrong = HandshakeAuth.DeriveClientKeys("guess", Salt, Iterations);
        Assert.False(HandshakeAuth.VerifyClientProof(verifier, transcript, HandshakeAuth.ComputeClientProof(wrong, transcript)));
        // A server that does not know the verifier cannot produce an acceptable MAC either (mutual authentication).
        PrincipalVerifier impostor = HandshakeAuth.CreateVerifier("other", Salt, Iterations);
        ClientKeys keys = HandshakeAuth.DeriveClientKeys("s3cret", Salt, Iterations);
        Assert.False(HandshakeAuth.VerifyServerMac(keys, transcript, HandshakeAuth.ComputeServerMac(impostor, transcript)));
    }

    [Fact]
    public void ProofIsBoundToTheTranscript()
    {
        PrincipalVerifier verifier = HandshakeAuth.CreateVerifier("s3cret", Salt, Iterations);
        ClientKeys keys = HandshakeAuth.DeriveClientKeys("s3cret", Salt, Iterations);
        (byte[] hello, byte[] challenge) = Frames();
        byte[] proof = HandshakeAuth.ComputeClientProof(keys, HandshakeAuth.ComputeTranscript(hello, challenge));

        // Replay on a connection with a different server nonce, a different client nonce or another principal fails.
        (byte[] h2, byte[] c2) = Frames(serverNonce: 9);
        Assert.False(HandshakeAuth.VerifyClientProof(verifier, HandshakeAuth.ComputeTranscript(h2, c2), proof));
        (byte[] h3, byte[] c3) = Frames(clientNonce: 9);
        Assert.False(HandshakeAuth.VerifyClientProof(verifier, HandshakeAuth.ComputeTranscript(h3, c3), proof));
        (byte[] h4, byte[] c4) = Frames(principal: "bob");
        Assert.False(HandshakeAuth.VerifyClientProof(verifier, HandshakeAuth.ComputeTranscript(h4, c4), proof));
        // Frame boundaries are length-prefixed: moving bytes between the frames changes the transcript.
        byte[] shifted = HandshakeAuth.ComputeTranscript([.. hello, challenge[0]], challenge[1..]);
        Assert.NotEqual(HandshakeAuth.ComputeTranscript(hello, challenge), shifted);
        Assert.False(HandshakeAuth.VerifyClientProof(verifier, HandshakeAuth.ComputeTranscript(hello, challenge), new byte[32]));
        Assert.False(HandshakeAuth.VerifyClientProof(verifier, HandshakeAuth.ComputeTranscript(hello, challenge), proof[..31]));
    }

    [Fact]
    public void VerifierDoesNotContainTheSecretOrClientKey()
    {
        PrincipalVerifier verifier = HandshakeAuth.CreateVerifier("s3cret", Salt, Iterations);
        ClientKeys keys = HandshakeAuth.DeriveClientKeys("s3cret", Salt, Iterations);
        Assert.Equal(keys.StoredKey, verifier.StoredKey);
        Assert.Equal(keys.ServerKey, verifier.ServerKey);
        Assert.NotEqual(keys.ClientKey, verifier.StoredKey);
        Assert.NotEqual(keys.ClientKey, verifier.ServerKey);
    }

    [Fact]
    public void NodeProofsRequireTheClusterKeyAndAreDirectional()
    {
        byte[] nodeKey = Enumerable.Repeat((byte)7, 32).ToArray();
        byte[] otherKey = Enumerable.Repeat((byte)8, 32).ToArray();
        (byte[] hello, byte[] challenge) = Frames();
        byte[] transcript = HandshakeAuth.ComputeTranscript(hello, challenge);

        byte[] proof = HandshakeAuth.ComputeNodeProof(nodeKey, transcript);
        Assert.True(HandshakeAuth.VerifyNodeProof(nodeKey, transcript, proof));
        Assert.False(HandshakeAuth.VerifyNodeProof(otherKey, transcript, proof));

        byte[] serverMac = HandshakeAuth.ComputeNodeServerMac(nodeKey, transcript);
        Assert.True(HandshakeAuth.VerifyNodeServerMac(nodeKey, transcript, serverMac));
        // A reflected proof is not a valid server MAC and vice versa.
        Assert.False(HandshakeAuth.VerifyNodeServerMac(nodeKey, transcript, proof));
        Assert.False(HandshakeAuth.VerifyNodeProof(nodeKey, transcript, serverMac));
    }

    [Fact]
    public void FakeSaltIsDeterministicPerPrincipal()
    {
        byte[] key = Enumerable.Repeat((byte)1, 32).ToArray();
        byte[] a1 = HandshakeAuth.FakeSalt(key, "ghost");
        byte[] a2 = HandshakeAuth.FakeSalt(key, "ghost");
        byte[] b = HandshakeAuth.FakeSalt(key, "phantom");
        Assert.Equal(HandshakeAuth.SaltSize, a1.Length);
        Assert.Equal(a1, a2);
        Assert.NotEqual(a1, b);
    }

    [Fact]
    public void IterationCountIsBounded()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => HandshakeAuth.DeriveClientKeys("s", Salt, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => HandshakeAuth.DeriveClientKeys("s", Salt, HandshakeAuth.MaxIterations + 1));
    }
}
