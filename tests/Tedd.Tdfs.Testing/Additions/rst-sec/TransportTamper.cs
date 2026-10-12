namespace Tedd.Tdfs.Testing;

/// <summary>How <see cref="TestNetwork.TamperNext"/> alters a protected transmission (SEC-011).</summary>
public enum TransportTamper
{
    /// <summary>Flip one bit of the protected payload (ciphertext or MAC-protected envelope), chosen from the cluster seed.</summary>
    FlipBit,

    /// <summary>Truncate the protected packet/envelope by a seeded number of bytes (at least one).</summary>
    Truncate,

    /// <summary>Append seeded garbage bytes to the protected packet/envelope.</summary>
    AppendGarbage,

    /// <summary>Deliver the protected packet/envelope twice (a byte-identical replay of an already accepted transmission).</summary>
    Replay,

    /// <summary>Replace the protected payload by seeded random bytes of the same length.</summary>
    Scramble,
}
