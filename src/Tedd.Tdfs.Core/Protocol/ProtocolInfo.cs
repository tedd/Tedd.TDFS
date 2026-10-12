namespace Tedd.Tdfs.Protocol;

/// <summary>Protocol version constants negotiated during the handshake, and fixed sizes used by the wire format.</summary>
public static class ProtocolInfo
{
    /// <summary>Protocol version spoken by this build.</summary>
    public const int CurrentVersion = 1;

    /// <summary>Oldest protocol version this build can still speak.</summary>
    public const int MinSupportedVersion = 1;

    /// <summary>Size in bytes of every HMAC-SHA256 tag carried on the wire (receipts, capabilities, handshake proofs).</summary>
    public const int MacSize = 32;

    /// <summary>Size in bytes of the handshake nonces.</summary>
    public const int NonceSize = 32;

    /// <summary>Wire name of the control Raft group.</summary>
    public const string ControlGroup = "control";

    /// <summary>Prefix of volume Raft group names on the wire (<c>"vol:" + VolumeId</c> in "D" format).</summary>
    public const string VolumeGroupPrefix = "vol:";

    /// <summary>
    /// Returns the wire group name of a volume's Raft group. Groups are keyed by volume id rather than name so that a
    /// deleted and re-created volume name never receives stale Raft traffic addressed to the old group.
    /// </summary>
    public static string VolumeGroup(VolumeId volume) => VolumeGroupPrefix + volume.ToString();
}
