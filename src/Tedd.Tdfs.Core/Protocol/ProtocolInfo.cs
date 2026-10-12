namespace Tedd.Tdfs.Protocol;

/// <summary>Protocol version constants negotiated during the handshake.</summary>
public static class ProtocolInfo
{
    /// <summary>Protocol version spoken by this build.</summary>
    public const int CurrentVersion = 1;

    /// <summary>Oldest protocol version this build can still speak.</summary>
    public const int MinSupportedVersion = 1;
}
