namespace Tedd.Tdfs.Protocol;

/// <summary>Decoder limits; any field exceeding them is rejected as <see cref="DfsErrorCode.ProtocolError"/>.</summary>
/// <param name="MaxStringBytes">Maximum UTF-8 bytes of one string field.</param>
/// <param name="MaxBytesLength">Maximum length of one byte-array field.</param>
/// <param name="MaxCollectionCount">Maximum element count of one collection (and of an extension block).</param>
public sealed record WireLimits(int MaxStringBytes = 64 * 1024, int MaxBytesLength = 64 << 20, int MaxCollectionCount = 1 << 20)
{
    /// <summary>The default limits.</summary>
    public static WireLimits Default { get; } = new();
}
