namespace Tedd.Tdfs;

/// <summary>Retention of a snapshot or backup checkpoint.</summary>
/// <param name="ExpiresUtc">Earliest time deletion is allowed; null means no time-based protection.</param>
/// <param name="Hold">True while a retention hold prevents deletion.</param>
public sealed record DfsRetention(DateTimeOffset? ExpiresUtc = null, bool Hold = false);
