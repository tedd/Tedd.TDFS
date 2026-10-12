namespace Tedd.Tdfs.Server;

/// <summary>A client session.</summary>
/// <param name="Id">Session id.</param>
/// <param name="Principal">Owning principal.</param>
/// <param name="Expired">True when the session expired.</param>
/// <param name="RetiredWatermark">Sequence numbers at or below this are retired.</param>
/// <param name="RetainedOutcomes">Outcome records currently retained.</param>
public sealed record SessionInspection(ulong Id, string Principal, bool Expired, ulong RetiredWatermark, int RetainedOutcomes);
