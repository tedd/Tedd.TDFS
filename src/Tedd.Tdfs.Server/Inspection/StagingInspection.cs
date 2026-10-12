namespace Tedd.Tdfs.Server;

/// <summary>A staging session.</summary>
/// <param name="Id">Staging id.</param>
/// <param name="Open">True while publications may still reference it.</param>
/// <param name="CreatedIndex">Raft index at which it was created.</param>
public sealed record StagingInspection(StagingId Id, bool Open, ulong CreatedIndex);
