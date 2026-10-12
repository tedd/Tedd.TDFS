namespace Tedd.Tdfs;

/// <summary>Adapter metadata recorded in a snapshot.</summary>
/// <param name="Id">Adapter id.</param>
/// <param name="Version">Adapter version.</param>
/// <param name="RestoreRequirements">Requirements a restore must satisfy.</param>
public sealed record DfsAdapterRecord(string Id, string Version, IReadOnlyDictionary<string, string> RestoreRequirements);
