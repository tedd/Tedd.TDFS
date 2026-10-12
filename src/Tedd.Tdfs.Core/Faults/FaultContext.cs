namespace Tedd.Tdfs;

/// <summary>Describes one fault-injection site hit.</summary>
/// <param name="Stage">Fault stage of the site.</param>
/// <param name="Site">Stable site name (see <see cref="FaultSites"/>).</param>
/// <param name="Node">Node (or client) name where the site was hit.</param>
/// <param name="Volume">Volume name, when applicable.</param>
/// <param name="Operation">Logical operation kind.</param>
/// <param name="OperationId">Operation id, when known.</param>
/// <param name="Path">Volume path, when applicable.</param>
/// <param name="Object">Object id, when applicable.</param>
/// <param name="Occurrence">Per-site occurrence index (for example the chunk index).</param>
/// <param name="Detail">Free-form detail (never payloads or secrets).</param>
public readonly record struct FaultContext(
    FaultStage Stage,
    string Site,
    string Node,
    string? Volume = null,
    DfsOperationKind Operation = DfsOperationKind.Unknown,
    DfsOperationId? OperationId = null,
    string? Path = null,
    ObjectId? Object = null,
    int Occurrence = 0,
    string? Detail = null);
