namespace Tedd.Tdfs.Storage;

/// <summary>Outcome of one <see cref="ObjectStore.ScrubAsync"/> pass.</summary>
/// <param name="Checked">Number of objects verified in this pass.</param>
/// <param name="Corrupt">Objects that failed verification (quarantined).</param>
public sealed record ScrubResult(int Checked, IReadOnlyList<ObjectId> Corrupt);
