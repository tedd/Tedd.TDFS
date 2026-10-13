namespace Tedd.Tdfs.Storage;

/// <summary>
/// Changes whose durability barrier reported success (a dishonest flush, <see cref="SimulatedMediumFaults.DishonestFlush"/>)
/// but which were never actually made durable: either still volatile (<see cref="SimulatedMedium.GetAcknowledgedVolatileState"/>)
/// or discarded by a crash (<see cref="SimulatedMedium.LastCrashLoss"/>). A non-empty report is evidence that the storage
/// stack violated the flush-durability assumption (HAR-018).
/// </summary>
/// <param name="Writes">Acknowledged file writes and length changes.</param>
/// <param name="Bytes">Acknowledged written bytes among <paramref name="Writes"/>.</param>
/// <param name="NamespaceChanges">Acknowledged namespace changes (create, rename, delete, directory changes).</param>
/// <param name="Paths">Affected medium-relative paths (ordinal, distinct).</param>
public sealed record SimulatedLossReport(int Writes, long Bytes, int NamespaceChanges, IReadOnlyList<string> Paths)
{
    /// <summary>No acknowledged change is affected.</summary>
    public static SimulatedLossReport None { get; } = new(0, 0, 0, []);

    /// <summary>True when nothing acknowledged is affected.</summary>
    public bool IsEmpty => Writes == 0 && NamespaceChanges == 0;

    /// <summary>Combines two reports (paths merged, ordinal).</summary>
    public SimulatedLossReport Merge(SimulatedLossReport other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (other.IsEmpty)
            return this;
        if (IsEmpty)
            return other;
        var paths = new SortedSet<string>(Paths, StringComparer.Ordinal);
        paths.UnionWith(other.Paths);
        return new SimulatedLossReport(Writes + other.Writes, Bytes + other.Bytes, NamespaceChanges + other.NamespaceChanges, [.. paths]);
    }

    /// <inheritdoc />
    public override string ToString() =>
        IsEmpty
            ? "no acknowledged change affected"
            : $"{Writes} acknowledged write(s) ({Bytes} bytes) and {NamespaceChanges} acknowledged namespace change(s) on "
              + string.Join(", ", Paths.Take(8)) + (Paths.Count > 8 ? ", ..." : string.Empty);
}
