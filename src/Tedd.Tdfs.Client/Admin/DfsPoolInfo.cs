namespace Tedd.Tdfs;

/// <summary>A storage pool (Addendum A, POOL-01).</summary>
/// <param name="Name">Pool name (<see cref="DfsPoolNames"/>).</param>
/// <param name="Nodes">Member node names, ordinal-sorted (for <c>default</c>: every live node not excluded from it).</param>
/// <param name="Volumes">Names of volumes bound to the pool or with a named policy naming it, ordinal-sorted.</param>
/// <param name="CapacityBytes">Total capacity of the member nodes.</param>
/// <param name="UsedBytes">Bytes used on the member nodes by objects placed through this pool.</param>
public sealed record DfsPoolInfo(string Name, IReadOnlyList<string> Nodes, IReadOnlyList<string> Volumes, long CapacityBytes, long UsedBytes);
