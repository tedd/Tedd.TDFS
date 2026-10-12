namespace Tedd.Tdfs.Testing;

/// <summary>Zone layout used when <see cref="TestClusterOptions.Nodes"/> is not given explicitly.</summary>
public enum TestTopology
{
    /// <summary>Node i is placed in zone <c>z{i+1}</c>.</summary>
    OnePerZone,

    /// <summary>All nodes share zone <c>z1</c>.</summary>
    SingleZone,

    /// <summary>Two zones: the first ceil(2n/3) nodes in <c>z1</c>, the rest in <c>z2</c> (3 nodes: 2+1).</summary>
    TwoPlusOne,

    /// <summary>Explicit layout; <see cref="TestClusterOptions.Nodes"/> must be set.</summary>
    Custom,
}
