namespace Tedd.Tdfs.Testing;

/// <summary>Who knows whom at startup in <see cref="BootstrapMode.SeedJoin"/> mode.</summary>
public enum SeedGraph
{
    /// <summary>Every joining node knows every other node.</summary>
    Full,

    /// <summary>Node i knows node i-1 (wrapping around).</summary>
    Ring,

    /// <summary>Every joining node knows only the first node.</summary>
    Star,

    /// <summary>Node i knows only node i-1.</summary>
    Line,

    /// <summary>A seeded random connected graph.</summary>
    RandomConnected,
}
