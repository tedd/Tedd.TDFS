namespace Tedd.Tdfs.Testing;

/// <summary>How the initial cluster membership is established.</summary>
public enum BootstrapMode
{
    /// <summary>All initial nodes start with an identical static bootstrap description.</summary>
    Static,

    /// <summary>One node bootstraps a single-member cluster; the others join through seed endpoints.</summary>
    SeedJoin,
}
