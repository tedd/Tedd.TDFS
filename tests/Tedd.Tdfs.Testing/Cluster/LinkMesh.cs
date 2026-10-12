namespace Tedd.Tdfs.Testing;

/// <summary>Which node-to-node links exist (fault model; clients always reach every node unless cut explicitly).</summary>
public enum LinkMesh
{
    /// <summary>Every node can reach every other node.</summary>
    Full,

    /// <summary>Node i links only to its two ring neighbours.</summary>
    Ring,

    /// <summary>Every node links only to the first node.</summary>
    Star,

    /// <summary>Node i links only to nodes i-1 and i+1.</summary>
    Line,
}
