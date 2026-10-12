namespace Tedd.Tdfs.Server;

/// <summary>Node log sink. Entries never contain payload bytes or secrets.</summary>
public interface INodeLog
{
    /// <summary>Writes one entry.</summary>
    /// <param name="level">Severity.</param>
    /// <param name="node">Node name.</param>
    /// <param name="category">Component (for example "raft", "gc", "rpc").</param>
    /// <param name="message">Message text.</param>
    void Write(NodeLogLevel level, string node, string category, string message);
}
