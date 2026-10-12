namespace Tedd.Tdfs.Server;

/// <summary>Log sink that discards every entry.</summary>
public sealed class NullNodeLog : INodeLog
{
    /// <summary>The shared instance.</summary>
    public static readonly NullNodeLog Instance = new();

    private NullNodeLog()
    {
    }

    /// <summary>Discards the entry.</summary>
    public void Write(NodeLogLevel level, string node, string category, string message)
    {
    }
}
