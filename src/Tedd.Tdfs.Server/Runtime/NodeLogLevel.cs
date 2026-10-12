namespace Tedd.Tdfs.Server;

/// <summary>Severity of a node log entry.</summary>
public enum NodeLogLevel
{
    /// <summary>Very detailed tracing.</summary>
    Trace,

    /// <summary>Debugging information.</summary>
    Debug,

    /// <summary>Normal operational events.</summary>
    Info,

    /// <summary>Unexpected but handled conditions.</summary>
    Warn,

    /// <summary>Failures.</summary>
    Error,
}
