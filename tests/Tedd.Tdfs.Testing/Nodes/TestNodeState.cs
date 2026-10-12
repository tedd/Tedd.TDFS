namespace Tedd.Tdfs.Testing;

/// <summary>Lifecycle state of a test node.</summary>
public enum TestNodeState
{
    /// <summary>The node loop is running.</summary>
    Running,

    /// <summary>The node loop is frozen; memory is retained and resumes where it stopped.</summary>
    Paused,

    /// <summary>The node was killed abruptly; only the durable disk image survives.</summary>
    Crashed,

    /// <summary>The node was stopped gracefully.</summary>
    Stopped,
}
