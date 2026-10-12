namespace Tedd.Tdfs.Testing;

/// <summary>What the client knows about an operation's effect.</summary>
public enum OpOutcome
{
    /// <summary>The operation took effect exactly once (output is authoritative).</summary>
    Ok,

    /// <summary>The operation definitely did not take effect.</summary>
    Failed,

    /// <summary>The operation may or may not have taken effect (in flight, timed out, outcome unknown).</summary>
    Unknown,
}
