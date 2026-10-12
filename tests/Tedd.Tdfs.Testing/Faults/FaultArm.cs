namespace Tedd.Tdfs.Testing;

/// <summary>An armed fault: a match plus the action to run at matching hits.</summary>
public sealed class FaultArm
{
    internal FaultArm(FaultMatch match, FaultAction action)
    {
        ArgumentNullException.ThrowIfNull(match);
        ArgumentNullException.ThrowIfNull(action);
        match.Validate();
        Match = match;
        Action = action;
    }

    /// <summary>The match.</summary>
    public FaultMatch Match { get; }

    /// <summary>The action.</summary>
    public FaultAction Action { get; }

    /// <summary>Number of matching hits observed.</summary>
    public int Hits => throw Stub.NotYet("FaultArm.Hits");

    /// <summary>True once the action ran at least once.</summary>
    public bool Triggered => throw Stub.NotYet("FaultArm.Triggered");

    /// <summary>Contexts of the hits at which the action ran.</summary>
    public IReadOnlyList<FaultContext> Contexts => throw Stub.NotYet("FaultArm.Contexts");

    /// <summary>Waits until the action triggered; throws if it is never reached (HAR-013).</summary>
    public Task WaitTriggeredAsync(TimeSpan timeout) => throw Stub.NotYet("FaultArm.WaitTriggeredAsync");

    /// <summary>Releases operations/nodes held by <see cref="FaultAction.Block"/> or <see cref="FaultAction.PauseNode"/>.</summary>
    public void Release() => throw Stub.NotYet("FaultArm.Release");

    /// <summary>Stops matching further hits.</summary>
    public void Disarm() => throw Stub.NotYet("FaultArm.Disarm");
}
