using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// What happens when an armed fault point is hit. Closed hierarchy: create instances through the static factory
/// methods and inspect them with pattern matching on the nested case records.
/// </summary>
public abstract record FaultAction
{
    private FaultAction()
    {
    }

    /// <summary>Crashes the node where the fault hit; its disk keeps only what <paramref name="mode"/> allows.</summary>
    public static FaultAction CrashNode(CrashMode mode = CrashMode.LoseUnflushed)
    {
        if (!Enum.IsDefined(mode))
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown crash mode.");
        return new CrashNodeFault(mode);
    }

    /// <summary>Pauses the node where the fault hit until <see cref="FaultArm.Release"/>.</summary>
    public static FaultAction PauseNode() => new PauseNodeFault();

    /// <summary>The hitting operation waits until <see cref="FaultArm.Release"/>.</summary>
    public static FaultAction Block() => new BlockFault();

    /// <summary>Throws the exception produced by <paramref name="factory"/> at the fault point.</summary>
    public static FaultAction Throw(Func<Exception> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        return new ThrowFault(factory);
    }

    /// <summary>Throws an <see cref="IOException"/> (EIO) at the fault point.</summary>
    public static FaultAction IoError() => new IoErrorFault();

    /// <summary>Fails with a disk-full condition (ENOSPC) at the fault point.</summary>
    public static FaultAction DiskFull() => new DiskFullFault();

    /// <summary>Delays the hitting operation by <paramref name="delay"/> on the node clock.</summary>
    public static FaultAction Delay(TimeSpan delay)
    {
        if (delay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Delay must be nonnegative.");
        return new DelayFault(delay);
    }

    /// <summary>The server executes the request but its reply is discarded.</summary>
    public static FaultAction DropReply() => new DropReplyFault();

    /// <summary>Cancels the client's token at this point (client sites).</summary>
    public static FaultAction CancelCaller() => new CancelCallerFault();

    /// <summary>Runs <paramref name="action"/> at the fault point (it may throw).</summary>
    public static FaultAction Custom(Func<FaultContext, Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return new CustomFault(action);
    }

    /// <summary>Crash the node.</summary>
    /// <param name="Mode">What the disk keeps.</param>
    public sealed record CrashNodeFault(CrashMode Mode) : FaultAction;

    /// <summary>Pause the node until released.</summary>
    public sealed record PauseNodeFault : FaultAction;

    /// <summary>Block the hitting operation until released.</summary>
    public sealed record BlockFault : FaultAction;

    /// <summary>Throw a custom exception.</summary>
    /// <param name="Factory">Creates the exception (called once per hit).</param>
    public sealed record ThrowFault(Func<Exception> Factory) : FaultAction;

    /// <summary>Throw an I/O error.</summary>
    public sealed record IoErrorFault : FaultAction;

    /// <summary>Report a full disk.</summary>
    public sealed record DiskFullFault : FaultAction;

    /// <summary>Delay the hitting operation.</summary>
    /// <param name="Duration">The delay.</param>
    public sealed record DelayFault(TimeSpan Duration) : FaultAction;

    /// <summary>Execute but discard the reply.</summary>
    public sealed record DropReplyFault : FaultAction;

    /// <summary>Cancel the client's token.</summary>
    public sealed record CancelCallerFault : FaultAction;

    /// <summary>Run custom code.</summary>
    /// <param name="Action">The code to run.</param>
    public sealed record CustomFault(Func<FaultContext, Task> Action) : FaultAction;
}
