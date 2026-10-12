namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// Thrown by <see cref="SimProcess.Crash"/> when a process crashes itself, to unwind the code that was executing at the
/// crash point. The scheduler ignores it (and any other exception) when it escapes a work item of a killed process.
/// </summary>
public sealed class SimProcessKilledException : Exception
{
    /// <summary>Creates the exception for the process with the given id.</summary>
    public SimProcessKilledException(string processId)
        : base($"Simulated process '{processId}' was killed.")
    {
        ProcessId = processId;
    }

    /// <summary>Id (<c>name#incarnation</c>) of the killed process.</summary>
    public string ProcessId { get; }
}
