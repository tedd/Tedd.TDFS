namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>A run exhausted its virtual-time (or real-time) budget before its condition was met.</summary>
public sealed class SimulationTimeoutException : TimeoutException
{
    /// <summary>Creates the exception.</summary>
    public SimulationTimeoutException(string message, TimeSpan virtualTime)
        : base(message)
    {
        VirtualTime = virtualTime;
    }

    /// <summary>Virtual time (elapsed since the simulation start) when the budget ran out.</summary>
    public TimeSpan VirtualTime { get; }
}
