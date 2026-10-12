namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>A deterministic simulation run failed (livelock, unhandled work-item exception, ...).</summary>
public class SimulationException : Exception
{
    /// <summary>Creates the exception.</summary>
    public SimulationException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }
}
