namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// A run could not make progress: nothing is runnable, no timer is pending and the awaited condition is unmet
/// (a deadlock, a lost wake-up, or work that depends on something outside the simulation).
/// </summary>
public sealed class SimulationStalledException : SimulationException
{
    /// <summary>Creates the exception.</summary>
    public SimulationStalledException(string message)
        : base(message)
    {
    }
}
