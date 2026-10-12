namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>An uncontrolled dependency was detected (HAR-002): the simulation is no longer deterministic.</summary>
public sealed class SimulationViolationException : SimulationException
{
    /// <summary>Creates the exception for <paramref name="violation"/>.</summary>
    public SimulationViolationException(SimViolation violation)
        : base($"Uncontrolled dependency detected in the simulation: {violation}")
    {
        Violation = violation;
    }

    /// <summary>The first violation that triggered the failure.</summary>
    public SimViolation Violation { get; }
}
