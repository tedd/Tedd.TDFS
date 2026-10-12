namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>An exception that escaped a work item of a live simulated process (or a global work item).</summary>
/// <param name="Process">Id (<c>name#incarnation</c>) of the owning process, or "global".</param>
/// <param name="Lane">Name of the lane the item ran on.</param>
/// <param name="VirtualTime">Virtual time of the failure.</param>
/// <param name="Exception">The exception.</param>
public sealed record SimUnhandledError(string Process, string Lane, TimeSpan VirtualTime, Exception Exception)
{
    /// <summary>One-line summary.</summary>
    public override string ToString() =>
        $"t={SimScheduler.FormatTime(VirtualTime)} {Lane}: {Exception.GetType().FullName}: {Exception.Message}";
}
