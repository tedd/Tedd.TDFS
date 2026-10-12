namespace Tedd.Tdfs;

/// <summary>Fault-injection hook called at every registered stage boundary in client and server code.</summary>
public interface IFaultInjector
{
    /// <summary>False when no faults can fire; callers may then skip building contexts.</summary>
    bool IsEnabled { get; }

    /// <summary>Reports a site hit; the injector may throw, delay, pause or crash the node.</summary>
    ValueTask HitAsync(FaultContext context, CancellationToken ct = default);
}
