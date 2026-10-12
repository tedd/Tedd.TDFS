namespace Tedd.Tdfs;

/// <summary>Production fault injector: never fires.</summary>
public sealed class NoFaults : IFaultInjector
{
    /// <summary>The shared instance.</summary>
    public static readonly NoFaults Instance = new();

    private NoFaults()
    {
    }

    /// <summary>Always false.</summary>
    public bool IsEnabled => false;

    /// <summary>Completes synchronously without effect.</summary>
    public ValueTask HitAsync(FaultContext context, CancellationToken ct = default) => default;
}
