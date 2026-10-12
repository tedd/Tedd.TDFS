namespace Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

/// <summary>Test fault injector: records every hit and optionally throws at one site.</summary>
public sealed class RecordingFaults : IFaultInjector
{
    private readonly List<FaultContext> _hits = [];

    public bool IsEnabled => true;

    /// <summary>Site at which <see cref="ThrowFactory"/> fires (null = never).</summary>
    public string? ThrowAtSite { get; set; }

    /// <summary>Number of further hits of <see cref="ThrowAtSite"/> that throw.</summary>
    public int ThrowCount { get; set; } = 1;

    public Func<Exception> ThrowFactory { get; set; } = () => new InvalidOperationException("injected fault");

    public IReadOnlyList<FaultContext> Hits
    {
        get
        {
            lock (_hits)
                return [.. _hits];
        }
    }

    public IReadOnlyList<string> Sites => [.. Hits.Select(h => h.Site)];

    public ValueTask HitAsync(FaultContext context, CancellationToken ct = default)
    {
        lock (_hits)
            _hits.Add(context);
        if (ThrowAtSite is not null && context.Site == ThrowAtSite && ThrowCount > 0)
        {
            ThrowCount--;
            throw ThrowFactory();
        }

        return default;
    }
}
