namespace Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

/// <summary>
/// Test fault injector that pauses selected hits until released, so tests can interleave operations deterministically
/// (for example two puts of the same object, or a put still waiting for its flush while another one rolls the segment).
/// </summary>
public sealed class PausingFaults : IFaultInjector
{
    private readonly List<(Func<FaultContext, bool> Match, TaskCompletionSource Release, TaskCompletionSource Reached)> _rules = [];

    public bool IsEnabled => true;

    /// <summary>Pauses the next hit matching <paramref name="match"/> (each rule fires once).</summary>
    /// <returns>A handle whose <see cref="Pause.Reached"/> completes when the hit happens and whose <see cref="Pause.Release"/> resumes it.</returns>
    public Pause PauseAt(Func<FaultContext, bool> match)
    {
        var release = new TaskCompletionSource();
        var reached = new TaskCompletionSource();
        lock (_rules)
            _rules.Add((match, release, reached));
        return new Pause(reached.Task, release);
    }

    public Pause PauseAt(string site, ObjectId? obj = null)
        => PauseAt(c => c.Site == site && (obj is null || c.Object == obj));

    public async ValueTask HitAsync(FaultContext context, CancellationToken ct = default)
    {
        (Func<FaultContext, bool> Match, TaskCompletionSource Release, TaskCompletionSource Reached)? rule = null;
        lock (_rules)
        {
            int i = _rules.FindIndex(r => r.Match(context));
            if (i >= 0)
            {
                rule = _rules[i];
                _rules.RemoveAt(i);
            }
        }

        if (rule is { } r2)
        {
            r2.Reached.TrySetResult();
            await r2.Release.Task;
        }
    }

    public sealed class Pause(Task reached, TaskCompletionSource release)
    {
        public Task Reached { get; } = reached;

        public void Release() => release.TrySetResult();
    }
}
