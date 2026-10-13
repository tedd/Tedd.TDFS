using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Text;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Testing;

/// <summary>Entry point for cluster scenarios.</summary>
/// <remarks>
/// <para><b>Realtime</b> runs start the cluster on real threads, run the scenario under a wall-clock
/// <see cref="TestClusterOptions.ScenarioTimeout"/>, check the end-of-run invariants and dispose the cluster.</para>
/// <para><b>Deterministic</b> runs create one <see cref="SimScheduler"/> seeded from the run's seed and drive it from a
/// dedicated driver thread in four phases — start (until stable), scenario (bounded by
/// <see cref="TestClusterOptions.ScenarioTimeout"/> of virtual time), end-of-run invariants, disposal — each through
/// <see cref="SimScheduler.RunAsync(Func{Task}, TimeSpan?)"/>, so the scenario and every SDK client it creates execute inside
/// the simulation on the scheduler's "scenario" process. Work posted into the simulation from foreign threads is recorded
/// (and still executed) instead of failing the run immediately, so the run can report every uncontrolled dependency.</para>
/// <para><b>Outcome precedence</b>: a run with environment-assumption violations throws
/// <see cref="AssumptionViolatedException"/> (inner: the scenario failure, else an invariant failure); otherwise a run with
/// uncontrolled dependencies throws <see cref="UncontrolledDependencyException"/> (inner: the scenario failure); otherwise the
/// scenario's own exception; otherwise an end-of-run invariant failure. On any failure the seed, mode, data root and the
/// tail of the trace are written to the test output and the data root is kept.</para>
/// </remarks>
public static class ClusterTest
{
    private const int TraceTailLines = 60;

    /// <summary>
    /// Starts a cluster, waits until it is stable (control and all volume leaders elected, all nodes caught up), runs the
    /// scenario (inside the simulation in deterministic mode), checks harness invariants and disposes the cluster.
    /// </summary>
    public static Task RunAsync(TestClusterOptions options, Func<TestCluster, Task> scenario, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        return RunCoreAsync<object?>(options, HarnessStartOptions.Default, async cluster =>
        {
            await scenario(cluster);
            return null;
        }, ct);
    }

    /// <summary>As <see cref="RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/>, returning the scenario result.</summary>
    public static Task<T> RunAsync<T>(TestClusterOptions options, Func<TestCluster, Task<T>> scenario, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        return RunCoreAsync(options, HarnessStartOptions.Default, scenario, ct);
    }

    /// <summary>Harness-internal overload with start settings (internal tests of the harness).</summary>
    internal static Task RunAsync(TestClusterOptions options, HarnessStartOptions start, Func<TestCluster, Task> scenario, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        return RunCoreAsync<object?>(options, start, async cluster =>
        {
            await scenario(cluster);
            return null;
        }, ct);
    }

    /// <summary>Harness-internal overload with start settings, returning the scenario result.</summary>
    internal static Task<T> RunAsync<T>(TestClusterOptions options, HarnessStartOptions start, Func<TestCluster, Task<T>> scenario, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        return RunCoreAsync(options, start, scenario, ct);
    }

    private static Task<T> RunCoreAsync<T>(TestClusterOptions options, HarnessStartOptions start, Func<TestCluster, Task<T>> scenario, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(start);
        options.Validate();
        string testName = TestCluster.CurrentTestName();
        ITestOutputHelper? output = TestContext.Current?.TestOutputHelper;
        return options.Mode == ClusterMode.Deterministic
            ? RunDeterministicAsync(options, start, scenario, testName, output, ct)
            : RunRealtimeAsync(options, start, scenario, testName, output, ct);
    }

    // -------------------------------------------------------------------------------------------- realtime

    private static async Task<T> RunRealtimeAsync<T>(TestClusterOptions options, HarnessStartOptions start, Func<TestCluster, Task<T>> scenario,
        string testName, ITestOutputHelper? output, CancellationToken ct)
    {
        ClusterRuntime runtime = ClusterRuntime.CreateRealtime();
        TestCluster cluster;
        try
        {
            cluster = await TestCluster.StartCoreAsync(options, runtime, testName, ct, start);
        }
        catch (Exception ex)
        {
            Report(output, options, options.ResolveSeed(testName), null, "cluster start failed", ex);
            throw;
        }

        Exception? scenarioFailure = null;
        Exception? invariantFailure = null;
        T result = default!;
        try
        {
            Task<T> run = RunScenarioAsync(cluster, scenario);
            if (!await runtime.WaitAsync(run, options.ScenarioTimeout))
            {
                cluster.Record($"scenario exceeded its timeout of {options.ScenarioTimeout}");
                scenarioFailure = new TimeoutException($"The scenario did not complete within ScenarioTimeout ({options.ScenarioTimeout}).");
            }
            else
            {
                result = await run;
            }
        }
        catch (Exception ex)
        {
            scenarioFailure = ex;
        }

        if (scenarioFailure is null || cluster.AssumptionViolations.Count > 0)
        {
            try
            {
                await cluster.CheckEndOfRunInvariantsAsync();
            }
            catch (Exception ex)
            {
                invariantFailure = ex;
            }
        }

        Exception? outcome = Classify(cluster, scenarioFailure, invariantFailure);
        if (outcome is not null)
            cluster.KeepData = true;

        // A disposal failure never hides the run's own outcome; it fails an otherwise passing run.
        Exception? disposeFailure = null;
        try
        {
            await cluster.DisposeAsync();
        }
        catch (Exception ex)
        {
            disposeFailure = ex;
        }

        outcome ??= disposeFailure;
        if (outcome is not null)
        {
            Report(output, options, cluster.Seed, cluster, "scenario failed", outcome);
            ExceptionDispatchInfo.Capture(outcome).Throw();
        }

        return result;
    }

    private static async Task<T> RunScenarioAsync<T>(TestCluster cluster, Func<TestCluster, Task<T>> scenario)
    {
        await cluster.WaitForStableAsync();
        cluster.Record("scenario: start");
        T result = await scenario(cluster);
        cluster.Record("scenario: end");
        return result;
    }

    // -------------------------------------------------------------------------------------------- deterministic

    private static Task<T> RunDeterministicAsync<T>(TestClusterOptions options, HarnessStartOptions start, Func<TestCluster, Task<T>> scenario,
        string testName, ITestOutputHelper? output, CancellationToken ct)
    {
        var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        ulong seed = options.ResolveSeed(testName);
        var thread = new Thread(() =>
        {
            try
            {
                completion.TrySetResult(DriveDeterministic(options, start, scenario, testName, output, seed, ct));
            }
            catch (Exception ex)
            {
                completion.TrySetException(ex);
            }
        })
        {
            IsBackground = true,
            Name = string.Create(CultureInfo.InvariantCulture, $"tdfs-sim 0x{seed:x16}"),
        };
        thread.Start();
        return completion.Task;
    }

    /// <summary>Drives one deterministic run on the calling (driver) thread.</summary>
    private static T DriveDeterministic<T>(TestClusterOptions options, HarnessStartOptions start, Func<TestCluster, Task<T>> scenario, string testName,
        ITestOutputHelper? output, ulong seed, CancellationToken ct)
    {
        var scheduler = new SimScheduler(SeededRandom.DeriveSeed(seed, "scheduler"), new SimSchedulerOptions
        {
            ClientProcessName = "scenario",
            ThrowOnViolation = false,
        });
        ClusterRuntime runtime = ClusterRuntime.CreateDeterministic(scheduler);

        // Phase 1: start until stable.
        TestCluster cluster;
        Task<TestCluster> started = scheduler.RunAsync(() =>
        {
            TestCluster.AmbientRuntime.Value = runtime;
            return TestCluster.StartCoreAsync(options, runtime, testName, CancellationToken.None, start);
        }, start.StartTimeout + TimeSpan.FromMinutes(1));
        try
        {
            cluster = started.GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            Report(output, options, seed, null, "cluster start failed", ex, scheduler);
            throw;
        }

        // Phase 2: the scenario, inside the simulation, on the "scenario" process. A run cancelled before it starts still
        // goes through disposal (the cluster is never leaked).
        Exception? scenarioFailure = null;
        T result = default!;
        try
        {
            ct.ThrowIfCancellationRequested();
            Task<T> run = scheduler.RunAsync(() =>
            {
                TestCluster.AmbientRuntime.Value = runtime;
                return RunScenarioAsync(cluster, scenario);
            }, options.ScenarioTimeout);
            result = run.GetAwaiter().GetResult();
        }
        catch (SimulationTimeoutException ex)
        {
            cluster.Record($"scenario exceeded its timeout of {options.ScenarioTimeout} (virtual time)");
            scenarioFailure = new TimeoutException($"The scenario did not complete within ScenarioTimeout ({options.ScenarioTimeout} of virtual time).", ex);
        }
        catch (Exception ex)
        {
            scenarioFailure = ex;
        }

        // Phase 3: end-of-run invariants.
        Exception? invariantFailure = null;
        if (scenarioFailure is null || cluster.AssumptionViolations.Count > 0)
        {
            try
            {
                scheduler.RunAsync(() => cluster.CheckEndOfRunInvariantsAsync(), TimeSpan.FromMinutes(5)).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                invariantFailure = ex;
            }
        }

        Exception? outcome = Classify(cluster, scenarioFailure, invariantFailure);
        if (outcome is not null)
            cluster.KeepData = true;

        // Phase 4: disposal.
        Exception? disposeFailure = null;
        try
        {
            scheduler.RunAsync(() => cluster.DisposeAsync().AsTask(), TimeSpan.FromMinutes(5)).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            disposeFailure = ex;
        }

        // Escapes recorded during disposal invalidate the run as well.
        outcome = Classify(cluster, scenarioFailure, invariantFailure) ?? disposeFailure;
        if (outcome is not null)
        {
            Report(output, options, seed, cluster, "scenario failed", outcome, scheduler);
            ExceptionDispatchInfo.Capture(outcome).Throw();
        }

        return result;
    }

    // -------------------------------------------------------------------------------------------- outcome

    /// <summary>The exception the run reports (precedence: assumptions, uncontrolled dependencies, scenario, invariants), or null.</summary>
    private static Exception? Classify(TestCluster cluster, Exception? scenarioFailure, Exception? invariantFailure)
    {
        IReadOnlyList<AssumptionViolation> assumptions = cluster.AssumptionViolations;
        if (assumptions.Count > 0)
            return new AssumptionViolatedException(assumptions, scenarioFailure ?? invariantFailure);
        IReadOnlyList<UncontrolledDependency> dependencies = cluster.UncontrolledDependencies;
        if (dependencies.Count > 0)
            return new UncontrolledDependencyException(dependencies, scenarioFailure);
        return scenarioFailure ?? invariantFailure;
    }

    /// <summary>Writes the replay information of a failed run to the test output.</summary>
    private static void Report(ITestOutputHelper? output, TestClusterOptions options, ulong seed, TestCluster? cluster, string what, Exception failure,
        SimScheduler? scheduler = null)
    {
        if (output is null)
            return;
        var sb = new StringBuilder();
        sb.Append(CultureInfo.InvariantCulture, $"TDFS {what}: seed 0x{seed:x16} (replay with TestClusterOptions.Seed = 0x{seed:x16}UL), mode {options.Mode}, ");
        sb.Append(CultureInfo.InvariantCulture, $"transport {options.Transport}, storage {options.Storage}, nodes {options.EffectiveNodeCount}");
        if (cluster is not null)
            sb.Append(CultureInfo.InvariantCulture, $", cluster {cluster.ClusterId}, data root {cluster.DataRoot}");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"Failure: {failure.GetType().FullName}: {failure.Message}").AppendLine();
        if (failure is UncontrolledDependencyException uncontrolled)
        {
            // Where the escapes happened (the summary only names their kind and component).
            foreach (UncontrolledDependency dependency in uncontrolled.Dependencies.Take(4))
                sb.Append(CultureInfo.InvariantCulture, $"Uncontrolled dependency ({dependency.Kind} in {dependency.Component}): {dependency.Detail}").AppendLine();
        }

        if (scheduler is not null)
            sb.Append(CultureInfo.InvariantCulture, $"Simulation: t={SimScheduler.FormatTime(scheduler.Now)}, steps {scheduler.StepCount}").AppendLine();
        if (cluster is not null)
        {
            IReadOnlyList<string> trace = cluster.Trace;
            sb.Append(CultureInfo.InvariantCulture, $"Trace (last {Math.Min(TraceTailLines, trace.Count)} of {trace.Count} events):").AppendLine();
            foreach (string line in trace.Skip(Math.Max(0, trace.Count - TraceTailLines)))
                sb.Append("  ").AppendLine(line);
            IReadOnlyList<string> log = cluster.NodeLog.Snapshot();
            if (log.Count > 0)
            {
                sb.Append(CultureInfo.InvariantCulture, $"Node warnings/errors (last {log.Count}):").AppendLine();
                foreach (string line in log)
                    sb.Append("  ").AppendLine(line);
            }
        }

        try
        {
            output.WriteLine(sb.ToString());
        }
        catch (InvalidOperationException)
        {
            // The test already finished (output closed); nothing else can carry the report.
        }
    }
}
