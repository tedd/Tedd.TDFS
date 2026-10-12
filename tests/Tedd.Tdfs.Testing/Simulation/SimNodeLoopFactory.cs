using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// <see cref="INodeLoopFactory"/> for simulated nodes (<c>NodeEnvironment.LoopFactory</c>). Each loop is the main lane of
/// the live <see cref="SimProcess"/> named after the node, created on demand; after the previous loop was stopped
/// (process killed) the next loop for the same name gets a new incarnation.
/// </summary>
public sealed class SimNodeLoopFactory : INodeLoopFactory
{
    /// <summary>Creates a factory bound to <paramref name="scheduler"/>.</summary>
    public SimNodeLoopFactory(SimScheduler scheduler)
    {
        ArgumentNullException.ThrowIfNull(scheduler);
        Scheduler = scheduler;
    }

    /// <summary>The scheduler loops post into.</summary>
    public SimScheduler Scheduler { get; }

    /// <summary>Raised after a loop was created.</summary>
    public event Action<SimNodeLoop>? LoopCreated;

    /// <summary>Creates the loop of <paramref name="nodeName"/>.</summary>
    /// <exception cref="InvalidOperationException">The live process of that name already has a loop (stop it first).</exception>
    public SimNodeLoop Create(string nodeName)
    {
        ArgumentException.ThrowIfNullOrEmpty(nodeName);
        SimProcess process = Scheduler.GetOrCreateProcess(nodeName);
        SimNodeLoop loop;
        lock (Scheduler.SyncRoot)
        {
            if (process.Loop is not null)
                throw new InvalidOperationException($"Process {process.Id} already has a node loop; stop it before creating another loop for '{nodeName}'.");
            loop = new SimNodeLoop(process);
            process.Loop = loop;
        }

        LoopCreated?.Invoke(loop);
        return loop;
    }

    /// <inheritdoc />
    INodeLoop INodeLoopFactory.Create(string nodeName) => Create(nodeName);
}
