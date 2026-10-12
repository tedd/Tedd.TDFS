namespace Tedd.Tdfs.Server;

/// <summary>Production loop factory: one dedicated thread per node running a FIFO work queue.</summary>
public sealed class ThreadNodeLoopFactory : INodeLoopFactory
{
    /// <summary>The shared instance.</summary>
    public static readonly ThreadNodeLoopFactory Instance = new();

    private ThreadNodeLoopFactory()
    {
    }

    /// <inheritdoc />
    public INodeLoop Create(string nodeName)
        => throw new NotImplementedException("ThreadNodeLoopFactory.Create: implemented in a later phase");
}
