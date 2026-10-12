namespace Tedd.Tdfs.Server;

/// <summary>
/// Work handed to a node loop through <see cref="INodeLoop.InvokeAsync(Action)"/> (or a synchronous
/// <see cref="SynchronizationContext.Send"/>) could not run to completion because the loop stopped.
/// </summary>
public sealed class NodeLoopStoppedException : InvalidOperationException
{
    /// <summary>Creates the exception for the named loop.</summary>
    /// <param name="loopName">Name of the stopped loop.</param>
    public NodeLoopStoppedException(string loopName)
        : base($"The node loop '{loopName}' has stopped; the work was not run to completion.")
    {
        LoopName = loopName;
    }

    /// <summary>Name of the stopped loop.</summary>
    public string LoopName { get; }
}
