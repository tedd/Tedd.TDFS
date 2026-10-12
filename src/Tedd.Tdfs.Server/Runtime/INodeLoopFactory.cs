namespace Tedd.Tdfs.Server;

/// <summary>Creates the event loop of a node.</summary>
public interface INodeLoopFactory
{
    /// <summary>Creates a new, running loop for the named node.</summary>
    INodeLoop Create(string nodeName);
}
