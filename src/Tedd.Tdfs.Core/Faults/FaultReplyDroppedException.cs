namespace Tedd.Tdfs;

/// <summary>
/// Thrown by an <see cref="IFaultInjector"/> from <see cref="IFaultInjector.HitAsync"/> at a reply-sending site
/// (<see cref="FaultSites.RpcReplySend"/>) to make the hitting server discard the reply it is about to send: the request
/// was executed, the caller only loses the answer (harness action <c>FaultAction.DropReply</c>). Servers catch it at the
/// site and send nothing; anywhere else it is an ordinary failure.
/// </summary>
public sealed class FaultReplyDroppedException : Exception
{
    /// <summary>Creates the exception.</summary>
    public FaultReplyDroppedException()
        : base("The reply was dropped by fault injection.")
    {
    }

    /// <summary>Creates the exception with a message.</summary>
    public FaultReplyDroppedException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and an inner exception.</summary>
    public FaultReplyDroppedException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
