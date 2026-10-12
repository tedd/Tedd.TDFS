using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// One message transmission observed by the harness link policy (<see cref="TestNetwork.GetMessageAttempts"/>), including
/// messages the policy dropped, delayed, held or black-holed (OPS-007).
/// </summary>
/// <param name="Sequence">Monotonic position of the observation in the cluster-wide network log (starting at 1).</param>
/// <param name="From">Sender identity ("n0", "b0", "client:c0").</param>
/// <param name="To">Receiver identity.</param>
/// <param name="Direction">Request, reply or one-way.</param>
/// <param name="Type">Message type from the frame header.</param>
/// <param name="CorrelationId">
/// Transport correlation id of the transmission: distinct for every request transmission (each retry or reroute of the same
/// logical operation is a new transmission with a new id); a reply carries the id of the request it answers.
/// </param>
/// <param name="OperationId">
/// The operation id carried in the request's operation header (every mutating request and <c>QueryOperation</c>); for a reply,
/// the operation id of the request it answers; null for messages without an operation header.
/// </param>
/// <param name="Bytes">Frame size in bytes.</param>
/// <param name="Decision">What the link policy did with the message.</param>
public sealed record MessageAttempt(
    long Sequence,
    string From,
    string To,
    LinkDirection Direction,
    MessageType Type,
    ulong CorrelationId,
    DfsOperationId? OperationId,
    int Bytes,
    LinkDecisionKind Decision);
