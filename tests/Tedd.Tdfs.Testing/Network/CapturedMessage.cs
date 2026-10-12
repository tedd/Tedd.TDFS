using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>A frame captured by a <see cref="MessageHold"/>, replayable through <see cref="TestNetwork.ReplayAsync"/>.</summary>
/// <param name="From">Original sender name.</param>
/// <param name="To">Original receiver name.</param>
/// <param name="Direction">Request, reply or one-way.</param>
/// <param name="Type">Message type.</param>
/// <param name="Frame">The complete frame (<c>[u16 type][body]</c>).</param>
public sealed record CapturedMessage(string From, string To, LinkDirection Direction, MessageType Type, ReadOnlyMemory<byte> Frame);
