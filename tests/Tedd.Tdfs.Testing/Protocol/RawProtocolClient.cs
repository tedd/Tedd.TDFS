using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Testing;

/// <summary>A protocol client that bypasses all SDK validation (PATH-012, SEC-004/005/014/015).</summary>
public sealed partial class RawProtocolClient : IAsyncDisposable
{
    internal RawProtocolClient()
    {
    }

    /// <summary>True while the underlying connection is open.</summary>
    public bool IsConnected => throw Stub.NotYet("RawProtocolClient.IsConnected");

    /// <summary>Runs the authentication handshake; returns the resulting error code (<see cref="DfsErrorCode.None"/> on success).</summary>
    public Task<DfsErrorCode> AuthenticateAsync(DfsCredentials credentials) => throw Stub.NotYet("RawProtocolClient.AuthenticateAsync");

    /// <summary>
    /// Sends a path operation with the path verbatim (wire UTF-8 of the string). Kinds: CreateDirectory, Delete,
    /// Open (CreateNew, write), Move, GetInfo, WriteAll.
    /// </summary>
    public Task<DfsErrorCode> SendPathOperationAsync(string volume, DfsOperationKind kind, string rawPath, string? rawPath2 = null) =>
        throw Stub.NotYet("RawProtocolClient.SendPathOperationAsync");

    /// <summary>Sends a path operation whose path is arbitrary (possibly malformed) UTF-8 bytes.</summary>
    public Task<DfsErrorCode> SendRawPathBytesAsync(string volume, DfsOperationKind kind, byte[] rawUtf8PathBytes) =>
        throw Stub.NotYet("RawProtocolClient.SendRawPathBytesAsync");

    /// <summary>Sends arbitrary bytes; <see cref="DfsErrorCode.ProtocolError"/> when the server closes the connection.</summary>
    public Task<DfsErrorCode> SendFrameAsync(ReadOnlyMemory<byte> frame) => throw Stub.NotYet("RawProtocolClient.SendFrameAsync");

    /// <summary>Reads an object directly from a storage node without a capability.</summary>
    public Task<DfsErrorCode> GetObjectDirectAsync(string volume, ObjectId id) => throw Stub.NotYet("RawProtocolClient.GetObjectDirectAsync");

    /// <summary>Writes an object directly to a storage node without a capability.</summary>
    public Task<DfsErrorCode> PutObjectDirectAsync(string volume, ObjectId id, ReadOnlyMemory<byte> data) =>
        throw Stub.NotYet("RawProtocolClient.PutObjectDirectAsync");

    /// <summary>Typed round-trip; an error reply is returned as a message.</summary>
    public Task<IWireMessage> CallAsync(IWireMessage request) => throw Stub.NotYet("RawProtocolClient.CallAsync");

    /// <summary>Closes the connection.</summary>
    public ValueTask DisposeAsync() => throw Stub.NotYet("RawProtocolClient.DisposeAsync");
}
