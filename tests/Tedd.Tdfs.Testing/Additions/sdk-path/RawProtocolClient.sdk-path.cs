using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Testing;

// Additions requested by the SDK/PATH test suites (docs/api-additions/sdk-path.md). OpenHandleAsync, which PATH-014 also
// uses, is defined once in Additions/lock/RawProtocolClient.lock.cs (it returns RawOpenHandleResult).
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Sends one handle-addressed request for <paramref name="handle"/> to the leader of <paramref name="volume"/>,
    /// bypassing every SDK check, and returns the reply's error code (<see cref="DfsErrorCode.None"/> on success).
    /// Supported kinds: <see cref="DfsOperationKind.Read"/> (ReadRange offset 0, length 1),
    /// <see cref="DfsOperationKind.KeepAlive"/> (renew only this handle), <see cref="DfsOperationKind.CloseHandle"/>,
    /// <see cref="DfsOperationKind.CommitFile"/> (an empty commit — no changes, unchanged length — carrying the handle's
    /// writer generation as last reported by the server, or 0 when unknown). The request carries the handle number
    /// verbatim; it is never translated through the raw client's own handle table. Other kinds throw
    /// <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    /// <remarks>Routed to <paramref name="volume"/>'s current leader. Used by PATH-014: a handle issued by another volume
    /// must be rejected (for example <see cref="DfsErrorCode.InvalidHandle"/>) even when this same client holds a handle
    /// with the same number in the target volume.</remarks>
    public async Task<DfsErrorCode> SendHandleOperationAsync(string volume, DfsOperationKind kind, HandleId handle)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        if (kind is not (DfsOperationKind.Read or DfsOperationKind.KeepAlive or DfsOperationKind.CloseHandle or DfsOperationKind.CommitFile))
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "Supported kinds: Read, KeepAlive, CloseHandle, CommitFile.");
        (_, RawSession? session) = await GetSessionAsync(volume);
        ulong generation = session?.GenerationOf(handle) ?? 0;
        switch (kind)
        {
            case DfsOperationKind.Read:
            {
                (DfsErrorCode code, _) = await SendVolumeRequestAsync(volume, mutating: false,
                    header => Encode(new ReadRangeMessage(header, handle, 0, default, 0, 1)));
                return code;
            }

            case DfsOperationKind.KeepAlive:
            {
                (DfsErrorCode code, IWireMessage? reply) = await SendVolumeRequestAsync(volume, mutating: false,
                    header => Encode(new KeepAliveMessage(header, [handle], [], [])));
                return KeepAliveOutcome(code, reply, [handle]);
            }

            case DfsOperationKind.CloseHandle:
            {
                (DfsErrorCode code, _) = await SendVolumeRequestAsync(volume, mutating: true, header => Encode(new CloseHandleMessage(header, handle, generation)));
                return code;
            }

            default:
            {
                // An empty commit: no changes and the length the handle's file has now (as far as the server tells this client).
                (_, IWireMessage? info) = await SendVolumeRequestAsync(volume, mutating: false, header => Encode(new GetInfoMessage(header, string.Empty, handle, 0)));
                long length = info is GetInfoResultMessage { Info: { } file } ? file.Length : 0;
                (DfsErrorCode code, _) = await SendVolumeRequestAsync(volume, mutating: true, header => Encode(new CommitFileMessage(header, handle, generation,
                    BaseRevision: 0, TruncateTo: -1, NewLength: length, Changes: [], Receipts: [], Staging: default, SnapshotName: null)));
                return code;
            }
        }
    }
}
