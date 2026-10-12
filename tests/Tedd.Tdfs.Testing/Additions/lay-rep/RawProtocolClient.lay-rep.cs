using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Testing;

/// <summary>Raw protocol additions for the layout (LAY) and replication (REP) suites.</summary>
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Opens a read handle on <paramref name="path"/> in <paramref name="volume"/> (as the authenticated principal, with
    /// <see cref="FileShare.ReadWrite"/>), sends one <c>ReadRange</c> request whose <paramref name="offset"/> and
    /// <paramref name="length"/> are encoded verbatim (no SDK validation, negative and overflowing values included),
    /// closes the handle and returns the server's error code (<see cref="DfsErrorCode.None"/> when the server returned a
    /// range result). The helper never allocates a buffer sized by <paramref name="length"/>; reply data is discarded.
    /// Used by LAY-018.
    /// </summary>
    /// <param name="volume">Volume name.</param>
    /// <param name="path">Path sent verbatim (must name an existing file for a meaningful result).</param>
    /// <param name="offset">Logical offset, sent unmodified.</param>
    /// <param name="length">Requested length, sent unmodified.</param>
    public async Task<DfsErrorCode> SendReadRangeAsync(string volume, string path, long offset, long length)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        ArgumentNullException.ThrowIfNull(path);
        (DfsErrorCode openCode, IWireMessage? openReply) = await SendVolumeRequestAsync(volume, mutating: true, header => Encode(new OpenHandleMessage(header,
            path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, FileOptions.None, VersionPinned: false, WriteThrough: false, StoragePolicy: null)));
        if (openReply is not OpenHandleResultMessage opened)
            return openCode == DfsErrorCode.None ? DfsErrorCode.ProtocolError : openCode;
        (DfsErrorCode code, _) = await SendVolumeRequestAsync(volume, mutating: false,
            header => Encode(new ReadRangeMessage(header, opened.Handle, 0, default, offset, length)));
        await SendVolumeRequestAsync(volume, mutating: true, header => Encode(new CloseHandleMessage(header, opened.Handle, opened.WriterGeneration)));
        return code;
    }

    /// <summary>
    /// Opens a writable handle on <paramref name="path"/> with <paramref name="mode"/> (<see cref="FileAccess.Write"/>,
    /// <see cref="FileShare.Read"/>) through the volume leader, creates a staging session bound to that handle and
    /// returns a <see cref="RawStagedWriter"/> that can upload objects to chosen storage nodes and publish arbitrary
    /// (possibly invalid) extent maps with arbitrary (possibly forged, stale or mismatched) receipts. Failures to open the
    /// handle or the staging session throw the mapped SDK exception (<see cref="DfsErrors.ToException"/>). Used by
    /// LAY-018, LAY-019, REP-012, REP-013 and REP-017.
    /// </summary>
    /// <param name="volume">Volume name.</param>
    /// <param name="path">Normalized path.</param>
    /// <param name="mode">Open mode (default <see cref="FileMode.OpenOrCreate"/>).</param>
    public async Task<RawStagedWriter> OpenStagedWriterAsync(string volume, string path, FileMode mode = FileMode.OpenOrCreate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        ArgumentNullException.ThrowIfNull(path);
        (DfsErrorCode openCode, IWireMessage? openReply) = await SendVolumeRequestAsync(volume, mutating: true, header => Encode(new OpenHandleMessage(header,
            path, mode, FileAccess.Write, FileShare.Read, FileOptions.None, VersionPinned: false, WriteThrough: false, StoragePolicy: null)));
        if (openReply is not OpenHandleResultMessage opened)
            throw Failure(openCode, openReply, $"OpenHandle of {path} on {volume}");
        (_, RawSession? session) = await GetSessionAsync(volume);
        if (session is null)
            throw DfsErrors.ToException(DfsErrorCode.Unavailable, $"The raw session on {volume} disappeared.");
        session.RememberGeneration(opened.Handle, opened.WriterGeneration);
        session.AutoRenew(opened.Handle);

        (DfsErrorCode stagingCode, IWireMessage? stagingReply) = await SendVolumeRequestAsync(volume, mutating: true,
            header => Encode(new CreateStagingMessage(header, opened.Handle, default)));
        if (stagingReply is not CreateStagingResultMessage staging)
        {
            session.Forget(opened.Handle);
            await SendVolumeRequestAsync(volume, mutating: true, header => Encode(new CloseHandleMessage(header, opened.Handle, opened.WriterGeneration)));
            throw Failure(stagingCode, stagingReply, $"CreateStaging for {path} on {volume}");
        }

        (_, VolumeId volumeId) = await ResolveVolumeIdAsync(volume);
        return new RawStagedWriter(this, session, volume, volumeId, opened.Handle, opened.Info.Inode, opened.WriterGeneration, opened.Info.Revision,
            staging.Staging, staging.Capability, staging.PlacementEpoch);
    }

    /// <summary>The SDK exception for a failed raw step.</summary>
    private static Exception Failure(DfsErrorCode code, IWireMessage? reply, string what) => reply is ErrorReplyMessage error
        ? error.ToException()
        : DfsErrors.ToException(code == DfsErrorCode.None ? DfsErrorCode.ProtocolError : code, $"{what} failed ({code}).");
}
