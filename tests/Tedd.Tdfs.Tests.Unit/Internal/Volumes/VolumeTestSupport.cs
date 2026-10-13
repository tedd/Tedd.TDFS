using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// Drives a real <see cref="VolumeStateMachine"/> with encoded commands (codec + operation gate + handlers), the way a
/// Raft group applies committed entries, with a controllable leader clock.
/// </summary>
internal sealed class SmDriver
{
    public static readonly Guid ClusterId = new("0a4b2c1d-1111-2222-3333-444455556666");
    public static readonly VolumeId VolumeId = new(new Guid("7f000000-0000-0000-0000-000000000001"));
    public static readonly NodeId Node = new(new Guid("6e000000-0000-0000-0000-000000000001"));
    public static readonly DateTimeOffset Epoch0 = new(2026, 10, 12, 8, 0, 0, TimeSpan.Zero);

    private readonly List<byte[]> _log = [];

    public SmDriver(VolumeCreation? creation = null, ulong firstIndex = 3)
    {
        Creation = creation ?? CreationFor();
        Machine = new VolumeStateMachine(Creation, ClusterId);
        Index = firstIndex;
    }

    public VolumeCreation Creation { get; }

    public VolumeStateMachine Machine { get; }

    public VolumeState State => Machine.State;

    /// <summary>Index of the last applied entry.</summary>
    public ulong Index { get; private set; }

    public DateTimeOffset Now { get; set; } = Epoch0;

    /// <summary>Encoded commands applied so far (replayed by determinism tests).</summary>
    public IReadOnlyList<byte[]> Log => _log;

    public static VolumeCreation CreationFor(int inlineThreshold = 4096, DfsVolumeLimits? limits = null, bool enforceTraverse = false,
        DfsSecurityDescriptor? rootSecurity = null)
        => new(VolumeId, "vol", 1, "alice", Epoch0, 0, new DfsMetadataPolicy(), DfsStoragePolicy.Default,
            new Dictionary<string, DfsStoragePolicy> { ["pair"] = new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 2 } }, inlineThreshold,
            limits ?? new DfsVolumeLimits(), DfsPoolNames.Default, rootSecurity, enforceTraverse, [Node]);

    /// <summary>Applies one command (stamped with <see cref="Now"/>) at the next index.</summary>
    public VolumeApplyResult Apply(VolumeCommand command)
    {
        byte[] bytes = CommandCodec.Encode(command with { LeaderTime = Now });
        _log.Add(bytes);
        ApplyOutcome outcome = Machine.Apply(++Index, 1, bytes);
        return Assert.IsType<VolumeApplyResult>(outcome.Value);
    }

    /// <summary>Applies a command that must succeed and returns its reply.</summary>
    public T Ok<T>(VolumeCommand command) where T : class
    {
        VolumeApplyResult result = Apply(command);
        Assert.True(result.IsSuccess, $"{command.Kind} failed: {result.Code} {result.Message}");
        return Assert.IsType<T>(result.Reply);
    }

    /// <summary>Applies a command that must fail with <paramref name="code"/>.</summary>
    public VolumeApplyResult Fails(DfsErrorCode code, VolumeCommand command)
    {
        VolumeApplyResult result = Apply(command);
        Assert.False(result.IsSuccess, $"{command.Kind} unexpectedly succeeded.");
        Assert.Equal(code, result.Code);
        return result;
    }

    public byte[] Checkpoint()
    {
        var buffer = new ArrayBufferWriter<byte>();
        Machine.WriteCheckpoint(buffer);
        return buffer.WrittenSpan.ToArray();
    }

    // ------------------------------------------------------------------------------------------------- headers

    public static OperationHeader Op(string principal = "alice", ulong session = 0, ulong sequence = 0, DfsOperationKind kind = DfsOperationKind.Unknown,
        UInt128 payload = default, DfsPermissions grants = DfsPermissions.All, bool admin = false, ulong ackWatermark = 0, params string[] groups)
    {
        var identities = new List<string> { "AuthenticatedUsers", "Everyone", "user:" + principal };
        identities.AddRange(groups.Select(g => "group:" + g));
        if (admin)
            identities.Add("Administrators");
        identities.Sort(StringComparer.Ordinal);
        return new OperationHeader(sequence == 0 ? default : new DfsOperationId(session, sequence), principal, kind, payload, session, ackWatermark, [.. identities], admin, grants,
            CommandFlags.None);
    }

    // ------------------------------------------------------------------------------------------------- common flows

    public ulong OpenSession(string principal = "alice", UInt128 nonce = default, int maxOutcomes = 16)
    {
        var reply = Ok<OpenSessionResultMessage>(new OpenSessionCommand("test", nonce == UInt128.Zero ? new UInt128(1, Index) : nonce,
            TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(10), maxOutcomes, TimeSpan.FromSeconds(30)) { Op = Op(principal) });
        return reply.Session;
    }

    public OpenHandleResultMessage Open(ulong session, string path, FileMode mode, FileAccess access, FileShare share, ulong sequence = 0,
        string principal = "alice", bool pinned = false)
        => Ok<OpenHandleResultMessage>(OpenCommand(session, path, mode, access, share, sequence, principal, pinned));

    public static OpenHandleCommand OpenCommand(ulong session, string path, FileMode mode, FileAccess access, FileShare share, ulong sequence = 0,
        string principal = "alice", bool pinned = false)
        => new(path, mode, access, share, FileOptions.None, pinned, null, session, TimeSpan.FromSeconds(2))
        {
            Op = Op(principal, session, sequence, DfsOperationKind.Open, new UInt128(0xAB, (ulong)path.GetHashCode(StringComparison.Ordinal))),
        };

    public ulong Staging(ulong session, ulong handle = 0, string principal = "alice")
        => Assert.IsType<StagingRecord>(Apply(new CreateStagingCommand(session, handle, default) { Op = Op(principal, session) }).Value).Id;

    /// <summary>A data change referencing a (pretend) uploaded object.</summary>
    public static StreamChange DataChange(long offset, long length, ObjectId obj, ulong staging, long objectOffset = 0, long? objectLength = null)
        => new(offset, length, RangeChangeKind.Data, new ExtentRef(offset, length, obj, objectOffset, objectLength ?? length, ObjectChecksum.Compute([]), 0, 1,
            new StagingId(staging), [Node]), ReadOnlyMemory<byte>.Empty);

    public static StreamChange Inline(long offset, byte[] bytes) => new(offset, bytes.Length, RangeChangeKind.Inline, null, bytes);

    public static StreamChange Zero(long offset, long length) => new(offset, length, RangeChangeKind.Zero, null, ReadOnlyMemory<byte>.Empty);

    public static PublishSpec Spec(long newLength, ulong staging, bool inline, long truncateTo = -1, params StreamChange[] changes)
        => new(truncateTo, newLength, [.. changes], inline, 0, staging, 0);

    public static ObjectId Obj(ulong n) => new(new UInt128(0x0B1E, n));

    public static UInt128 Hash(string text) => BinaryPrimitives.ReadUInt128LittleEndian(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text)));

    public InodeRecord Inode(string path)
    {
        Resolution res = new NamespaceView(State.Inodes).Resolve(DfsStreamPath.GetFilePath(DfsPath.Normalize(path)));
        Assert.True(res.Found, $"{path} does not exist");
        return res.Target!;
    }

    public bool Exists(string path) => new NamespaceView(State.Inodes).Resolve(DfsStreamPath.GetFilePath(DfsPath.Normalize(path))).Found;

    public byte[] InlineBytes(string path, string stream = "")
    {
        StreamRecord s = Inode(path).GetStream(stream) ?? throw new InvalidOperationException("no stream");
        Assert.True(s.IsInline);
        return s.Inline.ToArray();
    }
}
