using System.Buffers;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

internal sealed partial class VolumeService
{
    private const string ContinuationPrefix = "c1.";

    // ------------------------------------------------------------------------------------------------- mutations

    /// <summary><c>CreateDirectory</c>: recursive and idempotent options (committed).</summary>
    public Task<IWireMessage> CreateDirectoryAsync(RpcContext context, CreateDirectoryMessage request)
    {
        string path = NormalizePath(request.Path);
        if (DfsStreamPath.HasStream(path))
            throw new ArgumentException($"'{path}' addresses a stream; directories have no stream syntax.", "path");
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Write, "create directory");
        return MutateAsync(context, request, DfsOperationKind.CreateDirectory,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new CreateDirectoryCommand(path, request.Recursive, request.SnapshotName), op)));
    }

    /// <summary><c>Move</c>: atomic rename within the volume (committed).</summary>
    public Task<IWireMessage> MoveAsync(RpcContext context, MoveMessage request)
    {
        string source = NormalizePath(request.Source);
        string destination = NormalizePath(request.Destination);
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Write, "move");
        return MutateAsync(context, request, DfsOperationKind.Move,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new MoveCommand(source, destination, request.Overwrite, request.ExpectDirectory, request.SnapshotName), op)),
            cleanup: true);
    }

    /// <summary><c>Delete</c>: file, named stream or directory (recursive: atomic detach) (committed).</summary>
    public Task<IWireMessage> DeleteAsync(RpcContext context, DeleteMessage request)
    {
        string path = NormalizePath(request.Path);
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Write, "delete");
        return MutateAsync(context, request, DfsOperationKind.Delete,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new DeleteCommand(path, request.Recursive, request.ExpectDirectory, request.SnapshotName), op)),
            cleanup: true);
    }

    /// <summary><c>SetPolicy</c>: selects a named policy (validated against the volume's policies) for an inode (committed).</summary>
    public Task<IWireMessage> SetPolicyAsync(RpcContext context, SetPolicyMessage request)
    {
        string path = NormalizePath(request.Path);
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Write, "set storage policy");
        if (request.PolicyName.Length > 0 && !Record.NamedPolicies.ContainsKey(request.PolicyName))
            throw new DfsException(DfsErrorCode.InvalidPolicy, $"Volume '{Name}' has no storage policy named '{request.PolicyName}'.");
        return MutateAsync(context, request, DfsOperationKind.PolicyChange, async (op, ct) =>
        {
            var command = Stamp(new SetPolicyCommand(path, request.PolicyName), op);
            if (new NamespaceView(State.Inodes).Resolve(DfsStreamPath.GetFilePath(path)) is not { Found: true, Target: { } inode })
                return command;
            (DfsStoragePolicy policy, string pool) = PolicyOf(request.PolicyName);
            (ImmutableArray<StreamMaterialization> materialized, ulong staging) = await MaterializeInlineAsync(command, inode, policy, pool, op, ct);
            return staging == 0 ? command : command with { Materialized = materialized, LeaderStaging = staging };
        });
    }

    /// <summary><c>SetAccess</c>: the access-flag shortcut (committed <c>SetAcl</c>).</summary>
    public Task<IWireMessage> SetAccessAsync(RpcContext context, SetAccessMessage request)
    {
        string path = NormalizePath(request.Path);
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Write, "change access");
        return MutateAsync(context, request, DfsOperationKind.Admin,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new SetAclCommand(path, AclChangeKind.AccessFlags, null, request.OwnerOnly, request.ReadOnlyForOthers), op)));
    }

    // ------------------------------------------------------------------------------------------------- GetInfo

    /// <summary>
    /// <c>GetInfo</c> (FSD §5.4): absence is <c>Exists = false</c>; authorization requires ReadAttributes on the entry or
    /// ListDirectory on its parent (ACL-023), never reported as absence.
    /// </summary>
    public async Task<IWireMessage> GetInfoAsync(RpcContext context, GetInfoMessage request)
    {
        CancellationToken ct = context.Cancellation;
        string principal = context.Principal ?? string.Empty;
        BindTransaction(request, principal);
        RequireGrant(principal, DfsPermissions.Read, "get info");
        (IReadOnlySet<DfsIdentity> ids, bool admin) = IdentitySetOf(principal);
        if (!request.Handle.IsEmpty)
        {
            ulong applied = await ReadBarrierAsync(ct);
            VolumeState s = State;
            if (!s.Handles.TryGetValue(request.Handle.Value, out HandleRecord? h))
            {
                if (!s.IssuedHandle(request.Handle.Value))
                    throw new DfsException(DfsErrorCode.InvalidHandle, $"Handle {request.Handle.Value} was never issued by this volume.");
                throw new DfsLeaseLostException($"Handle {request.Handle.Value} is no longer active.");
            }

            if (!string.Equals(h.Principal, principal, StringComparison.Ordinal))
                throw new DfsException(DfsErrorCode.InvalidHandle, $"Handle {h.Id} belongs to another principal.");
            RenewHandle(h.Id);
            InodeRecord inode = h.View == HandleView.VersionPinned && h.PinnedInode is { } pinned ? pinned
                : s.Find(h.Inode) ?? throw new DfsLeaseLostException($"The inode of handle {h.Id} no longer exists.");
            string? current = new NamespaceView(s.Inodes).PathOf(h.Inode);
            string path = current is null ? string.Empty : DfsStreamPath.Combine(current, h.Stream.Length == 0 ? null : h.Stream);
            return new GetInfoResultMessage(true, VolumeStateMachine.Describe(s, inode, path, h.Stream.Length == 0 ? null : h.Stream), Token(applied));
        }

        string normalized = NormalizePath(request.Path);
        ImmutableSortedDictionary<ulong, InodeRecord> root;
        VolumeState? live = null;
        CommitToken observed;
        if (request.SnapshotPin != 0)
        {
            await ReadBarrierAsync(ct);
            (PinRecord pin, root) = ReaderPin(request.SnapshotPin, principal, request.Header.Session);
            RenewPin(pin.Id);
            observed = Token(pin.PinIndex);
        }
        else
        {
            ulong applied = await ReadBarrierAsync(ct);
            live = State;
            root = live.Inodes;
            observed = Token(applied);
        }

        (string filePath, string? stream) = DfsStreamPath.Split(normalized);
        var view = new NamespaceView(root);
        Resolution res = view.Resolve(filePath);
        RequireTraverse(res, ids, admin, principal);
        if (!res.Found)
        {
            RequireListOnNearest(res, ids, admin, principal, normalized);
            return new GetInfoResultMessage(false, null, observed);
        }

        InodeRecord target = res.Target!;
        IReadOnlyList<InodeRecord> chain = res.ChainNearestFirst();
        bool attributes = VolumeAccess.Has(VolumeAccess.Rights(chain, ids, admin), DfsFileSystemRights.ReadAttributes);
        bool list = res.Parent is not null && VolumeAccess.Has(VolumeAccess.Rights(res.ParentChainNearestFirst(), ids, admin), DfsFileSystemRights.ListDirectory);
        if (!attributes && !list)
            throw new UnauthorizedAccessException($"Principal '{principal}' may not read the attributes of '{normalized}'.");
        if (stream is not null && !target.Streams.ContainsKey(stream))
            return new GetInfoResultMessage(false, null, observed);
        return new GetInfoResultMessage(true, VolumeStateMachine.Describe(live, target, normalized, stream), observed);
    }

    /// <summary>Traverse enforcement for leader-side reads (ACL-025).</summary>
    private void RequireTraverse(Resolution res, IReadOnlySet<DfsIdentity> ids, bool admin, string principal)
    {
        if (!VolumeAccess.TraverseAllowed(State.Config.EnforceTraverseChecking, res, ids, admin))
            throw new UnauthorizedAccessException($"Principal '{principal}' lacks Traverse on an ancestor of '{res.Path}'.");
    }

    /// <summary>Reporting absence needs ListDirectory on the nearest existing ancestor directory (no disclosure through absence).</summary>
    private static void RequireListOnNearest(Resolution res, IReadOnlySet<DfsIdentity> ids, bool admin, string principal, string path)
    {
        if (res.Ancestors.Count == 0)
            return;
        IReadOnlyList<InodeRecord> chain = res.ParentChainNearestFirst();
        if (!VolumeAccess.Has(VolumeAccess.Rights(chain, ids, admin), DfsFileSystemRights.ListDirectory))
            throw new UnauthorizedAccessException($"Principal '{principal}' may not list the directory of '{path}'.");
    }

    // ------------------------------------------------------------------------------------------------- ListDirectory

    /// <summary>
    /// <c>ListDirectory</c> (NS-07, PROTOCOL.md §8.3): one pinned namespace version per enumeration. A listing that does not fit
    /// one page acquires an enumeration pin (committed <c>AcquirePin</c>, holding the version's root); continuations bind the
    /// volume, epoch, pin, directory, filter, principal and position, and fail with ContinuationExpired once the pin is gone.
    /// </summary>
    public async Task<IWireMessage> ListDirectoryAsync(RpcContext context, ListDirectoryMessage request)
    {
        CancellationToken ct = context.Cancellation;
        string principal = context.Principal ?? string.Empty;
        BindTransaction(request, principal);
        RequireGrant(principal, DfsPermissions.Read, "enumerate");
        if (request.PageSize is < 1 or > 10000)
            throw new ArgumentException("The page size must be between 1 and 10000.");
        if (request.Kinds == WireEntryKinds.None || ((int)request.Kinds & ~(int)WireEntryKinds.All) != 0)
            throw new ArgumentException($"Invalid entry kinds {(int)request.Kinds}.");
        if (GetPatternError(request.Pattern) is { } patternError)
            throw new ArgumentException(patternError);
        string path = NormalizePath(request.Path);
        if (DfsStreamPath.HasStream(path))
            throw new ArgumentException($"'{path}' addresses a stream; directories have no stream syntax.", "path");
        (IReadOnlySet<DfsIdentity> ids, bool admin) = IdentitySetOf(principal);
        await ReadBarrierAsync(ct);

        if (request.Continuation is { } continuation)
            return ContinueListing(request, continuation, path, principal);

        // First page: the snapshot view's root, or the live root (pinned when more than one page is needed).
        ImmutableSortedDictionary<ulong, InodeRecord> root;
        ulong snapshotPin = request.SnapshotPin;
        CommitToken pinnedAt;
        if (snapshotPin != 0)
        {
            (PinRecord readerPin, root) = ReaderPin(snapshotPin, principal, request.Header.Session);
            RenewPin(readerPin.Id);
            pinnedAt = Token(readerPin.PinIndex);
        }
        else
        {
            root = State.Inodes;
            pinnedAt = Token(State.AppliedIndex);
        }

        InodeRecord directory = ResolveListedDirectory(root, path, ids, admin, principal);
        string binding = Binding(directory.Id, request.Pattern, request.Kinds, principal, snapshotPin);
        List<DirectoryEntryData> page = Page(root, directory, request.Pattern, request.Kinds, null, request.PageSize, out bool more);
        if (!more)
            return new ListDirectoryResultMessage(page, null, pinnedAt, 0, new InodeId(directory.Id), directory.Revision);

        ulong pinId = snapshotPin;
        if (snapshotPin == 0)
        {
            // Pin the live version and serve the page from the pinned root (the pin is the linearization point).
            OperationHeader op = Header(context, request, DfsOperationKind.Enumerate, default);
            VolumeApplyResult acquired = await ProposeAsync(Stamp(new AcquirePinCommand(PinKind.Enumeration, request.Header.Session, null, directory.Id,
                binding, null), op), ct);
            if (!acquired.IsSuccess || acquired.Value is not PinRecord pin || pin.Root is null)
                return acquired.Reply ?? new ErrorReplyMessage(acquired.Code, acquired.Message);
            RenewPin(pin.Id);
            pinId = pin.Id;
            root = pin.Root;
            pinnedAt = Token(pin.CreatedIndex);
            directory = root.GetValueOrDefault(directory.Id) ?? throw new DirectoryNotFoundException($"'{path}' does not exist.");
            page = Page(root, directory, request.Pattern, request.Kinds, null, request.PageSize, out more);
            if (!more)
            {
                await ReleaseEnumerationPinAsync(pinId);
                return new ListDirectoryResultMessage(page, null, pinnedAt, 0, new InodeId(directory.Id), directory.Revision);
            }
        }

        string token = EncodeContinuation(pinId, directory.Id, page[^1].Name, binding);
        return new ListDirectoryResultMessage(page, token, pinnedAt, pinId, new InodeId(directory.Id), directory.Revision);
    }

    private IWireMessage ContinueListing(ListDirectoryMessage request, string continuation, string path, string principal)
    {
        if (!TryDecodeContinuation(continuation, out VolumeId volume, out ulong epoch, out ulong pinId, out ulong directoryId, out string after,
                out string tokenBinding) || volume != Record.Id || epoch != Record.Epoch)
        {
            throw new DfsPinExpiredException(DfsErrorCode.ContinuationExpired, "The continuation does not belong to this volume or epoch.");
        }

        string binding = Binding(directoryId, request.Pattern, request.Kinds, principal, request.SnapshotPin);
        if (!string.Equals(binding, tokenBinding, StringComparison.Ordinal))
            throw new DfsPinExpiredException(DfsErrorCode.ContinuationExpired, "The continuation belongs to another query (directory, filter or principal).");
        if (!State.Pins.TryGetValue(pinId, out PinRecord? pin) || pin.Root is null
            || !string.Equals(pin.Principal, principal, StringComparison.Ordinal)
            || (pin.Kind == PinKind.Enumeration && !string.Equals(pin.Binding, binding, StringComparison.Ordinal))
            || (pin.Kind == PinKind.SnapshotReader && pin.Id != request.SnapshotPin))
        {
            throw new DfsPinExpiredException(DfsErrorCode.ContinuationExpired, "The enumeration's pinned version expired; restart the enumeration.");
        }

        ImmutableSortedDictionary<ulong, InodeRecord> root = pin.Root;
        InodeRecord directory = root.GetValueOrDefault(directoryId) is { IsDirectory: true } d
            ? d
            : throw new DfsPinExpiredException(DfsErrorCode.ContinuationExpired, "The enumerated directory is not in the pinned version.");
        string? resolved = new NamespaceView(root).PathOf(directoryId);
        if (resolved is not null && !string.Equals(resolved, path, StringComparison.Ordinal))
            throw new DfsPinExpiredException(DfsErrorCode.ContinuationExpired, "The continuation belongs to another directory.");
        RenewPin(pin.Id);
        List<DirectoryEntryData> page = Page(root, directory, request.Pattern, request.Kinds, after, request.PageSize, out bool more);
        CommitToken pinnedAt = Token(pin.Kind == PinKind.Enumeration ? pin.CreatedIndex : pin.PinIndex);
        if (!more)
        {
            if (pin.Kind == PinKind.Enumeration)
                _ = ReleaseEnumerationPinAsync(pin.Id);
            return new ListDirectoryResultMessage(page, null, pinnedAt, 0, new InodeId(directory.Id), directory.Revision);
        }

        return new ListDirectoryResultMessage(page, EncodeContinuation(pin.Id, directory.Id, page[^1].Name, binding), pinnedAt, pin.Id,
            new InodeId(directory.Id), directory.Revision);
    }

    private async Task ReleaseEnumerationPinAsync(ulong pin)
    {
        if (!State.Pins.TryGetValue(pin, out PinRecord? record))
            return;
        var op = new OperationHeader(default, record.Principal, DfsOperationKind.Enumerate, UInt128.Zero, record.Session, 0, [], false, DfsPermissions.None,
            CommandFlags.None);
        await ProposeHousekeepingAsync(new ReleasePinCommand(pin) { Op = op });
        _pinRenewals.Remove(pin);
    }

    private InodeRecord ResolveListedDirectory(ImmutableSortedDictionary<ulong, InodeRecord> root, string path, IReadOnlySet<DfsIdentity> ids, bool admin,
        string principal)
    {
        Resolution res = new NamespaceView(root).Resolve(path);
        RequireTraverse(res, ids, admin, principal);
        if (!res.Found)
            throw new DirectoryNotFoundException($"'{path}' does not exist.");
        if (!res.Target!.IsDirectory)
            throw new DfsException(DfsErrorCode.NotADirectory, $"'{path}' is not a directory.");
        if (!VolumeAccess.Has(VolumeAccess.Rights(res.ChainNearestFirst(), ids, admin), DfsFileSystemRights.ListDirectory))
            throw new UnauthorizedAccessException($"Principal '{principal}' may not list '{path}'.");
        return res.Target;
    }

    private static List<DirectoryEntryData> Page(ImmutableSortedDictionary<ulong, InodeRecord> root, InodeRecord directory, string pattern,
        WireEntryKinds kinds, string? after, int pageSize, out bool more)
    {
        var page = new List<DirectoryEntryData>(Math.Min(pageSize, 256));
        more = false;
        foreach (KeyValuePair<string, ulong> child in directory.Children)
        {
            if (after is not null && string.CompareOrdinal(child.Key, after) <= 0)
                continue;
            if (root.GetValueOrDefault(child.Value) is not { } node)
                continue;
            bool include = node.IsDirectory ? (kinds & WireEntryKinds.Directories) != 0 : (kinds & WireEntryKinds.Files) != 0;
            if (!include || !Matches(pattern, child.Key))
                continue;
            if (page.Count == pageSize)
            {
                more = true;
                break;
            }

            long length = node.IsDirectory ? 0 : node.DefaultStream?.Length ?? 0;
            page.Add(new DirectoryEntryData(child.Key, node.IsDirectory ? WireEntryKind.Directory : WireEntryKind.File, new InodeId(node.Id), node.ReportedRevision,
                length, node.ModifiedUtc));
        }

        return page;
    }

    /// <summary>Validates an enumeration pattern: '*' and '?' wildcards, no '/', NUL or backslash.</summary>
    internal static string? GetPatternError(string? pattern)
    {
        if (pattern is null)
            return "A pattern is required.";
        if (pattern.Length == 0)
            return "The pattern must not be empty.";
        if (Encoding.UTF8.GetByteCount(pattern) > DfsPath.MaxComponentBytes)
            return "The pattern is longer than a path component.";
        foreach (char ch in pattern)
        {
            if (ch is '/' or '\\' or '\0')
                return $"The pattern '{pattern}' contains an invalid character.";
        }

        return null;
    }

    /// <summary>Ordinal wildcard match ('*' any run, '?' one character).</summary>
    internal static bool Matches(string pattern, string name)
    {
        int p = 0;
        int n = 0;
        int star = -1;
        int mark = 0;
        while (n < name.Length)
        {
            if (p < pattern.Length && (pattern[p] == '?' || pattern[p] == name[n]))
            {
                p++;
                n++;
            }
            else if (p < pattern.Length && pattern[p] == '*')
            {
                star = p++;
                mark = n;
            }
            else if (star >= 0)
            {
                p = star + 1;
                n = ++mark;
            }
            else
            {
                return false;
            }
        }

        while (p < pattern.Length && pattern[p] == '*')
            p++;
        return p == pattern.Length;
    }

    private string Binding(ulong directory, string pattern, WireEntryKinds kinds, string principal, ulong snapshotPin)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var w = new WireWriter(buffer);
        w.WriteVolumeId(Record.Id);
        w.WriteVarUInt(Record.Epoch);
        w.WriteVarUInt(directory);
        w.WriteString(pattern);
        w.WriteVarInt((int)kinds);
        w.WriteString(principal);
        w.WriteVarUInt(snapshotPin);
        return Convert.ToHexStringLower(SHA256.HashData(buffer.WrittenSpan)[..16]);
    }

    private string EncodeContinuation(ulong pin, ulong directory, string after, string binding)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var w = new WireWriter(buffer);
        w.WriteVolumeId(Record.Id);
        w.WriteVarUInt(Record.Epoch);
        w.WriteVarUInt(pin);
        w.WriteVarUInt(directory);
        w.WriteString(after);
        w.WriteString(binding);
        return ContinuationPrefix + Convert.ToBase64String(buffer.WrittenSpan).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    private static bool TryDecodeContinuation(string token, out VolumeId volume, out ulong epoch, out ulong pin, out ulong directory, out string after,
        out string binding)
    {
        volume = default;
        epoch = 0;
        pin = 0;
        directory = 0;
        after = string.Empty;
        binding = string.Empty;
        if (!token.StartsWith(ContinuationPrefix, StringComparison.Ordinal) || token.Length > 4096)
            return false;
        string body = token[ContinuationPrefix.Length..].Replace('-', '+').Replace('_', '/');
        body = body.PadRight(body.Length + ((4 - (body.Length % 4)) % 4), '=');
        byte[] bytes;
        try
        {
            bytes = Convert.FromBase64String(body);
        }
        catch (FormatException)
        {
            return false;
        }

        try
        {
            var r = new WireReader(bytes, WireLimits.Default);
            volume = r.ReadVolumeId();
            epoch = r.ReadVarUInt();
            pin = r.ReadVarUInt();
            directory = r.ReadVarUInt();
            after = r.ReadString();
            binding = r.ReadString();
            r.EnsureEnd();
            return true;
        }
        catch (DfsProtocolException)
        {
            return false;
        }
    }
}
