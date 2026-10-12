using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

internal sealed partial class VolumeService
{
    /// <summary><c>GetAccessControl</c> (Addendum A): the full descriptor in evaluation order; requires ReadPermissions.</summary>
    public async Task<IWireMessage> GetAccessControlAsync(RpcContext context, GetAccessControlMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        (ImmutableSortedDictionary<ulong, InodeRecord> root, Resolution res, string path, CommitToken observed) =
            await ResolveForReadAsync(context, request, request.Path, request.SnapshotPin, "read permissions");
        (IReadOnlySet<DfsIdentity> ids, bool admin) = IdentitySetOf(principal);
        IReadOnlyList<InodeRecord> chain = res.ChainNearestFirst();
        if (!VolumeAccess.Has(VolumeAccess.Rights(chain, ids, admin), DfsFileSystemRights.ReadPermissions))
            throw new UnauthorizedAccessException($"Principal '{principal}' lacks ReadPermissions on '{path}'.");
        InodeRecord target = res.Target!;
        IReadOnlyList<DfsAccessRule> rules = VolumeAccess.EffectiveRules(chain, DfsStreamPath.GetFilePath(path));
        var descriptor = new DfsSecurityDescriptor(target.Owner, rules, target.Security.AreAccessRulesProtected);
        _ = root;
        return new GetAccessControlResultMessage(descriptor, new InodeId(target.Id), target.Revision, observed);
    }

    /// <summary><c>SetAccessControl</c>: replaces explicit entries and the protection flag (committed <c>SetAcl</c>).</summary>
    public Task<IWireMessage> SetAccessControlAsync(RpcContext context, SetAccessControlMessage request)
    {
        string path = NormalizePath(request.Path);
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Write, "change permissions");
        var explicitOnly = new DfsSecurityDescriptor(request.Descriptor.Owner,
            request.Descriptor.Rules.Where(r => r is not null && !r.IsInherited).ToArray(), request.Descriptor.AreAccessRulesProtected);
        if (explicitOnly.GetValidationError() is { } error)
            throw new ArgumentException("Invalid security descriptor: " + error);
        return MutateAsync(context, request, DfsOperationKind.Admin,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new SetAclCommand(path, AclChangeKind.Descriptor, explicitOnly, false, false), op)));
    }

    /// <summary><c>SetOwner</c>: TakeOwnership (self) or Administrators (any owner) (committed).</summary>
    public Task<IWireMessage> SetOwnerAsync(RpcContext context, SetOwnerMessage request)
    {
        string path = NormalizePath(request.Path);
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Write, "take ownership");
        if (request.Owner.IsDefault || request.Owner == DfsIdentity.CreatorOwner)
            throw new ArgumentException("CreatorOwner (or an empty identity) cannot own an inode.");
        return MutateAsync(context, request, DfsOperationKind.Admin,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new SetOwnerCommand(path, request.Owner), op)));
    }

    /// <summary>
    /// <c>GetEffectiveRights</c> (ACL-034): <see cref="DfsAccessEvaluator.GrantedRights"/> for the caller or another identity
    /// (whose groups and administrator flag come from the identity database; querying another identity needs ReadPermissions).
    /// </summary>
    public async Task<IWireMessage> GetEffectiveRightsAsync(RpcContext context, GetEffectiveRightsMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        (_, Resolution res, string path, CommitToken observed) = await ResolveForReadAsync(context, request, request.Path, request.SnapshotPin, "effective rights");
        IReadOnlyList<InodeRecord> chain = res.ChainNearestFirst();
        (IReadOnlySet<DfsIdentity> callerIds, bool callerAdmin) = IdentitySetOf(principal);
        IReadOnlySet<DfsIdentity> ids = callerIds;
        bool admin = callerAdmin;
        if (request.Identity is { } identity && identity != DfsIdentity.User(principal))
        {
            if (!VolumeAccess.Has(VolumeAccess.Rights(chain, callerIds, callerAdmin), DfsFileSystemRights.ReadPermissions))
                throw new UnauthorizedAccessException($"Principal '{principal}' lacks ReadPermissions on '{path}'.");
            (ids, admin) = IdentitySetOf(identity);
        }

        return new GetEffectiveRightsResultMessage(VolumeAccess.Rights(chain, ids, admin), observed);
    }

    /// <summary>The identity set of an arbitrary identity as the evaluator would see it.</summary>
    private (IReadOnlySet<DfsIdentity> Set, bool IsAdmin) IdentitySetOf(DfsIdentity identity)
    {
        ControlState control = _s.Control.State;
        switch (identity.Kind)
        {
            case DfsIdentityKind.User:
                return IdentitySetOf(identity.Name);
            case DfsIdentityKind.Group:
                return (new HashSet<DfsIdentity> { identity, DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers }, false);
            default:
                if (identity == DfsIdentity.Administrators)
                    return (new HashSet<DfsIdentity> { DfsIdentity.Administrators, DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers }, true);
                if (identity == DfsIdentity.AuthenticatedUsers)
                    return (new HashSet<DfsIdentity> { DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers }, false);
                _ = control;
                return (new HashSet<DfsIdentity> { DfsIdentity.Everyone }, false);
        }
    }

    /// <summary><c>ListStreams</c> (ADS-03): named streams of a file or directory with lengths and revisions.</summary>
    public async Task<IWireMessage> ListStreamsAsync(RpcContext context, ListStreamsMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        string normalized = NormalizePath(request.Path);
        if (DfsStreamPath.HasStream(normalized))
            throw new ArgumentException($"'{normalized}' addresses a stream; list the streams of its file.", "path");
        (_, Resolution res, string path, CommitToken observed) = await ResolveForReadAsync(context, request, normalized, request.SnapshotPin, "list streams");
        (IReadOnlySet<DfsIdentity> ids, bool admin) = IdentitySetOf(principal);
        bool attributes = VolumeAccess.Has(VolumeAccess.Rights(res.ChainNearestFirst(), ids, admin), DfsFileSystemRights.ReadAttributes);
        bool list = res.Parent is not null && VolumeAccess.Has(VolumeAccess.Rights(res.ParentChainNearestFirst(), ids, admin), DfsFileSystemRights.ListDirectory);
        if (!attributes && !list)
            throw new UnauthorizedAccessException($"Principal '{principal}' may not read the attributes of '{path}'.");
        var streams = res.Target!.Streams
            .Where(p => p.Key.Length > 0)
            .Select(p => new StreamInfoData(p.Key, p.Value.Length, p.Value.Revision))
            .ToArray();
        return new ListStreamsResultMessage(streams, observed);
    }

    /// <summary>
    /// Resolves an existing path for a read in the live view (after a ReadIndex barrier) or a snapshot view; absent entries
    /// fail with FileNotFound/DirectoryNotFound (a stream path resolves its file; an absent stream fails).
    /// </summary>
    private async Task<(ImmutableSortedDictionary<ulong, InodeRecord> Root, Resolution Resolution, string Path, CommitToken Observed)> ResolveForReadAsync(
        RpcContext context, IVolumeRequest request, string rawPath, ulong snapshotPin, string what)
    {
        CancellationToken ct = context.Cancellation;
        RejectTransaction(request);
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Read, what);
        string path = NormalizePath(rawPath);
        ulong applied = await ReadBarrierAsync(ct);
        ImmutableSortedDictionary<ulong, InodeRecord> root;
        CommitToken observed;
        if (snapshotPin != 0)
        {
            (PinRecord pin, root) = ReaderPin(snapshotPin, principal, request.Header.Session);
            RenewPin(pin.Id);
            observed = Token(pin.PinIndex);
        }
        else
        {
            root = State.Inodes;
            observed = Token(applied);
        }

        (string filePath, string? stream) = DfsStreamPath.Split(path);
        Resolution res = new NamespaceView(root).Resolve(filePath);
        (IReadOnlySet<DfsIdentity> ids, bool admin) = IdentitySetOf(principal);
        RequireTraverse(res, ids, admin, principal);
        if (!res.Found)
        {
            if (!res.ParentExists)
                throw new DirectoryNotFoundException($"The directory of '{path}' does not exist.");
            throw new FileNotFoundException($"'{path}' does not exist.", path);
        }

        if (stream is not null && !res.Target!.Streams.ContainsKey(stream))
            throw new FileNotFoundException($"Stream '{path}' does not exist.", path);
        return (root, res, path, observed);
    }
}
