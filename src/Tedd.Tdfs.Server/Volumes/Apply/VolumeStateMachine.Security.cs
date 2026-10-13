using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

internal sealed partial class VolumeStateMachine
{
    /// <summary>Resolves an existing inode for a metadata change (a stream path addresses its file).</summary>
    private static (Resolution Resolution, InodeRecord Inode, string Path) ResolveExisting(VolumeState s, ApplyContext ctx, string rawPath)
    {
        string path = NormalizePath(rawPath);
        string filePath = DfsStreamPath.GetFilePath(path);
        Resolution res = new NamespaceView(s.Inodes).Resolve(filePath);
        RequireTraverse(s, ctx, res);
        if (!res.Found)
            throw Fail(res.ParentExists ? DfsErrorCode.FileNotFound : DfsErrorCode.DirectoryNotFound, $"'{path}' does not exist.");
        if (DfsStreamPath.GetStreamName(path) is { } stream && !res.Target!.Streams.ContainsKey(stream))
            throw Fail(DfsErrorCode.FileNotFound, $"Stream '{path}' does not exist.");
        return (res, res.Target!, path);
    }

    private static (VolumeState, VolumeApplyResult) ApplySetPolicy(VolumeState s, ApplyContext ctx, SetPolicyCommand c)
        => WithStagingClosed(s, ctx, c.LeaderStaging, state => SetPolicyCore(state, ctx, c));

    private static (VolumeState, VolumeApplyResult) SetPolicyCore(VolumeState s, ApplyContext ctx, SetPolicyCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "set storage policy");
        (Resolution res, InodeRecord inode, _) = ResolveExisting(s, ctx, c.TargetPath);
        Require(ctx, res.ChainNearestFirst(), DfsFileSystemRights.WriteAttributes, $"set the storage policy of '{c.TargetPath}'");
        ImmutableSortedDictionary<string, StreamRecord> streams = inode.Streams;
        foreach (KeyValuePair<string, StreamRecord> pair in inode.Streams)
        {
            StreamRecord stored = MaterializedStream(s, c.Materialized, c.LeaderStaging, pair.Key, pair.Value);
            if (!ReferenceEquals(stored, pair.Value))
                streams = streams.SetItem(pair.Key, stored with { Revision = ctx.Index });
        }

        InodeRecord updated = TouchMetadata(inode with { PolicyName = c.PolicyName, Streams = streams }, ctx);
        s = Put(s, updated);
        return (s, VolumeApplyResult.Ok(new SetPolicyResultMessage(new InodeId(updated.Id), updated.ReportedRevision, s.TokenAt(ctx.Index))));
    }

    private static (VolumeState, VolumeApplyResult) ApplySetAcl(VolumeState s, ApplyContext ctx, SetAclCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "change permissions");
        (Resolution res, InodeRecord inode, string path) = ResolveExisting(s, ctx, c.TargetPath);
        IReadOnlyList<InodeRecord> chain = res.ChainNearestFirst();
        DfsSecurityDescriptor next;
        if (c.Change == AclChangeKind.AccessFlags)
        {
            // The SetAccess shortcut: only the owner (or an administrator) may change the access flags.
            OperationHeader op = ctx.Op!;
            bool isOwner = op.IdentitySet.Contains(inode.Owner) || (inode.Owner == DfsIdentity.Administrators && op.IsAdmin);
            if (!isOwner && !op.IsAdmin)
                Require(ctx, chain, DfsFileSystemRights.ChangePermissions, $"change the access flags of '{path}'");
            next = DfsSecurityDescriptor.FromAccessFlags(inode.Owner, c.OwnerOnly, c.ReadOnlyForOthers);
        }
        else
        {
            Require(ctx, chain, DfsFileSystemRights.ChangePermissions, $"change the permissions of '{path}'");
            DfsSecurityDescriptor supplied = c.Descriptor ?? throw Fail(DfsErrorCode.InvalidArgument, "A descriptor is required.");
            var explicitOnly = new DfsSecurityDescriptor(inode.Owner, supplied.Rules.Where(r => r is not null && !r.IsInherited).ToArray(),
                supplied.AreAccessRulesProtected);
            if (explicitOnly.GetValidationError() is { } error)
                throw Fail(DfsErrorCode.InvalidArgument, "Invalid security descriptor: " + error);
            next = Canonical(explicitOnly);
        }

        InodeRecord updated = TouchMetadata(inode with { Security = next }, ctx);
        s = Put(s, updated);
        IWireMessage reply = c.Change == AclChangeKind.AccessFlags
            ? new SetAccessResultMessage(new InodeId(updated.Id), updated.ReportedRevision, s.TokenAt(ctx.Index))
            : new SetAccessControlResultMessage(new InodeId(updated.Id), updated.ReportedRevision, s.TokenAt(ctx.Index));
        return (s, VolumeApplyResult.Ok(reply));
    }

    /// <summary>Explicit entries in canonical order: Deny before Allow, otherwise stored order (ACL-04).</summary>
    internal static DfsSecurityDescriptor Canonical(DfsSecurityDescriptor descriptor)
    {
        DfsAccessRule[] rules = [.. descriptor.Rules.Where(r => r.Type == DfsAccessControlType.Deny), .. descriptor.Rules.Where(r => r.Type != DfsAccessControlType.Deny)];
        return new DfsSecurityDescriptor(descriptor.Owner, rules.Select(r => r with { IsInherited = false, InheritedFrom = null }).ToArray(),
            descriptor.AreAccessRulesProtected);
    }

    private static (VolumeState, VolumeApplyResult) ApplySetOwner(VolumeState s, ApplyContext ctx, SetOwnerCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "take ownership");
        if (c.Owner.IsDefault || c.Owner == DfsIdentity.CreatorOwner)
            throw Fail(DfsErrorCode.InvalidArgument, "CreatorOwner (or an empty identity) cannot own an inode.");
        (Resolution res, InodeRecord inode, string path) = ResolveExisting(s, ctx, c.TargetPath);
        OperationHeader op = ctx.Op!;
        if (!op.IsAdmin)
        {
            Require(ctx, res.ChainNearestFirst(), DfsFileSystemRights.TakeOwnership, $"take ownership of '{path}'");
            if (c.Owner != op.User)
                throw Fail(DfsErrorCode.AccessDenied, $"Only administrators may make another identity ({c.Owner}) the owner of '{path}'.");
        }

        InodeRecord updated = TouchMetadata(inode with { Security = inode.Security with { Owner = c.Owner } }, ctx);
        s = Put(s, updated);
        return (s, VolumeApplyResult.Ok(new SetOwnerResultMessage(new InodeId(updated.Id), updated.ReportedRevision, s.TokenAt(ctx.Index))));
    }
}
