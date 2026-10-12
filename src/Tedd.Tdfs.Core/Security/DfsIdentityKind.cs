namespace Tedd.Tdfs;

/// <summary>Kind of a <see cref="DfsIdentity"/>.</summary>
public enum DfsIdentityKind
{
    /// <summary>A principal (user), written <c>user:&lt;name&gt;</c>.</summary>
    User = 0,
    /// <summary>A control-group managed group, written <c>group:&lt;name&gt;</c>.</summary>
    Group = 1,
    /// <summary>A well-known identity: <c>Everyone</c>, <c>AuthenticatedUsers</c>, <c>Administrators</c> or <c>CreatorOwner</c>.</summary>
    WellKnown = 2,
}
