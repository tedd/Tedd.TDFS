using System.Text;

namespace Tedd.Tdfs;

/// <summary>
/// A security principal named in owners and access control entries (Addendum A, ACL-01). Text forms:
/// <c>user:&lt;name&gt;</c>, <c>group:&lt;name&gt;</c>, or one of the well-known names <c>Everyone</c>,
/// <c>AuthenticatedUsers</c>, <c>Administrators</c>, <c>CreatorOwner</c>. Prefixes and well-known names are
/// case-sensitive; names compare ordinally.
/// </summary>
/// <remarks>
/// User and group names must be non-empty, at most <see cref="MaxNameBytes"/> UTF-8 bytes, well-formed UTF-16, and
/// must not contain control characters or <c>':'</c> (so the text form is unambiguous). Names are not Unicode-normalized:
/// a user identity names a principal exactly as the principal was created. The constructor validates, so every
/// non-default instance is valid; <c>default(DfsIdentity)</c> is not a valid identity (see <see cref="IsDefault"/>).
/// </remarks>
/// <param name="Kind">Identity kind.</param>
/// <param name="Name">User or group name, or the well-known name.</param>
public readonly record struct DfsIdentity(DfsIdentityKind Kind, string Name)
{
    /// <summary>Text prefix of user identities.</summary>
    public const string UserPrefix = "user:";

    /// <summary>Text prefix of group identities.</summary>
    public const string GroupPrefix = "group:";

    /// <summary>Maximum UTF-8 bytes of a user or group name.</summary>
    public const int MaxNameBytes = 255;

    /// <summary>Well-known name of <see cref="Everyone"/>.</summary>
    public const string EveryoneName = "Everyone";

    /// <summary>Well-known name of <see cref="AuthenticatedUsers"/>.</summary>
    public const string AuthenticatedUsersName = "AuthenticatedUsers";

    /// <summary>Well-known name of <see cref="Administrators"/>.</summary>
    public const string AdministratorsName = "Administrators";

    /// <summary>Well-known name of <see cref="CreatorOwner"/>.</summary>
    public const string CreatorOwnerName = "CreatorOwner";

    /// <summary>Identity kind.</summary>
    public DfsIdentityKind Kind { get; } = Kind;

    /// <summary>User or group name, or the well-known name (null only for <c>default</c>).</summary>
    public string Name { get; } = ValidateName(Kind, Name);

    /// <summary>Every caller.</summary>
    public static DfsIdentity Everyone { get; } = new(DfsIdentityKind.WellKnown, EveryoneName);

    /// <summary>Every authenticated principal (in TDFS every caller is authenticated).</summary>
    public static DfsIdentity AuthenticatedUsers { get; } = new(DfsIdentityKind.WellKnown, AuthenticatedUsersName);

    /// <summary>Principals with the ClusterAdmin permission.</summary>
    public static DfsIdentity Administrators { get; } = new(DfsIdentityKind.WellKnown, AdministratorsName);

    /// <summary>Inheritance template: in an inherited entry it stands for the inheriting object's owner (ACL-03).</summary>
    public static DfsIdentity CreatorOwner { get; } = new(DfsIdentityKind.WellKnown, CreatorOwnerName);

    /// <summary>True for <c>default(DfsIdentity)</c>, which is not a valid identity.</summary>
    public bool IsDefault => Name is null;

    /// <summary>True for <see cref="DfsIdentityKind.WellKnown"/> identities.</summary>
    public bool IsWellKnown => Kind == DfsIdentityKind.WellKnown && Name is not null;

    /// <summary>The user identity <c>user:<paramref name="name"/></c>.</summary>
    /// <exception cref="ArgumentException">The name is invalid.</exception>
    public static DfsIdentity User(string name) => new(DfsIdentityKind.User, name);

    /// <summary>The group identity <c>group:<paramref name="name"/></c>.</summary>
    /// <exception cref="ArgumentException">The name is invalid.</exception>
    public static DfsIdentity Group(string name) => new(DfsIdentityKind.Group, name);

    /// <summary>Parses the text form produced by <see cref="ToString"/>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="s"/> is null.</exception>
    /// <exception cref="FormatException">The text is not a valid identity.</exception>
    public static DfsIdentity Parse(string s)
    {
        ArgumentNullException.ThrowIfNull(s);
        string? error = TryParseCore(s, out DfsIdentity id);
        return error is null ? id : throw new FormatException(error);
    }

    /// <summary>Tries to parse the text form produced by <see cref="ToString"/>.</summary>
    public static bool TryParse(string? s, out DfsIdentity id)
    {
        id = default;
        return s is not null && TryParseCore(s, out id) is null;
    }

    /// <summary>True when <paramref name="name"/> is a valid user or group name.</summary>
    public static bool IsValidName(string? name) => name is not null && GetNameError(name) is null;

    /// <summary>Returns <c>user:name</c>, <c>group:name</c> or the well-known name (empty for <c>default</c>).</summary>
    public override string ToString() => Name is null
        ? string.Empty
        : Kind switch
        {
            DfsIdentityKind.User => UserPrefix + Name,
            DfsIdentityKind.Group => GroupPrefix + Name,
            _ => Name,
        };

    private static string? TryParseCore(string s, out DfsIdentity id)
    {
        id = default;
        DfsIdentityKind kind;
        string name;
        if (s.StartsWith(UserPrefix, StringComparison.Ordinal))
        {
            kind = DfsIdentityKind.User;
            name = s[UserPrefix.Length..];
        }
        else if (s.StartsWith(GroupPrefix, StringComparison.Ordinal))
        {
            kind = DfsIdentityKind.Group;
            name = s[GroupPrefix.Length..];
        }
        else if (IsWellKnownName(s))
        {
            id = new DfsIdentity(DfsIdentityKind.WellKnown, s);
            return null;
        }
        else
        {
            return $"'{s}' is not an identity; expected 'user:<name>', 'group:<name>', '{EveryoneName}', '{AuthenticatedUsersName}', '{AdministratorsName}' or '{CreatorOwnerName}' (case-sensitive).";
        }

        string? error = GetNameError(name);
        if (error is not null)
            return error;
        id = new DfsIdentity(kind, name);
        return null;
    }

    private static bool IsWellKnownName(string name) =>
        name is EveryoneName or AuthenticatedUsersName or AdministratorsName or CreatorOwnerName;

    private static string ValidateName(DfsIdentityKind kind, string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        switch (kind)
        {
            case DfsIdentityKind.User:
            case DfsIdentityKind.Group:
                string? error = GetNameError(name);
                return error is null ? name : throw new ArgumentException(error, nameof(name));
            case DfsIdentityKind.WellKnown:
                return IsWellKnownName(name)
                    ? name
                    : throw new ArgumentException($"'{name}' is not a well-known identity name (case-sensitive).", nameof(name));
            default:
                throw new ArgumentException($"Undefined identity kind {(int)kind}.", nameof(kind));
        }
    }

    private static string? GetNameError(string name)
    {
        if (name.Length == 0)
            return "A user or group name must not be empty.";
        if (name.Length > MaxNameBytes)
            return $"A user or group name must be at most {MaxNameBytes} UTF-8 bytes.";
        foreach (char c in name)
        {
            if (char.IsControl(c))
                return "A user or group name must not contain control characters.";
            if (c == ':')
                return "A user or group name must not contain ':'.";
        }

        if (DfsUnicode.IndexOfUnpairedSurrogate(name) >= 0)
            return "A user or group name must be well-formed Unicode (it contains an unpaired surrogate).";
        if (Encoding.UTF8.GetByteCount(name) > MaxNameBytes)
            return $"A user or group name must be at most {MaxNameBytes} UTF-8 bytes.";
        return null;
    }
}
