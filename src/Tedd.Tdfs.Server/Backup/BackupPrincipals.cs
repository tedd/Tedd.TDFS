using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// The backup node's own principals and grants (FSD §12.4, §14: backups have separate authorization; source principals are
/// unknown here). Verifiers use the SCRAM key schedule of the handshake (PROTOCOL.md §3) with salts derived from the node's
/// identity seed, so they are stable across restarts.
/// </summary>
/// <remarks>
/// Rights on a backup host: every principal holding any backup grant may read published snapshots (list, open, read) — the
/// reader, the export principal (it supplied the data) and the retention administrator; <see cref="DfsPermissions.BackupExport"/>
/// allows the export protocol and extending retention; <see cref="DfsPermissions.RetentionAdmin"/> allows changing retention
/// in both directions and deleting unprotected snapshots. Nothing allows mutating snapshot contents.
/// </remarks>
internal sealed class BackupPrincipals
{
    private readonly SortedDictionary<string, Entry> _principals = new(StringComparer.Ordinal);
    private readonly byte[] _saltKey;

    public BackupPrincipals(BackupNodeOptions options, byte[] saltKey)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(saltKey);
        _saltKey = saltKey;
        foreach (BootstrapPrincipal p in options.Principals)
        {
            if (string.IsNullOrEmpty(p.Name) || string.IsNullOrEmpty(p.Secret))
                throw new ArgumentException("Backup principals need a name and a secret.", nameof(options));
            if (_principals.ContainsKey(p.Name))
                throw new ArgumentException($"Backup principal '{p.Name}' is listed twice.", nameof(options));
            DfsPermissions grants = options.Grants.TryGetValue(p.Name, out DfsPermissions g) ? g : DfsPermissions.None;
            PrincipalVerifier verifier = HandshakeAuth.CreateVerifier(p.Secret, Salt("principal-salt", p.Name), HandshakeAuth.DefaultIterations);
            _principals[p.Name] = new Entry(p.Secret, grants, verifier);
        }
    }

    /// <summary>The verifier of a principal, or null when unknown.</summary>
    public PrincipalVerifier? Find(string principal) => _principals.TryGetValue(principal, out Entry? e) ? e.Verifier : null;

    /// <summary>The verifier answered for an unknown principal (existence is not disclosed; no proof matches it).</summary>
    public PrincipalVerifier FakeVerifier(string principal)
    {
        byte[] stored = HMACSHA256.HashData(_saltKey, Encoding.UTF8.GetBytes("tdfs/fake-stored-key/v1\0" + principal));
        byte[] server = HMACSHA256.HashData(_saltKey, Encoding.UTF8.GetBytes("tdfs/fake-server-key/v1\0" + principal));
        return new PrincipalVerifier(HandshakeAuth.FakeSalt(_saltKey, principal), HandshakeAuth.DefaultIterations, stored, server);
    }

    /// <summary>The grants of a principal.</summary>
    public DfsPermissions Grants(string? principal)
        => principal is not null && _principals.TryGetValue(principal, out Entry? e) ? e.Grants : DfsPermissions.None;

    /// <summary>The configured secret of a principal (the export principal's secret keys the manifest transit encryption).</summary>
    public string? SecretOf(string principal) => _principals.TryGetValue(principal, out Entry? e) ? e.Secret : null;

    /// <summary>True when the principal may read published snapshots.</summary>
    public bool CanRead(string? principal)
        => (Grants(principal) & (DfsPermissions.Read | DfsPermissions.BackupExport | DfsPermissions.RetentionAdmin | DfsPermissions.BackupAdmin)) != 0;

    /// <summary>Requires read access.</summary>
    public void RequireRead(string? principal, string what)
    {
        if (!CanRead(principal))
            throw new UnauthorizedAccessException($"Principal '{principal}' may not {what} on this backup host.");
    }

    /// <summary>Requires the export grant.</summary>
    public void RequireExport(string? principal)
    {
        if ((Grants(principal) & DfsPermissions.BackupExport) == 0)
            throw new UnauthorizedAccessException($"Principal '{principal}' may not export to this backup host.");
    }

    /// <summary>True for retention administrators.</summary>
    public bool IsRetentionAdmin(string? principal) => (Grants(principal) & DfsPermissions.RetentionAdmin) != 0;

    /// <summary>True for export principals.</summary>
    public bool IsExporter(string? principal) => (Grants(principal) & DfsPermissions.BackupExport) != 0;

    private byte[] Salt(string purpose, string principal)
    {
        byte[] mac = HMACSHA256.HashData(_saltKey, Encoding.UTF8.GetBytes("tdfs/" + purpose + "/v1\0" + principal));
        return mac.AsSpan(0, HandshakeAuth.SaltSize).ToArray();
    }

    private sealed record Entry(string Secret, DfsPermissions Grants, PrincipalVerifier Verifier);
}
