namespace Tedd.Tdfs.Server;

/// <summary>An initial principal created at bootstrap.</summary>
/// <param name="Name">Principal name.</param>
/// <param name="Secret">Principal secret (stored only as a salted hash).</param>
/// <param name="ClusterAdmin">True to grant cluster administration.</param>
public sealed record BootstrapPrincipal(string Name, string Secret, bool ClusterAdmin);
