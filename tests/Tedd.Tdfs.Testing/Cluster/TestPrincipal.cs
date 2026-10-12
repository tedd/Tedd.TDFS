namespace Tedd.Tdfs.Testing;

/// <summary>A principal created by <see cref="TestCluster.CreatePrincipalAsync"/>.</summary>
/// <param name="Name">Principal name.</param>
/// <param name="Credentials">Credentials for connecting as this principal.</param>
public sealed record TestPrincipal(string Name, DfsCredentials Credentials);
