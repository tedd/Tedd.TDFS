namespace Tedd.Tdfs;

/// <summary>Cluster identity fixed at bootstrap; every handshake checks it.</summary>
/// <param name="ClusterId">Cluster GUID.</param>
/// <param name="Name">Human-readable cluster name.</param>
public readonly record struct ClusterIdentity(Guid ClusterId, string Name);
