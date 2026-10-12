namespace Tedd.Tdfs.Server;

/// <summary>
/// Static bootstrap description, identical on every initial node (or a single-member description for seed-join
/// bootstrap).
/// </summary>
/// <param name="ClusterId">Cluster id fixed at bootstrap.</param>
/// <param name="ClusterName">Cluster name.</param>
/// <param name="Members">Initial members.</param>
/// <param name="ControlVoters">Names of the initial control-group voters.</param>
/// <param name="Principals">Initial principals.</param>
public sealed record ClusterBootstrap(Guid ClusterId, string ClusterName, IReadOnlyList<BootstrapMember> Members,
    IReadOnlyList<string> ControlVoters, IReadOnlyList<BootstrapPrincipal> Principals);
