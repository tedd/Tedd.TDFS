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
    IReadOnlyList<string> ControlVoters, IReadOnlyList<BootstrapPrincipal> Principals)
{
    /// <summary>
    /// Number of control voters the control leader promotes learners to as members join (ARCHITECTURE §2.4/2.5: 3 or 5,
    /// never more than the live members). Null: the number of <see cref="ControlVoters"/>, or for a single-member seed-join
    /// bootstrap the default of three.
    /// </summary>
    public int? ControlVoterTarget { get; init; }
}
