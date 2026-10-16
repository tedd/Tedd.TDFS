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
    /// The configured number of control voters (ARCHITECTURE §2.4/2.5: 3 or 5, never more than the live members): the
    /// control leader promotes caught-up learners, one change at a time, until this many voters exist (seed join, nodes
    /// added later). Null keeps the defaults: the number of <see cref="ControlVoters"/> of a multi-member description, and
    /// <c>max(3, ControlVoters.Count)</c> for a single-member (seed-join) bootstrap. Never below the initial voter count.
    /// </summary>
    public int? ControlVoterTarget { get; init; }
}
