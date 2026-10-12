namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>Three-valued availability expectation computed by <see cref="MeshOracle"/>.</summary>
public enum MeshTri
{
    /// <summary>The specification requires the operation to succeed (eventually, within a bounded settle window).</summary>
    Must,

    /// <summary>The specification permits both success and a typed availability failure; only safety is asserted.</summary>
    Either,

    /// <summary>No correct implementation can succeed; the operation must fail with a typed exception (never fabricated success).</summary>
    Never,
}

/// <summary>Why an operation may or must fail; selects the exception types a failure is allowed to surface as.</summary>
public enum MeshCause
{
    /// <summary>
    /// No connected set of live nodes holds a majority of the volume group's <b>configured</b> voters while the client
    /// reaches at least one live voter: <see cref="DfsQuorumUnavailableException"/> (mutations may instead report
    /// <see cref="DfsCommitOutcomeUnknownException"/>).
    /// </summary>
    MetadataQuorum,

    /// <summary>As <see cref="MetadataQuorum"/> for the control group (membership/principal/policy changes).</summary>
    ControlQuorum,

    /// <summary>
    /// The client cannot exchange messages with the side that holds (or could hold) the quorum, or with any live voter:
    /// any <see cref="DfsUnavailableException"/> (mutations may instead report <see cref="DfsCommitOutcomeUnknownException"/>).
    /// </summary>
    Unreachable,

    /// <summary>
    /// Fewer distinct hosts/zones than the acknowledgement policy requires are usable:
    /// <see cref="DfsPlacementUnavailableException"/> with the policy's required counts and smaller achieved counts. The
    /// leader validates receipts before proposing, so the outcome is definite (never unknown).
    /// </summary>
    Placement,

    /// <summary>No verified copy of a referenced object is reachable: <see cref="DfsUnavailableException"/> or <see cref="DfsIntegrityException"/>, never invented bytes.</summary>
    NoHealthyReplica,
}

/// <summary>An operation probed by <see cref="MeshScenario"/> through the public SDK.</summary>
public enum MeshProbeKind
{
    /// <summary><c>WriteAllBytesAsync</c> of a small file (inline when the metadata group's durability satisfies the policy).</summary>
    WriteSmall,

    /// <summary><c>WriteAllBytesAsync</c> of a multi-chunk file (always extent-backed).</summary>
    WriteLarge,

    /// <summary><c>ReadAllBytesAsync</c> of a small file written before the fault.</summary>
    ReadSmall,

    /// <summary><c>ReadAllBytesAsync</c> of a multi-chunk file written before the fault.</summary>
    ReadLarge,

    /// <summary><c>AppendAllBytesAsync</c> of a 16-byte entry to a per-client log.</summary>
    Append,

    /// <summary>A control-group mutation (<c>DfsAdminClient.CreatePrincipalAsync</c>).</summary>
    ControlChange,
}

/// <summary>An availability expectation with the failure causes that are legal when the operation fails.</summary>
/// <param name="Outcome">Required outcome class.</param>
/// <param name="Causes">Causes a failure may be attributed to (empty for <see cref="MeshTri.Must"/>).</param>
/// <param name="Reason">Human-readable derivation (quorum arithmetic, host/zone counts) for assertion messages.</param>
public sealed record MeshExpectation(MeshTri Outcome, IReadOnlySet<MeshCause> Causes, string Reason)
{
    /// <summary>A required success.</summary>
    public static MeshExpectation Must(string reason) => new(MeshTri.Must, new HashSet<MeshCause>(), reason);

    /// <summary>A required typed failure.</summary>
    public static MeshExpectation Never(MeshCause cause, string reason) => new(MeshTri.Never, new HashSet<MeshCause> { cause }, reason);

    /// <summary>Success or a typed failure attributed to one of <paramref name="causes"/>.</summary>
    public static MeshExpectation Either(IEnumerable<MeshCause> causes, string reason) => new(MeshTri.Either, causes.ToHashSet(), reason);

    /// <summary>
    /// Combines the expectations of the components an operation needs (metadata quorum, data placement, replica
    /// reachability): Never when any component is Never, Must when all are Must, otherwise Either. The legal failure
    /// causes are those of every component that is not Must (a client may hit any of them first).
    /// </summary>
    public static MeshExpectation Combine(params MeshExpectation[] parts)
    {
        ArgumentNullException.ThrowIfNull(parts);
        if (parts.Length == 0)
            throw new ArgumentException("At least one component is required.", nameof(parts));
        var causes = new HashSet<MeshCause>();
        foreach (MeshExpectation part in parts.Where(p => p.Outcome != MeshTri.Must))
            causes.UnionWith(part.Causes);
        string reason = string.Join(" & ", parts.Select(p => $"{p.Outcome}({p.Reason})"));
        if (parts.Any(p => p.Outcome == MeshTri.Never))
            return new MeshExpectation(MeshTri.Never, causes, reason);
        if (parts.All(p => p.Outcome == MeshTri.Must))
            return Must(reason);
        return new MeshExpectation(MeshTri.Either, causes, reason);
    }

    /// <inheritdoc />
    public override string ToString() =>
        Outcome + (Causes.Count == 0 ? string.Empty : "[" + string.Join(",", Causes.OrderBy(c => c)) + "]") + ": " + Reason;
}
