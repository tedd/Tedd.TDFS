namespace Tedd.Tdfs.Testing;

/// <summary>
/// An emulated server build used by rolling-upgrade scenarios (CHA-011): the protocol-version window it speaks and the
/// feature set it understands. Applied with <see cref="TestNode.RestartWithSoftwareAsync"/>.
/// </summary>
/// <remarks>
/// <see cref="Current"/> and <see cref="Next"/> each return one cached instance, and <see cref="TestNode.SoftwareVersion"/>
/// returns the instance the node was (re)started with, so reference/record equality is stable; tests compare
/// <see cref="Name"/> where value semantics matter (record equality compares <see cref="Features"/> by reference).
/// </remarks>
/// <param name="Name">Display name ("current", "next").</param>
/// <param name="MinProtocolVersion">Lowest protocol version the build accepts.</param>
/// <param name="MaxProtocolVersion">Highest protocol version the build speaks.</param>
/// <param name="Features">Feature names the build understands (ordinal).</param>
public sealed record TestSoftwareVersion(string Name, int MinProtocolVersion, int MaxProtocolVersion, IReadOnlySet<string> Features)
{
    /// <summary>
    /// A feature understood only by <see cref="Next"/>. Activating it (<c>DfsAdminClient.SetFeatureAsync</c>) changes the
    /// replicated/on-disk format so that <see cref="Current"/> builds can no longer interpret new state; activation must
    /// therefore wait until every member (voters, learners and data nodes) runs a build that supports it (FMT-009).
    /// </summary>
    public const string NextFeature = "test.next-format";

    /// <summary>
    /// The build under test: the protocol window and feature set the product advertises by default (every node starts as
    /// this version).
    /// </summary>
    public static TestSoftwareVersion Current => throw Stub.NotYet("TestSoftwareVersion.Current");

    /// <summary>
    /// The next build: <see cref="MinProtocolVersion"/> equal to <see cref="Current"/>'s (backward compatible window),
    /// <see cref="MaxProtocolVersion"/> one higher, and <see cref="Current"/>'s features plus <see cref="NextFeature"/>.
    /// Mixed clusters of <see cref="Current"/> and <see cref="Next"/> negotiate <see cref="Current"/>'s protocol until every
    /// member runs <see cref="Next"/>.
    /// </summary>
    public static TestSoftwareVersion Next => throw Stub.NotYet("TestSoftwareVersion.Next");
}
