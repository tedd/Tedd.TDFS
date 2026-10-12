namespace Tedd.Tdfs.Testing;

/// <summary>Builds the exception thrown by harness members that a later phase implements.</summary>
internal static class Stub
{
    /// <summary>Creates a <see cref="NotImplementedException"/> naming the member.</summary>
    public static NotImplementedException NotYet(string typeAndMember) =>
        new($"{typeAndMember}: implemented in a later phase");
}
