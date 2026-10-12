namespace Tedd.Tdfs.Testing;

/// <summary>One named test case of the test plan catalog (section 7).</summary>
/// <param name="Id">Test id, for example "OPEN-001".</param>
/// <param name="Suite">Suite prefix, for example "OPEN".</param>
/// <param name="Level">Primary level: U, S, I, H, P or M.</param>
/// <param name="Name">Catalog name, for example "CreateNewAbsent".</param>
/// <param name="Assertion">The required assertion text.</param>
public sealed record CatalogEntry(string Id, string Suite, string Level, string Name, string Assertion)
{
    /// <summary>The conventional test method name, for example "OPEN_001_CreateNewAbsent".</summary>
    public string MethodName => Id.Replace('-', '_') + "_" + Name;
}
