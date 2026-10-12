namespace Tedd.Tdfs;

/// <summary>A control-group managed group of principals (Addendum A, ACL-01).</summary>
/// <param name="Name">Group name (as in <c>group:&lt;name&gt;</c>).</param>
/// <param name="Members">Member principal names, ordinal-sorted.</param>
public sealed record DfsGroupInfo(string Name, IReadOnlyList<string> Members);
