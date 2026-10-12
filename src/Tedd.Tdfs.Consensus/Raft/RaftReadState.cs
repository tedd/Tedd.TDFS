namespace Tedd.Tdfs.Consensus;

/// <summary>A confirmed ReadIndex: the read may be served once the applied index reaches <paramref name="Index"/>.</summary>
/// <param name="Index">Commit index captured when leadership was confirmed.</param>
/// <param name="RequestContext">Caller-supplied context passed to <see cref="RaftNode.ReadIndex"/>.</param>
public sealed record RaftReadState(ulong Index, ulong RequestContext);
