namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>
/// Sequential model of one file that is renamed back and forth between two names <c>a</c> and <c>b</c> of one
/// directory (atomic rename, NS-05). The state is the name currently holding the file.
/// </summary>
/// <remarks>
/// Kinds: <c>"move"</c> — input <c>"a&gt;b"</c> or <c>"b&gt;a"</c>; output <c>"moved"</c> (the rename happened, so the
/// state must have been the source) or <c>"absent"</c> (the server answered that the source does not exist, so the
/// state must have been the other name; no change). <c>"probe"</c> — output is the set of names observed in one pinned
/// directory listing encoded as <c>"a"</c>, <c>"b"</c>, <c>"none"</c> or <c>"both"</c>; only the current state is legal
/// (a torn rename would show "none" or "both"). Unknown-outcome moves may or may not have happened.
/// </remarks>
public sealed class MeshRenameModel : ISequentialModel<string>
{
    /// <inheritdoc />
    public string Initial => "a";

    /// <inheritdoc />
    public bool Step(string state, HistoryOp op, out string next)
    {
        ArgumentNullException.ThrowIfNull(op);
        next = state;
        switch (op.Kind)
        {
            case "move":
            {
                string input = op.Input as string ?? throw new ArgumentException($"Operation #{op.Id}: move input must be \"a>b\" or \"b>a\".");
                string from = input[..1];
                string to = input[^1..];
                if (op.Outcome != OpOutcome.Ok)
                {
                    // Unknown: if it took effect it was a successful rename.
                    if (state != from)
                        return false;
                    next = to;
                    return true;
                }

                switch (op.Output as string)
                {
                    case "moved":
                        if (state != from)
                            return false;
                        next = to;
                        return true;
                    case "absent":
                        return state != from;
                    default:
                        return false;
                }
            }

            case "probe":
                return op.Outcome != OpOutcome.Ok || string.Equals(op.Output as string, state, StringComparison.Ordinal);
            default:
                throw new ArgumentException($"Operation #{op.Id}: MeshRenameModel does not support kind '{op.Kind}'.");
        }
    }

    /// <inheritdoc />
    public string Key(string state) => state;
}
