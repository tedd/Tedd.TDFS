namespace Tedd.Tdfs.Testing.SnpApp;

/// <summary>
/// Independent oracle of a volume's file contents as a function of commit position (SNP-001, SNP-009, APP-010/011).
/// Tests record every successfully committed mutation with the <see cref="CommitToken"/> it returned; the oracle then
/// answers which files and bytes a snapshot with a given boundary must contain: exactly the effects of all commits with
/// sequence at or before the boundary, applied in sequence order. One commit may write several paths atomically
/// (transactions, moves).
/// </summary>
/// <remarks>
/// Only use for histories without unknown outcomes (every recorded commit definitely happened). Thread-safe.
/// </remarks>
public sealed class CommitHistoryOracle
{
    private readonly object _lock = new();
    private readonly SortedDictionary<string, byte[]> _initial = new(StringComparer.Ordinal);
    private readonly SortedDictionary<ulong, (CommitToken Token, SortedDictionary<string, byte[]?> Writes)> _commits = [];

    /// <summary>Creates the oracle with the state before the first recorded commit (files only).</summary>
    public CommitHistoryOracle(IReadOnlyDictionary<string, byte[]>? initial = null)
    {
        if (initial is null)
            return;
        foreach (KeyValuePair<string, byte[]> pair in initial)
            _initial[pair.Key] = (byte[])pair.Value.Clone();
    }

    /// <summary>Number of distinct recorded commits.</summary>
    public int Count
    {
        get
        {
            lock (_lock)
                return _commits.Count;
        }
    }

    /// <summary>Records that commit <paramref name="token"/> set <paramref name="path"/> to <paramref name="content"/> (null = deleted).</summary>
    public void Record(CommitToken token, string path, byte[]? content) =>
        Record(token, new Dictionary<string, byte[]?>(StringComparer.Ordinal) { [path] = content });

    /// <summary>Records an atomic multi-path commit (null values = deleted).</summary>
    /// <exception cref="ArgumentException">
    /// None token, a token from another history than earlier records, or the same sequence recorded with contradicting
    /// contents for a path.
    /// </exception>
    public void Record(CommitToken token, IReadOnlyDictionary<string, byte[]?> writes)
    {
        ArgumentNullException.ThrowIfNull(writes);
        if (token.IsNone)
            throw new ArgumentException("A committed mutation must have a commit token.", nameof(token));
        lock (_lock)
        {
            foreach ((CommitToken existing, _) in _commits.Values)
            {
                if (!existing.IsComparableWith(token))
                    throw new ArgumentException($"Token {token} is not from the same history as {existing}.", nameof(token));
                break;
            }

            if (!_commits.TryGetValue(token.Sequence, out var entry))
            {
                entry = (token, new SortedDictionary<string, byte[]?>(StringComparer.Ordinal));
                _commits[token.Sequence] = entry;
            }

            foreach (KeyValuePair<string, byte[]?> write in writes)
            {
                if (entry.Writes.TryGetValue(write.Key, out byte[]? prior)
                    && !(prior is null && write.Value is null)
                    && !(prior is not null && write.Value is not null && prior.AsSpan().SequenceEqual(write.Value)))
                {
                    throw new ArgumentException($"Commit {token} recorded twice with different contents for '{write.Key}'.", nameof(writes));
                }

                entry.Writes[write.Key] = write.Value is null ? null : (byte[])write.Value.Clone();
            }
        }
    }

    /// <summary>The files (path → bytes) that exist at <paramref name="boundary"/> among the tracked paths.</summary>
    /// <exception cref="ArgumentException">The boundary is not comparable with the recorded tokens.</exception>
    public SortedDictionary<string, byte[]> StateAt(CommitToken boundary)
    {
        lock (_lock)
        {
            var state = new SortedDictionary<string, byte[]>(_initial, StringComparer.Ordinal);
            foreach ((CommitToken token, SortedDictionary<string, byte[]?> writes) in _commits.Values)
            {
                if (token.CompareTo(boundary) > 0)
                    break;
                foreach (KeyValuePair<string, byte[]?> write in writes)
                {
                    if (write.Value is null)
                        state.Remove(write.Key);
                    else
                        state[write.Key] = write.Value;
                }
            }

            return state;
        }
    }

    /// <summary>The latest recorded commit at or before <paramref name="boundary"/>, or null.</summary>
    public CommitToken? LatestAtOrBefore(CommitToken boundary)
    {
        lock (_lock)
        {
            CommitToken? latest = null;
            foreach ((CommitToken token, _) in _commits.Values)
            {
                if (token.CompareTo(boundary) > 0)
                    break;
                latest = token;
            }

            return latest;
        }
    }

    /// <summary>Every path the oracle tracks (initial or written by any recorded commit).</summary>
    public IReadOnlySet<string> TrackedPaths
    {
        get
        {
            lock (_lock)
            {
                var paths = new SortedSet<string>(_initial.Keys, StringComparer.Ordinal);
                foreach ((_, SortedDictionary<string, byte[]?> writes) in _commits.Values)
                    paths.UnionWith(writes.Keys);
                return paths;
            }
        }
    }

    /// <summary>
    /// Fails the test unless the files in <paramref name="actualFiles"/> restricted to the tracked paths equal the oracle
    /// state at <paramref name="boundary"/> (same set of existing files, byte-identical contents).
    /// </summary>
    public void AssertMatches(CommitToken boundary, IReadOnlyDictionary<string, byte[]> actualFiles, string because)
    {
        ArgumentNullException.ThrowIfNull(actualFiles);
        IReadOnlySet<string> tracked = TrackedPaths;
        SortedDictionary<string, byte[]> expected = StateAt(boundary);
        var actual = actualFiles.Where(p => tracked.Contains(p.Key)).ToDictionary(p => p.Key, p => (byte[]?)p.Value, StringComparer.Ordinal);
        SnapshotTree.AssertEqual(expected.ToDictionary(p => p.Key, p => (byte[]?)p.Value, StringComparer.Ordinal), actual,
            $"{because} (boundary {boundary}, latest recorded commit at or before it: {LatestAtOrBefore(boundary)?.ToString() ?? "none"})");
    }
}
