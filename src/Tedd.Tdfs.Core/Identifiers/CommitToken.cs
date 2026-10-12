using System.Globalization;

namespace Tedd.Tdfs;

/// <summary>
/// Identifies an authoritative volume commit: cluster, volume, volume epoch and commit sequence (the Raft log index
/// of the applied entry). Tokens are only ordered within the same cluster, volume and epoch (OUT-018); comparing
/// tokens from different histories throws instead of comparing sequence integers.
/// String form: <c>ct:{cluster}/{volume}/{epoch}/{sequence}</c> (GUIDs in "D" form, numbers in decimal).
/// </summary>
/// <param name="ClusterId">Cluster identity.</param>
/// <param name="VolumeId">Volume identity.</param>
/// <param name="VolumeEpoch">Volume epoch (increments on restore/promotion).</param>
/// <param name="Sequence">Commit sequence (Raft log index) within the epoch.</param>
public readonly record struct CommitToken(Guid ClusterId, VolumeId VolumeId, ulong VolumeEpoch, ulong Sequence)
{
    /// <summary>String prefix of the canonical form.</summary>
    public const string Prefix = "ct:";

    /// <summary>The all-zero token, meaning "no commit".</summary>
    public static readonly CommitToken None;

    /// <summary>True when every component is zero.</summary>
    public bool IsNone => this == None;

    /// <summary>True when both tokens are non-None and belong to the same cluster, volume and epoch.</summary>
    public bool IsComparableWith(CommitToken other)
        => !IsNone && !other.IsNone && ClusterId == other.ClusterId && VolumeId == other.VolumeId && VolumeEpoch == other.VolumeEpoch;

    /// <summary>Compares commit positions within one history.</summary>
    /// <returns>Negative, zero or positive as this token is before, at, or after <paramref name="other"/>.</returns>
    /// <exception cref="ArgumentException">The tokens are not comparable (different cluster, volume or epoch, or None).</exception>
    public int CompareTo(CommitToken other)
    {
        if (!IsComparableWith(other))
            throw new ArgumentException(
                $"Commit tokens are not comparable: '{this}' and '{other}' do not belong to the same cluster, volume and epoch.",
                nameof(other));
        return Sequence.CompareTo(other.Sequence);
    }

    /// <summary>True when this token is at or after <paramref name="other"/> in the same history.</summary>
    /// <exception cref="ArgumentException">The tokens are not comparable.</exception>
    public bool IsAtOrAfter(CommitToken other) => CompareTo(other) >= 0;

    /// <summary>Returns <c>ct:{cluster}/{volume}/{epoch}/{sequence}</c>.</summary>
    public override string ToString()
        => string.Concat(
            Prefix,
            IdentifierFormat.FormatGuid(ClusterId), "/",
            IdentifierFormat.FormatGuid(VolumeId.Value), "/",
            VolumeEpoch.ToString(CultureInfo.InvariantCulture), "/",
            Sequence.ToString(CultureInfo.InvariantCulture));

    /// <summary>Parses the form produced by <see cref="ToString"/>.</summary>
    /// <exception cref="FormatException">The string is not a valid commit token.</exception>
    public static CommitToken Parse(string s)
    {
        ArgumentNullException.ThrowIfNull(s);
        return TryParse(s, out var token) ? token : throw new FormatException("Invalid commit token; expected 'ct:{cluster}/{volume}/{epoch}/{sequence}'.");
    }

    /// <summary>Tries to parse the form produced by <see cref="ToString"/>.</summary>
    public static bool TryParse(string? s, out CommitToken token)
    {
        token = default;
        if (s is null || !s.StartsWith(Prefix, StringComparison.Ordinal))
            return false;
        ReadOnlySpan<char> rest = s.AsSpan(Prefix.Length);
        Span<Range> parts = stackalloc Range[5];
        if (rest.Split(parts, '/') != 4)
            return false;
        if (!IdentifierFormat.TryParseGuid(rest[parts[0]], out var cluster) ||
            !IdentifierFormat.TryParseGuid(rest[parts[1]], out var volume) ||
            !IdentifierFormat.TryParseDecimal64(rest[parts[2]], out ulong epoch) ||
            !IdentifierFormat.TryParseDecimal64(rest[parts[3]], out ulong sequence))
            return false;
        token = new CommitToken(cluster, new VolumeId(volume), epoch, sequence);
        return true;
    }
}
