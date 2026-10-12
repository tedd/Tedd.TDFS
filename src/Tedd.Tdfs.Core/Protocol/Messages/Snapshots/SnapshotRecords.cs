namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>Identity and restore requirements of an application snapshot adapter recorded with a snapshot (FSD §11.3).</summary>
/// <param name="Id">Adapter id.</param>
/// <param name="Version">Adapter version.</param>
/// <param name="RestoreRequirements">Restore requirements declared by the adapter (ordinal keys).</param>
public sealed record AdapterRecordData(
    string Id,
    string Version,
    IReadOnlyDictionary<string, string> RestoreRequirements) : IWireRecord<AdapterRecordData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 4;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Id);
        writer.WriteString(Version);
        writer.WriteStringMap(RestoreRequirements);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdapterRecordData Read(ref WireReader reader)
    {
        string id = reader.ReadString();
        string version = reader.ReadString();
        IReadOnlyDictionary<string, string> requirements = reader.ReadStringMap();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdapterRecordData(id, version, requirements) { Extensions = extensions };
    }
}

/// <summary>Description of a snapshot (live volume or published backup snapshot).</summary>
/// <param name="Id">Snapshot identity.</param>
/// <param name="Name">Snapshot name (unique per volume).</param>
/// <param name="Volume">Volume name.</param>
/// <param name="Boundary">Commit boundary captured by the snapshot (the entry that created it).</param>
/// <param name="Label">Consistency label.</param>
/// <param name="Adapters">Adapters whose prepare succeeded (required for <see cref="DfsConsistencyLabel.ApplicationConsistent"/>).</param>
/// <param name="CreatedUtc">Creation time (leader wall clock carried in the log entry).</param>
/// <param name="Retention">Retention (expiry and hold).</param>
public sealed record SnapshotInfoData(
    SnapshotId Id,
    string Name,
    string Volume,
    CommitToken Boundary,
    DfsConsistencyLabel Label,
    IReadOnlyList<AdapterRecordData> Adapters,
    DateTimeOffset CreatedUtc,
    DfsRetention Retention) : IWireRecord<SnapshotInfoData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 16 + 1 + 1 + 34 + 1 + 1 + 1 + 2 + 1;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteSnapshotId(Id);
        writer.WriteString(Name);
        writer.WriteString(Volume);
        writer.WriteCommitToken(Boundary);
        writer.WriteDefinedEnum(Label);
        writer.WriteList(Adapters);
        writer.WriteDateTimeOffset(CreatedUtc);
        writer.WriteRetention(Retention);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SnapshotInfoData Read(ref WireReader reader)
    {
        SnapshotId id = reader.ReadSnapshotId();
        string name = reader.ReadString();
        string volume = reader.ReadString();
        CommitToken boundary = reader.ReadCommitToken();
        DfsConsistencyLabel label = reader.ReadEnum<DfsConsistencyLabel>();
        IReadOnlyList<AdapterRecordData> adapters = reader.ReadList<AdapterRecordData>();
        DateTimeOffset created = reader.ReadDateTimeOffset();
        DfsRetention retention = reader.ReadRetention();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SnapshotInfoData(id, name, volume, boundary, label, adapters, created, retention) { Extensions = extensions };
    }
}
