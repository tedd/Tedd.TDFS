using System.Collections;

namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Structured details of an <see cref="ErrorReplyMessage"/>, so the typed exception the SDK raises carries the same
/// properties the server's exception had (FMT-011, API.md §A.2): the copy/zone shortfall of
/// <see cref="DfsPlacementUnavailableException"/>, the limit of <see cref="DfsLimitExceededException"/>, the object of
/// <see cref="DfsIntegrityException"/> and the adapter/phase of <see cref="DfsSnapshotAdapterException"/>. Fields that do
/// not apply to the reply's code are zero/null.
/// </summary>
/// <param name="RequiredCopies">Placement: durable copies (distinct hosts) the policy requires.</param>
/// <param name="AchievedCopies">Placement: durable copies achieved.</param>
/// <param name="RequiredZones">Placement: distinct (region, zone) pairs the policy requires.</param>
/// <param name="AchievedZones">Placement: distinct zones achieved.</param>
/// <param name="LimitName">Limit: name of the exceeded limit (for example <c>MaxPublishBytes</c>).</param>
/// <param name="Limit">Limit: the configured limit.</param>
/// <param name="Requested">Limit: the requested amount.</param>
/// <param name="Object">Integrity: the corrupt or missing object, when known.</param>
/// <param name="AdapterId">Adapter failure: the adapter id.</param>
/// <param name="Phase">Adapter failure: the failing phase (prepare, resume, abort).</param>
public sealed record ErrorDetailData(
    int RequiredCopies = 0,
    int AchievedCopies = 0,
    int RequiredZones = 0,
    int AchievedZones = 0,
    string? LimitName = null,
    long Limit = 0,
    long Requested = 0,
    ObjectId? Object = null,
    string? AdapterId = null,
    string? Phase = null) : IWireRecord<ErrorDetailData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 4 + 1 + 1 + 1 + 1 + 1 + 1 + 1;

    /// <summary>Maximum UTF-8 bytes of each detail string (longer values are truncated so encoding never fails).</summary>
    public const int MaxDetailStringBytes = 1024;

    /// <summary>Extracts the transportable details of <paramref name="exception"/>; null when it has none.</summary>
    public static ErrorDetailData? FromException(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception switch
        {
            DfsPlacementUnavailableException p when p.RequiredCopies != 0 || p.AchievedCopies != 0 || p.RequiredZones != 0 || p.AchievedZones != 0
                => new ErrorDetailData(RequiredCopies: p.RequiredCopies, AchievedCopies: p.AchievedCopies, RequiredZones: p.RequiredZones, AchievedZones: p.AchievedZones),
            DfsLimitExceededException l when !string.IsNullOrEmpty(l.LimitName) || l.Limit != 0 || l.Requested != 0
                => new ErrorDetailData(LimitName: Sanitize(l.LimitName), Limit: l.Limit, Requested: l.Requested),
            DfsIntegrityException { ObjectId: { } obj } => new ErrorDetailData(Object: obj),
            DfsSnapshotAdapterException a when !string.IsNullOrEmpty(a.AdapterId) || !string.IsNullOrEmpty(a.Phase)
                => new ErrorDetailData(AdapterId: Sanitize(a.AdapterId), Phase: Sanitize(a.Phase)),
            AggregateException { InnerExceptions.Count: 1 } aggregate => FromException(aggregate.InnerExceptions[0]), // as DfsErrors.ToCode
            _ => null,
        };
    }

    /// <summary>
    /// Returns <paramref name="exception"/> (created by <see cref="DfsErrors"/> from the reply) recreated with these details
    /// when its type carries them; other exceptions are returned unchanged. Operation id, retryability and the
    /// exception's data entries are preserved.
    /// </summary>
    internal Exception ApplyTo(Exception exception, string message)
    {
        Exception detailed = exception switch
        {
            DfsPlacementUnavailableException => new DfsPlacementUnavailableException(message, RequiredCopies, AchievedCopies, RequiredZones, AchievedZones),
            DfsLimitExceededException => new DfsLimitExceededException(LimitName ?? string.Empty, Limit, Requested, message),
            DfsIntegrityException when Object.HasValue => new DfsIntegrityException(Object.Value, message),
            DfsSnapshotAdapterException => new DfsSnapshotAdapterException(AdapterId ?? string.Empty, Phase ?? string.Empty, message),
            _ => exception,
        };

        if (ReferenceEquals(detailed, exception))
            return exception;

        var source = (DfsException)exception;
        var target = (DfsException)detailed;
        target.SetOperationId(source.OperationId);
        target.SetRetryable(source.IsRetryable);
        foreach (DictionaryEntry entry in exception.Data)
            detailed.Data[entry.Key] = entry.Value;
        return detailed;
    }

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt32(RequiredCopies);
        writer.WriteVarInt32(AchievedCopies);
        writer.WriteVarInt32(RequiredZones);
        writer.WriteVarInt32(AchievedZones);
        writer.WriteOptionalString(LimitName);
        writer.WriteVarInt(Limit);
        writer.WriteVarInt(Requested);
        writer.WriteBool(Object.HasValue);
        if (Object.HasValue)
            writer.WriteObjectId(Object.Value);
        writer.WriteOptionalString(AdapterId);
        writer.WriteOptionalString(Phase);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ErrorDetailData Read(ref WireReader reader)
    {
        int requiredCopies = reader.ReadVarInt32();
        int achievedCopies = reader.ReadVarInt32();
        int requiredZones = reader.ReadVarInt32();
        int achievedZones = reader.ReadVarInt32();
        string? limitName = reader.ReadOptionalString();
        long limit = reader.ReadVarInt();
        long requested = reader.ReadVarInt();
        ObjectId? obj = reader.ReadBool() ? reader.ReadObjectId() : null;
        string? adapterId = reader.ReadOptionalString();
        string? phase = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ErrorDetailData(requiredCopies, achievedCopies, requiredZones, achievedZones, limitName, limit, requested, obj, adapterId, phase)
        {
            Extensions = extensions,
        };
    }

    private static string? Sanitize(string? value) => value is null ? null : WireText.Sanitize(value, MaxDetailStringBytes);
}
