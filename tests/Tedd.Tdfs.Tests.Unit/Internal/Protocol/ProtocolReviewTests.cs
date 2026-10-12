using System.Collections.Immutable;
using System.Reflection;
using System.Text.RegularExpressions;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>
/// Adversarial review tests: payload identity of re-uploaded retries (OUT-002/003/006), typed error details across the
/// wire (FMT-011), process boot ids for cloned-disk fencing (RSY-021), GC decision epochs, and PROTOCOL.md staying in
/// sync with the records.
/// </summary>
public sealed class ProtocolReviewTests
{
    private static readonly DfsOperationId Op = new(7, 42);
    private static readonly VolumeId Volume = new(Guid.Parse("10203040-5060-7080-90a0-b0c0d0e0f001"));
    private static readonly UInt128 Checksum = new(0xFEED, 0xBEEF);

    // ------------------------------------------------------------------ payload identity (OUT-002/003/006)

    /// <summary>One "upload attempt" of the same content: random object ids, its own staging session and receipts.</summary>
    private static (RangeChange[] Changes, ObjectReceipt[] Receipts, StagingId Staging) Upload(ulong attempt, UInt128? checksum = null, byte inlineFill = 1)
    {
        UInt128 sum = checksum ?? Checksum;
        var obj = new ObjectId(new UInt128(attempt * 7919 + 1, attempt * 104729 + 3)); // random per attempt in real uploads
        var staging = new StagingId(100 + attempt);
        RangeChange[] changes =
        [
            RangeChange.ForData(0, 4096, obj, 0, 4096, sum),
            RangeChange.ForZero(4096, 100),
            RangeChange.ForInline(4196, new byte[] { inlineFill, 2, 3 }),
        ];
        ObjectReceipt[] receipts =
        [
            new ObjectReceipt(obj, Volume, staging, sum, 4096, ObjectEncodings.Plain, new NodeId(Guid.Parse("00000000-0000-0000-0000-0000000000a1")),
                3, 9, 638_000_000_000_000_000 + (long)attempt, new byte[32]),
            new ObjectReceipt(obj, Volume, staging, sum, 4096, ObjectEncodings.Plain, new NodeId(Guid.Parse("00000000-0000-0000-0000-0000000000a2")),
                5, 9, 638_000_000_000_000_001 + (long)attempt, Enumerable.Repeat((byte)attempt, 32).ToArray()),
        ];
        return (changes, receipts, staging);
    }

    private static RequestHeader Header(ulong session, ulong ack = 40) => new("vol", Volume, 3, Session: session, Operation: Op, AckWatermark: ack);

    public static TheoryData<string> PublicationKinds() => ["WriteAll", "Append", "CommitFile"];

    private static IVolumeRequest Publication(string kind, RequestHeader header, (RangeChange[] Changes, ObjectReceipt[] Receipts, StagingId Staging) upload)
        => kind switch
        {
            "WriteAll" => new WriteAllMessage(header, "/a.bin", 4199, upload.Changes, upload.Receipts, upload.Staging, null, null, false, 0),
            "Append" => new AppendMessage(header, "/log.bin", default, 0, 4199, upload.Changes, upload.Receipts, upload.Staging, true, null, null),
            "CommitFile" => new CommitFileMessage(header, new HandleId(5), 2, 11, -1, 4199, upload.Changes, upload.Receipts, upload.Staging, null),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

    [Theory]
    [MemberData(nameof(PublicationKinds))]
    public void RetryThatReuploadedTheContentKeepsThePayloadIdentity(string kind)
    {
        // OUT-006: the original attempt committed but its reply was lost and the process died. A new process retries with the
        // persisted operation id: it must upload again (new staging session, new random object ids, new receipts).
        IVolumeRequest original = Publication(kind, Header(session: 7), Upload(attempt: 1));
        IVolumeRequest retry = Publication(kind, Header(session: 900, ack: 0), Upload(attempt: 2));
        Assert.NotEqual(MessageCodec.Encode(original), MessageCodec.Encode(retry));
        Assert.Equal(OperationPayload.ComputeHash(original), OperationPayload.ComputeHash(retry));
    }

    [Theory]
    [MemberData(nameof(PublicationKinds))]
    public void DifferentContentUnderTheSameIdIsStillAMismatch(string kind)
    {
        // OUT-003: reusing the id for different bytes must be rejected, re-upload artifacts notwithstanding.
        byte[] baseline = OperationPayload.ComputeHash(Publication(kind, Header(7), Upload(1)));
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(Publication(kind, Header(7), Upload(1, checksum: Checksum + 1))));
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(Publication(kind, Header(7), Upload(1, inlineFill: 9))));

        var (changes, receipts, staging) = Upload(1);
        RangeChange[] shifted = [changes[0] with { Offset = 1 }, changes[1], changes[2]];
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(Publication(kind, Header(7), (shifted, receipts, staging))));
        RangeChange[] otherSlice = [changes[0] with { ObjectOffset = 8, ObjectLength = 8192 }, changes[1], changes[2]];
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(Publication(kind, Header(7), (otherSlice, receipts, staging))));
        RangeChange[] hole = [RangeChange.ForZero(0, 4096), changes[1], changes[2]];
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(Publication(kind, Header(7), (hole, receipts, staging))));
        RangeChange[] encoded = [changes[0] with { Encoding = 1 }, changes[1], changes[2]];
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(Publication(kind, Header(7), (encoded, receipts, staging))));
    }

    [Fact]
    public void NormalizeRequestLeavesTheOriginalUntouchedAndOnlyStripsUploadArtifacts()
    {
        var (changes, receipts, staging) = Upload(1);
        var request = (WriteAllMessage)Publication("WriteAll", Header(7), (changes, receipts, staging));
        var normalized = Assert.IsType<WriteAllMessage>(OperationPayload.NormalizeRequest(request));

        Assert.Equal(2, request.Receipts.Count);
        Assert.Equal(staging, request.Staging);
        Assert.False(request.Changes[0].Object.IsEmpty);

        Assert.Empty(normalized.Receipts);
        Assert.True(normalized.Staging.IsEmpty);
        Assert.All(normalized.Changes, c => Assert.True(c.Object.IsEmpty));
        Assert.Equal(request.Changes[0].Checksum, normalized.Changes[0].Checksum);
        Assert.Equal(request.Changes[2].InlineData.ToArray(), normalized.Changes[2].InlineData.ToArray());
        Assert.Equal("/a.bin", normalized.Path);
        Assert.Equal(0UL, normalized.Header.Session);

        // Requests without upload artifacts are only header-normalized.
        var copy = new CopyMessage(Header(7), "/a", "/b", true, 12, "hot", null);
        WireDeepEquality.AssertEqual(copy with { Header = OperationPayload.Normalize(copy.Header) }, OperationPayload.NormalizeRequest(copy));
    }

    // ------------------------------------------------------------------ typed error details (FMT-011)

    [Fact]
    public void PlacementShortfallSurvivesTheWire()
    {
        var thrown = new DfsPlacementUnavailableException("2 of 3 copies", requiredCopies: 3, achievedCopies: 2, requiredZones: 2, achievedZones: 1)
        {
            OperationId = new DfsOperationId(4, 5),
        };
        byte[] frame = ErrorCodec.Encode(thrown, leaderHint: "quic://n1:7100");
        var received = Assert.IsType<DfsPlacementUnavailableException>(ErrorCodec.DecodeToException(frame, path: "/a"));
        Assert.Equal((3, 2, 2, 1), (received.RequiredCopies, received.AchievedCopies, received.RequiredZones, received.AchievedZones));
        Assert.Equal(DfsErrorCode.PlacementUnavailable, received.ErrorCode);
        Assert.Equal(new DfsOperationId(4, 5), received.OperationId);
        Assert.Equal(thrown.IsRetryable, received.IsRetryable);
        Assert.Equal("quic://n1:7100", received.LeaderHint);
        Assert.Equal("/a", DfsErrors.GetPath(received));
        Assert.Equal("2 of 3 copies", received.Message);
    }

    [Fact]
    public void LimitIntegrityAndAdapterDetailsSurviveTheWire()
    {
        var limit = Assert.IsType<DfsLimitExceededException>(ErrorCodec.DecodeToException(ErrorCodec.Encode(
            new DfsLimitExceededException("MaxPublishBytes", 1024, 4096))));
        Assert.Equal(("MaxPublishBytes", 1024L, 4096L), (limit.LimitName, limit.Limit, limit.Requested));

        var integrity = Assert.IsType<DfsIntegrityException>(ErrorCodec.DecodeToException(ErrorCodec.Encode(
            new DfsIntegrityException(new ObjectId(77), "checksum mismatch"))));
        Assert.Equal(new ObjectId(77), integrity.ObjectId);

        var adapter = Assert.IsType<DfsSnapshotAdapterException>(ErrorCodec.DecodeToException(ErrorCodec.Encode(
            new DfsSnapshotAdapterException("db", "prepare", "adapter failed"))));
        Assert.Equal(("db", "prepare"), (adapter.AdapterId, adapter.Phase));

        // Encoding an error never fails, even when a detail string is not valid UTF-16 (the codec rejects unpaired surrogates).
        var odd = Assert.IsType<DfsLimitExceededException>(ErrorCodec.DecodeToException(ErrorCodec.Encode(
            new DfsLimitExceededException("Max\uD800Pins", 1, 2))));
        Assert.StartsWith("Max", odd.LimitName, StringComparison.Ordinal);
        Assert.Equal((1L, 2L), (odd.Limit, odd.Requested));

        // A single wrapped exception carries its details like DfsErrors.ToCode carries its code.
        ErrorReplyMessage wrapped = ErrorReplyMessage.FromException(new AggregateException(new DfsLimitExceededException("MaxPinsPerSession", 4, 5)));
        Assert.Equal("MaxPinsPerSession", wrapped.Detail?.LimitName);
    }

    [Fact]
    public void ErrorsWithoutDetailsKeepTheirCompactEncoding()
    {
        ErrorReplyMessage plain = ErrorReplyMessage.FromException(new DfsPlacementUnavailableException("no detail"));
        Assert.Null(plain.Detail);
        Assert.Null(ErrorReplyMessage.FromException(new FileNotFoundException("gone")).Detail);
        var received = Assert.IsType<DfsPlacementUnavailableException>(MessageCodec.Decode<ErrorReplyMessage>(MessageCodec.Encode(plain)).ToException());
        Assert.Equal(0, received.RequiredCopies);

        // A detail that does not apply to the code is ignored rather than changing the exception type.
        var mismatched = new ErrorReplyMessage(DfsErrorCode.FileNotFound, "gone") { Detail = new ErrorDetailData(RequiredCopies: 3) };
        Assert.IsType<FileNotFoundException>(MessageCodec.Decode<ErrorReplyMessage>(MessageCodec.Encode(mismatched)).ToException());
    }

    // ------------------------------------------------------------------ boot ids (RSY-021) and GC decision epochs

    [Fact]
    public void BootIdsTravelWithNodeIdentityAndAreBoundIntoTheHandshakeTranscript()
    {
        var node = new NodeId(Guid.Parse("00000000-0000-0000-0000-0000000000aa"));
        HelloMessage Hello(UInt128 boot) => new(Guid.Empty, 1, 1, ImmutableSortedSet<string>.Empty, PeerRole.Node, "n0", node, 4, null, new byte[32], boot);
        var challenge = new HelloChallengeMessage(Guid.Empty, "c", 1, ImmutableSortedSet<string>.Empty, "n1", PeerRole.Node, null, 2, new byte[32], ReadOnlyMemory<byte>.Empty, 0);
        byte[] challengeFrame = MessageCodec.Encode(challenge);

        HelloMessage original = Hello(new UInt128(1, 2));
        Assert.Equal(new UInt128(1, 2), MessageCodec.Decode<HelloMessage>(MessageCodec.Encode(original)).BootId);

        // A cloned process (same node id and incarnation, other boot id) produces a different transcript, so a proof
        // computed for one process can never be replayed for the other.
        byte[] key = Enumerable.Repeat((byte)7, 32).ToArray();
        byte[] t1 = HandshakeAuth.ComputeTranscript(MessageCodec.Encode(original), challengeFrame);
        byte[] t2 = HandshakeAuth.ComputeTranscript(MessageCodec.Encode(Hello(new UInt128(1, 3))), challengeFrame);
        Assert.NotEqual(t1, t2);
        Assert.False(HandshakeAuth.VerifyNodeProof(key, t2, HandshakeAuth.ComputeNodeProof(key, t1)));

        var register = new RegisterIncarnationMessage(node, 5, "n0", new DfsTopologyLabels("r", "z", "k", "h", "d"), ["mem://n0"], 1 << 30, new UInt128(9, 9));
        Assert.Equal(new UInt128(9, 9), MessageCodec.Decode<RegisterIncarnationMessage>(MessageCodec.Encode(register)).BootId);
        var join = new JoinClusterMessage(node, "n0", new DfsTopologyLabels("r", "z", "k", "h", "d"), ["mem://n0"], 1, 1 << 30, 1,
            ImmutableSortedSet<string>.Empty, new UInt128(3, 4));
        Assert.Equal(new UInt128(3, 4), MessageCodec.Decode<JoinClusterMessage>(MessageCodec.Encode(join)).BootId);
    }

    [Fact]
    public void GcDecisionsNameTheStagingSessionAndEpochTheyWereTakenFor()
    {
        // ARCHITECTURE §8: deletability is decided per (object, staging) inventory item. A copy that a later upload re-bound to
        // a newer staging session must be distinguishable from the garbage copy the decision was about (GC-005).
        var garbage = new ObjectStagingRef(new ObjectId(1), new StagingId(4));
        var result = new InventoryReportResultMessage([garbage], [new ObjectId(2)], 812, VolumeEpoch: 3);
        InventoryReportResultMessage decoded = MessageCodec.Decode<InventoryReportResultMessage>(MessageCodec.Encode(result));
        Assert.Equal(3UL, decoded.VolumeEpoch);
        Assert.Equal(812UL, decoded.DecisionIndex);
        ObjectStagingRef item = Assert.Single(decoded.Delete);
        Assert.Equal((garbage.Object, garbage.Staging), (item.Object, item.Staging));

        var delete = new DeleteObjectsMessage(Volume, 3, 812, [garbage, new ObjectStagingRef(new ObjectId(1), new StagingId(9))], ExcessReplicas: false);
        DeleteObjectsMessage decodedDelete = MessageCodec.Decode<DeleteObjectsMessage>(MessageCodec.Encode(delete));
        Assert.Equal([new StagingId(4), new StagingId(9)], decodedDelete.Objects.Select(o => o.Staging));
    }

    // ------------------------------------------------------------------ PROTOCOL.md stays in sync with the records

    [Fact]
    public void ProtocolReferenceDocumentsEveryMessageAndRecordInWireOrder()
    {
        string? path = FindProtocolDocument();
        if (path is null)
            Assert.Skip("docs/PROTOCOL.md is not reachable from the test output directory.");
        string text = File.ReadAllText(path);
        Assert.DoesNotContain("///", text, StringComparison.Ordinal);
        Assert.DoesNotContain("<see", text, StringComparison.Ordinal);
        Dictionary<string, List<string>> sections = ParseFieldTables(text);

        var problems = new List<string>();
        foreach (MessageType type in MessageTypes.All)
        {
            Type clr = MessageCodec.GetMessageClrType(type)!;
            Compare($"`{type}` = {(ushort)type} (`{clr.Name}`)", clr);
        }

        foreach (Type record in ProtocolSamples.RecordTypes())
            Compare($"`{record.Name}`", record);

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));

        void Compare(string heading, Type clr)
        {
            if (!sections.TryGetValue(heading, out List<string>? documented))
            {
                problems.Add($"{clr.Name}: no '#### {heading}' section");
                return;
            }

            List<string> expected = WireFieldNames(clr);
            if (!expected.SequenceEqual(documented, StringComparer.Ordinal))
                problems.Add($"{clr.Name}: documented [{string.Join(", ", documented)}] but the record has [{string.Join(", ", expected)}]");
        }
    }

    /// <summary>Wire fields of a record: its primary constructor parameters, plus init-only wire fields added later.</summary>
    private static List<string> WireFieldNames(Type type)
    {
        ConstructorInfo primary = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .Where(c => !(c.GetParameters().Length == 1 && c.GetParameters()[0].ParameterType == type))
            .OrderByDescending(c => c.GetParameters().Length)
            .First();
        List<string> names = primary.GetParameters().Select(p => p.Name!).ToList();
        if (type == typeof(ErrorReplyMessage))
            names.Add(nameof(ErrorReplyMessage.Detail));
        return names;
    }

    private static Dictionary<string, List<string>> ParseFieldTables(string text)
    {
        var sections = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        List<string>? current = null;
        var row = new Regex(@"^\| \d+ \| (?:`(?<name>[^`]+)`|—) \|", RegexOptions.CultureInvariant);
        foreach (string raw in text.Split('\n'))
        {
            string line = raw.TrimEnd('\r');
            if (line.StartsWith("#### ", StringComparison.Ordinal))
            {
                current = [];
                sections[line[5..].Trim()] = current;
                continue;
            }

            if (line.StartsWith("## ", StringComparison.Ordinal) || line.StartsWith("### ", StringComparison.Ordinal))
            {
                current = null;
                continue;
            }

            Match match = row.Match(line);
            if (current is not null && match.Success && match.Groups["name"].Success)
                current.Add(match.Groups["name"].Value);
        }

        return sections;
    }

    private static string? FindProtocolDocument()
    {
        for (DirectoryInfo? dir = new(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            string candidate = Path.Combine(dir.FullName, "docs", "PROTOCOL.md");
            if (File.Exists(candidate) && File.Exists(Path.Combine(dir.FullName, "Tedd.Tdfs.slnx")))
                return candidate;
        }

        return null;
    }
}
