using System.Collections;
using System.Collections.Immutable;
using System.Reflection;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>
/// Builds populated instances of protocol records by reflection over their primary constructors, so every message and
/// nested record (including ones added later) is exercised without hand-written samples. <see cref="Random"/> mode fills
/// every field with seeded random values (nullable values are sometimes null, collections hold 0..3 elements, optional
/// extensions are sometimes attached); <see cref="Minimal"/> mode produces the smallest encoding (zeros, empty strings and
/// collections, nulls).
/// </summary>
internal sealed class ProtocolSamples
{
    private const int MaxDepth = 4;
    // NullabilityInfoContext is not thread-safe (it caches per member); tests run in parallel.
    private static readonly NullabilityInfoContext Nullability = new();
    private static readonly System.Threading.Lock NullabilityLock = new();
    private static readonly string[] Words = ["a", "zone-1", "n0", "/docs/report.json", "ÆØÅ", "名前", "\U0001F600x", "", "%2F", "x y"];

    private readonly Random _random;
    private readonly bool _minimal;

    private ProtocolSamples(int seed, bool minimal)
    {
        _random = new Random(seed);
        _minimal = minimal;
    }

    /// <summary>A generator of seeded random instances.</summary>
    public static ProtocolSamples Random(int seed) => new(seed, minimal: false);

    /// <summary>A generator of minimal instances.</summary>
    public static ProtocolSamples Minimal() => new(0, minimal: true);

    /// <summary>All registered message CLR types.</summary>
    public static IReadOnlyList<Type> MessageTypes()
        => MessageCodec.RegisteredTypes.Select(t => MessageCodec.GetMessageClrType(t)!).ToArray();

    /// <summary>All non-message protocol record types (implementing <see cref="IWireRecord{TSelf}"/>) in Core.</summary>
    public static IReadOnlyList<Type> RecordTypes()
        => typeof(IWireRecord<>).Assembly.GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false } &&
                        t.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IWireRecord<>)))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToArray();

    /// <summary>Creates an instance of <paramref name="type"/>.</summary>
    public object Create(Type type) => Create(type, "root", 0)!;

    /// <summary>Creates a message of the given wire type.</summary>
    public IWireMessage CreateMessage(MessageType type) => (IWireMessage)Create(MessageCodec.GetMessageClrType(type)!);

    private object? Create(Type type, string name, int depth)
    {
        if (type == typeof(DataCapability))
            return CreateCapability();

        Type? underlying = Nullable.GetUnderlyingType(type);
        if (underlying is not null)
            return !_minimal && _random.Next(3) > 0 ? Create(underlying, name, depth) : null;

        if (type == typeof(string)) return NextString();
        if (type == typeof(bool)) return !_minimal && _random.Next(2) == 1;
        if (type == typeof(byte)) return _minimal ? (byte)0 : (byte)_random.Next(256);
        if (type == typeof(ushort)) return _minimal ? (ushort)0 : (ushort)_random.Next(ushort.MaxValue + 1);
        if (type == typeof(int)) return _minimal ? 0 : NextInt32();
        if (type == typeof(uint)) return _minimal ? 0u : (uint)_random.NextInt64(0, uint.MaxValue + 1L);
        if (type == typeof(long)) return _minimal ? 0L : NextInt64();
        if (type == typeof(ulong)) return _minimal ? 0UL : NextUInt64();
        if (type == typeof(UInt128)) return _minimal ? UInt128.Zero : new UInt128(NextUInt64(), NextUInt64());
        if (type == typeof(Guid)) return _minimal ? Guid.Empty : NextGuid();
        if (type == typeof(TimeSpan)) return _minimal ? TimeSpan.Zero : TimeSpan.FromTicks(NextInt64());
        if (type == typeof(DateTimeOffset))
            return _minimal ? new DateTimeOffset(0, TimeSpan.Zero) : new DateTimeOffset(_random.NextInt64(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks + 1), TimeSpan.Zero);
        if (type == typeof(ReadOnlyMemory<byte>)) return NextBytes(name);
        if (type.IsEnum) return NextEnum(type);

        if (type == typeof(DfsIdentity))
        {
            return _minimal
                ? DfsIdentity.Everyone
                : _random.Next(3) switch
                {
                    0 => DfsIdentity.User("u" + _random.Next(1000).ToString(System.Globalization.CultureInfo.InvariantCulture)),
                    1 => DfsIdentity.Group("g" + _random.Next(1000).ToString(System.Globalization.CultureInfo.InvariantCulture)),
                    _ => DfsIdentity.AuthenticatedUsers,
                };
        }

        if (type == typeof(NodeId)) return new NodeId((Guid)Create(typeof(Guid), name, depth)!);
        if (type == typeof(VolumeId)) return new VolumeId((Guid)Create(typeof(Guid), name, depth)!);
        if (type == typeof(SnapshotId)) return new SnapshotId((Guid)Create(typeof(Guid), name, depth)!);
        if (type == typeof(ObjectId)) return new ObjectId((UInt128)Create(typeof(UInt128), name, depth)!);
        if (type == typeof(InodeId)) return new InodeId((ulong)Create(typeof(ulong), name, depth)!);
        if (type == typeof(HandleId)) return new HandleId((ulong)Create(typeof(ulong), name, depth)!);
        if (type == typeof(StagingId)) return new StagingId((ulong)Create(typeof(ulong), name, depth)!);
        if (type == typeof(DfsOperationId)) return new DfsOperationId((ulong)Create(typeof(ulong), name, depth)!, (ulong)Create(typeof(ulong), name, depth)!);
        if (type == typeof(DfsTransactionId)) return new DfsTransactionId((DfsOperationId)Create(typeof(DfsOperationId), name, depth)!);
        if (type == typeof(CommitToken))
            return new CommitToken((Guid)Create(typeof(Guid), name, depth)!, (VolumeId)Create(typeof(VolumeId), name, depth)!,
                (ulong)Create(typeof(ulong), name, depth)!, (ulong)Create(typeof(ulong), name, depth)!);

        if (type.IsGenericType)
        {
            Type def = type.GetGenericTypeDefinition();
            Type[] args = type.GetGenericArguments();
            if (def == typeof(IReadOnlyList<>))
                return CreateList(args[0], name, depth);
            if (def == typeof(IReadOnlySet<>) && args[0] == typeof(string))
                return ImmutableSortedSet.CreateRange(StringComparer.Ordinal, DistinctStrings());
            if (def == typeof(IReadOnlyDictionary<,>) && args[0] == typeof(string))
                return CreateMap(args[1], name, depth);
        }

        if (type.IsClass)
            return CreateObject(type, depth);

        throw new NotSupportedException($"ProtocolSamples cannot create {type} ({name}).");
    }

    private object CreateObject(Type type, int depth)
    {
        ConstructorInfo? primary = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .Where(c => !(c.GetParameters().Length == 1 && c.GetParameters()[0].ParameterType == type))
            .OrderByDescending(c => c.GetParameters().Length)
            .FirstOrDefault()
            ?? throw new NotSupportedException($"{type} has no public constructor.");

        ParameterInfo[] parameters = primary.GetParameters();
        object? instance;
        if (parameters.Length == 0)
        {
            instance = primary.Invoke(null);
            // Records configured through init properties (DfsStoragePolicy, DfsVolumeLimits, ...).
            if (!IsProtocolRecord(type))
            {
                foreach (PropertyInfo property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (property.SetMethod is null || !property.SetMethod.IsPublic || property.GetIndexParameters().Length > 0)
                        continue;
                    property.SetValue(instance, CreateForMember(property.PropertyType, property.Name, NullabilityOf(property), depth + 1));
                }
            }
        }
        else
        {
            object?[] values = parameters
                .Select(p => CreateForMember(p.ParameterType, p.Name!, NullabilityOf(p), depth + 1))
                .ToArray();
            instance = primary.Invoke(values);
        }

        // Wire fields added as init-only properties rather than constructor parameters (ErrorReplyMessage.Detail, the
        // Addendum A trailing fields of AdminVolumeSpecBody and VolumeEntry).
        if (!_minimal && IsProtocolRecord(type))
        {
            foreach (PropertyInfo property in InitOnlyWireFields(type))
            {
                if (_random.Next(2) == 0)
                    property.SetValue(instance, CreateForMember(property.PropertyType, property.Name, NullabilityOf(property), depth + 1));
            }
        }

        if (!_minimal && IsProtocolRecord(type) && _random.Next(4) == 0)
        {
            PropertyInfo extensions = type.GetProperty("Extensions")!;
            extensions.SetValue(instance, NextExtensions());
        }

        return instance!;
    }

    private object? CreateForMember(Type type, string name, NullabilityInfo nullability, int depth)
    {
        // Reference-type members annotated nullable (string?, records?) are sometimes null.
        if (!type.IsValueType && nullability.ReadState == NullabilityState.Nullable)
        {
            if (_minimal || _random.Next(3) == 0)
                return null;
        }

        if (depth > MaxDepth && IsCollection(type))
            return CreateEmptyCollection(type);
        return Create(type, name, depth);
    }

    private object CreateList(Type element, string name, int depth)
    {
        int count = _minimal || depth > MaxDepth ? 0 : _random.Next(4);
        Array array = Array.CreateInstance(element, count);
        for (int i = 0; i < count; i++)
            array.SetValue(Create(element, name, depth + 1), i);
        return array;
    }

    private object CreateMap(Type valueType, string name, int depth)
    {
        Type dictType = typeof(SortedDictionary<,>).MakeGenericType(typeof(string), valueType);
        var dict = (IDictionary)Activator.CreateInstance(dictType, StringComparer.Ordinal)!;
        foreach (string key in DistinctStrings())
            dict[key] = Create(valueType, name, depth + 1);
        return dict;
    }

    private static object CreateEmptyCollection(Type type)
    {
        Type def = type.GetGenericTypeDefinition();
        Type[] args = type.GetGenericArguments();
        if (def == typeof(IReadOnlyList<>))
            return Array.CreateInstance(args[0], 0);
        if (def == typeof(IReadOnlySet<>))
            return ImmutableSortedSet.Create<string>(StringComparer.Ordinal);
        return Activator.CreateInstance(typeof(SortedDictionary<,>).MakeGenericType(args), StringComparer.Ordinal)!;
    }

    private DataCapability CreateCapability()
    {
        Guid cluster = (Guid)Create(typeof(Guid), "cluster", 0)!;
        var volume = new VolumeId((Guid)Create(typeof(Guid), "volume", 0)!);
        ulong epoch = (ulong)Create(typeof(ulong), "epoch", 0)!;
        string principal = NextString();
        var expiry = (DateTimeOffset)Create(typeof(DateTimeOffset), "expiry", 0)!;
        DataCapability capability = _minimal || _random.Next(2) == 0
            ? DataCapability.ForStaging(cluster, volume, epoch, principal, new StagingId((ulong)Create(typeof(ulong), "staging", 0)!), expiry)
            : DataCapability.ForRead(cluster, volume, epoch, principal, Enumerable.Range(0, _random.Next(5)).Select(_ => new ObjectId(new UInt128(NextUInt64(), NextUInt64()))), expiry);
        return _minimal ? capability : capability.Sign(NextBytesExact(32));
    }

    private IReadOnlyList<WireExtension> NextExtensions()
    {
        int count = _random.Next(1, 3);
        var result = new WireExtension[count];
        for (int i = 0; i < count; i++)
            result[i] = new WireExtension((uint)(1000 + i * 7 + _random.Next(5)), Required: false, NextBytesExact(_random.Next(0, 6)));
        return result;
    }

    private object NextEnum(Type type)
    {
        Array values = Enum.GetValues(type);
        if (_minimal)
            return Enum.IsDefined(type, Enum.ToObject(type, 0)) ? Enum.ToObject(type, 0) : values.GetValue(0)!;
        return values.GetValue(_random.Next(values.Length))!;
    }

    private ReadOnlyMemory<byte> NextBytes(string name)
    {
        // Fixed-size fields: nonces, proofs and receipt MACs are exactly 32 bytes; HelloResult.ServerMac is empty or 32.
        if (name is "Nonce" or "ServerNonce" or "Proof" or "Mac")
            return NextBytesExact(ProtocolInfo.MacSize);
        if (name == "ServerMac")
            return !_minimal && _random.Next(2) == 0 ? NextBytesExact(ProtocolInfo.MacSize) : ReadOnlyMemory<byte>.Empty;
        return _minimal ? ReadOnlyMemory<byte>.Empty : NextBytesExact(_random.Next(0, 40));
    }

    private byte[] NextBytesExact(int count)
    {
        var bytes = new byte[count];
        _random.NextBytes(bytes);
        return bytes;
    }

    private string NextString() => _minimal ? string.Empty : Words[_random.Next(Words.Length)] + (_random.Next(3) == 0 ? _random.Next(1000).ToString(System.Globalization.CultureInfo.InvariantCulture) : "");

    private IEnumerable<string> DistinctStrings()
    {
        if (_minimal)
            return [];
        var set = new SortedSet<string>(StringComparer.Ordinal);
        int count = _random.Next(4);
        for (int i = 0; i < count; i++)
            set.Add(NextString());
        return set;
    }

    private int NextInt32() => _random.Next(4) switch
    {
        0 => _random.Next(-200, 200),
        1 => int.MaxValue - _random.Next(3),
        2 => int.MinValue + _random.Next(3),
        _ => _random.Next(),
    };

    private long NextInt64() => _random.Next(4) switch
    {
        0 => _random.NextInt64(-300, 300),
        1 => long.MaxValue - _random.Next(3),
        2 => long.MinValue + _random.Next(3),
        _ => _random.NextInt64(),
    };

    private ulong NextUInt64() => _random.Next(4) switch
    {
        0 => (ulong)_random.Next(0, 300),
        1 => ulong.MaxValue - (ulong)_random.Next(3),
        _ => (ulong)_random.NextInt64() ^ ((ulong)_random.Next(2) << 63),
    };

    private Guid NextGuid()
    {
        Span<byte> bytes = stackalloc byte[16];
        _random.NextBytes(bytes);
        return new Guid(bytes);
    }

    /// <summary>
    /// Wire fields declared as public init-only properties of a protocol record (beyond its primary constructor
    /// parameters and the extension block), in declaration order.
    /// </summary>
    public static IReadOnlyList<PropertyInfo> InitOnlyWireFields(Type type)
    {
        ConstructorInfo primary = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .Where(c => !(c.GetParameters().Length == 1 && c.GetParameters()[0].ParameterType == type))
            .OrderByDescending(c => c.GetParameters().Length)
            .First();
        var parameters = new HashSet<string>(primary.GetParameters().Select(p => p.Name!), StringComparer.Ordinal);
        return type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(p => p.SetMethod is { IsPublic: true } && p.GetIndexParameters().Length == 0
                && p.Name != "Extensions" && !parameters.Contains(p.Name)
                && p.SetMethod.ReturnParameter.GetRequiredCustomModifiers().Any(m => m.FullName == "System.Runtime.CompilerServices.IsExternalInit"))
            .OrderBy(p => p.MetadataToken)
            .ToArray();
    }

    private static NullabilityInfo NullabilityOf(PropertyInfo property)
    {
        lock (NullabilityLock)
            return Nullability.Create(property);
    }

    private static NullabilityInfo NullabilityOf(ParameterInfo parameter)
    {
        lock (NullabilityLock)
            return Nullability.Create(parameter);
    }

    private static bool IsProtocolRecord(Type type)
        => type.GetInterfaces().Any(i => i.IsGenericType &&
            (i.GetGenericTypeDefinition() == typeof(IWireRecord<>) || i.GetGenericTypeDefinition() == typeof(IWireMessage<>)));

    private static bool IsCollection(Type type)
        => type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(IReadOnlyList<>) ||
                                  type.GetGenericTypeDefinition() == typeof(IReadOnlySet<>) ||
                                  type.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>));
}
