using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// Issues and verifies data capabilities (PROTOCOL.md §7.3): staging capabilities for <c>PutObject</c> and per-object read
/// capabilities for <c>GetObject</c>, MAC'd with the cluster capability key. Object ids alone grant nothing (SEC-004).
/// </summary>
internal sealed class Capabilities
{
    private readonly KeyHierarchy _keys;
    private readonly Func<Guid> _clusterId;
    private readonly TimeProvider _time;
    private readonly DateTimeOffset _wallAnchor;
    private readonly long _monotonicAnchor;

    public Capabilities(KeyHierarchy keys, Func<Guid> clusterId, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(clusterId);
        ArgumentNullException.ThrowIfNull(time);
        _keys = keys;
        _clusterId = clusterId;
        _time = time;
        _wallAnchor = time.GetUtcNow();
        _monotonicAnchor = time.GetTimestamp();
    }

    /// <summary>
    /// Capability time: this node's wall clock when it started, advanced by its monotonic clock. Expiries are issued and
    /// verified on it, so a later wall-clock step on the issuing leader or on a verifying storage node neither expires valid
    /// capabilities early nor revives expired ones (SEC-017, HAR-003); the nodes' wall clocks only need to agree at start.
    /// </summary>
    private DateTimeOffset Now => _wallAnchor + _time.GetElapsedTime(_monotonicAnchor);

    /// <summary>A signed staging capability for one open staging session.</summary>
    public byte[] IssueStaging(VolumeRecord volume, string principal, StagingId staging, TimeSpan lifetime, out DateTimeOffset expires)
    {
        ArgumentNullException.ThrowIfNull(volume);
        expires = Now + lifetime;
        return DataCapability.ForStaging(_clusterId(), volume.Id, volume.Epoch, principal, staging, expires).Sign(_keys.CapabilityKey).Encode();
    }

    /// <summary>A signed read capability for exactly <paramref name="objects"/>.</summary>
    public byte[] IssueRead(VolumeRecord volume, string principal, IEnumerable<ObjectId> objects, TimeSpan lifetime, out DateTimeOffset expires)
    {
        ArgumentNullException.ThrowIfNull(volume);
        expires = Now + lifetime;
        return DataCapability.ForRead(_clusterId(), volume.Id, volume.Epoch, principal, objects, expires).Sign(_keys.CapabilityKey).Encode();
    }

    /// <summary>
    /// Verifies a capability presented by <paramref name="principal"/> for <paramref name="kind"/> on <paramref name="volume"/>
    /// (MAC in constant time, cluster, volume and epoch, principal, expiry on this node's clock, staging session or object).
    /// </summary>
    /// <exception cref="UnauthorizedAccessException">The capability does not authorize the request.</exception>
    public DataCapability Verify(ReadOnlySpan<byte> token, DataCapabilityKind kind, VolumeRecord volume, string principal, StagingId? staging = null,
        ObjectId? obj = null)
    {
        ArgumentNullException.ThrowIfNull(volume);
        if (token.IsEmpty || !DataCapability.TryDecode(token, out DataCapability? capability) || capability is null)
            throw Denied("a data capability is required");
        if (!capability.VerifyMac(_keys.CapabilityKey))
            throw Denied("the capability is not authentic");
        if (capability.Kind != kind)
            throw Denied($"a {kind} capability is required");
        if (capability.ClusterId != _clusterId())
            throw Denied("the capability was issued by another cluster");
        if (capability.Volume != volume.Id)
            throw Denied("the capability is for another volume");
        if (capability.VolumeEpoch != volume.Epoch)
            throw Denied("the capability is for another volume epoch");
        if (!string.Equals(capability.Principal, principal, StringComparison.Ordinal))
            throw Denied("the capability was issued to another principal");
        if (capability.IsExpired(Now))
            throw Denied("the capability expired");
        if (staging is { } s && capability.Staging != s)
            throw Denied("the capability is for another staging session");
        if (obj is { } o && !capability.AllowsRead(o))
            throw Denied("the capability does not cover the object");
        return capability;
    }

    private static UnauthorizedAccessException Denied(string reason) => new("Access denied: " + reason + ".");
}
