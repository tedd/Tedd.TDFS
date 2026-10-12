namespace Tedd.Tdfs.Testing;

/// <summary>
/// Network control for a test cluster. Names: "n0".."n9", backups "b0".., clients "client:c0". Operations affect both
/// directions unless stated otherwise.
/// </summary>
public sealed partial class TestNetwork
{
    internal TestNetwork()
    {
    }

    /// <summary>Message statistics.</summary>
    public MessageLog Log => throw Stub.NotYet("TestNetwork.Log");

    /// <summary>Nodes not in the same group cannot exchange messages.</summary>
    public void Partition(params string[][] groups) => throw Stub.NotYet("TestNetwork.Partition");

    /// <summary>Cuts every link of <paramref name="name"/>.</summary>
    public void Isolate(string name) => throw Stub.NotYet("TestNetwork.Isolate");

    /// <summary>Cuts one direction only.</summary>
    public void Cut(string from, string to) => throw Stub.NotYet("TestNetwork.Cut");

    /// <summary>Cuts both directions between two names.</summary>
    public void CutBoth(string a, string b) => throw Stub.NotYet("TestNetwork.CutBoth");

    /// <summary>Connection stays open but messages vanish (half-open).</summary>
    public void BlackHole(string from, string to) => throw Stub.NotYet("TestNetwork.BlackHole");

    /// <summary>Restores one direction.</summary>
    public void Restore(string from, string to) => throw Stub.NotYet("TestNetwork.Restore");

    /// <summary>Removes all cuts, black holes and mesh restrictions.</summary>
    public void Heal() => throw Stub.NotYet("TestNetwork.Heal");

    /// <summary>Adds latency to one direction.</summary>
    public void SetLatency(string from, string to, TimeSpan latency) => throw Stub.NotYet("TestNetwork.SetLatency");

    /// <summary>Limits bandwidth of one direction.</summary>
    public void SetBandwidth(string from, string to, long bytesPerSecond) => throw Stub.NotYet("TestNetwork.SetBandwidth");

    /// <summary>Closes all connections of <paramref name="name"/> (they reconnect).</summary>
    public void ResetConnections(string name) => throw Stub.NotYet("TestNetwork.ResetConnections");

    /// <summary>Drops the next matching messages.</summary>
    public MessageRule DropNext(MessageFilter filter, int count = 1) => throw Stub.NotYet("TestNetwork.DropNext");

    /// <summary>Delivers and executes matching requests but loses their replies.</summary>
    public MessageRule DropReplies(MessageFilter filter, int count = 1) => throw Stub.NotYet("TestNetwork.DropReplies");

    /// <summary>Duplicates the next matching messages.</summary>
    public MessageRule DuplicateNext(MessageFilter filter, int count = 1) => throw Stub.NotYet("TestNetwork.DuplicateNext");

    /// <summary>Delays the next matching messages.</summary>
    public MessageRule DelayNext(MessageFilter filter, TimeSpan delay, int count = 1) => throw Stub.NotYet("TestNetwork.DelayNext");

    /// <summary>Captures matching messages until released.</summary>
    public MessageHold Hold(MessageFilter filter) => throw Stub.NotYet("TestNetwork.Hold");

    /// <summary>Re-delivers a previously captured frame from its original sender identity; returns the reply error code.</summary>
    public Task<DfsErrorCode> ReplayAsync(CapturedMessage message) => throw Stub.NotYet("TestNetwork.ReplayAsync");
}
