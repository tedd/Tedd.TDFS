using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Acl;

/// <summary>ACL — NTFS-like permissions over real transport and disks (FSD Addendum A, ACL-07).</summary>
public sealed class AclIntegrationTests
{
    private const string Vol = "vol";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DfsAccessRule Allow(DfsIdentity who, DfsFileSystemRights rights, DfsInheritanceFlags inheritance = DfsInheritanceFlags.None) =>
        new(who, rights, DfsAccessControlType.Allow, inheritance);

    private static async Task AssertDeniedAsync(Func<Task> action, string because)
    {
        Exception? caught = await Record.ExceptionAsync(action);
        Assert.True(caught is UnauthorizedAccessException and not DfsAuthenticationException,
            $"expected UnauthorizedAccessException but got {(caught is null ? "success" : caught.GetType().Name + ": " + caught.Message)} — {because}");
        Assert.Equal(DfsErrorCode.AccessDenied, DfsErrors.ToCode(caught!));
    }

    /// <summary>
    /// A principal denied ReadData (holding every other right and the volume Read grant) cannot obtain inline bytes,
    /// object ids or read capabilities through raw read-path requests, cannot read the objects directly from their holders
    /// with no capability or with another principal's capability — also after a holder restarts from disk — while the
    /// same requests by an allowed principal return verified bytes (positive control), and granting ReadData makes them
    /// work for the principal too.
    /// </summary>
    [Fact]
    [Catalog("ACL-031", "I", "INV-08", "INV-12", "INV-14")]
    public Task ACL_031_DirectObjectReadStillNeedsAcl() =>
        ClusterTest.RunAsync(new TestClusterOptions
        {
            NodeCount = 3,
            Transport = TransportKind.Quic,
            Storage = StorageKind.Disk,
            ScenarioTimeout = TimeSpan.FromMinutes(4),
            // Read capabilities live max(PinDuration, LeaseDuration) — 2 s with the fast test timings — but the stolen
            // capabilities are replayed across ~40 sequential connections per phase; an expired capability would make the
            // positive control fail and the theft checks pass for the wrong reason (expiry instead of the principal binding).
            Timings = NodeTimings.Fast with { PinDuration = TimeSpan.FromMinutes(2) },
        }, async cluster =>
        {
            await using DfsClient adminClient = await cluster.ConnectAsync(new TestClientOptions { Name = "acl-admin" }, Ct);
            DfsVolume admin = adminClient.GetVolume(Vol);
            TestPrincipal alice = await cluster.CreateUserAsync("alice");
            await using DfsClient aliceClient = await cluster.ConnectAsync(new TestClientOptions { Name = "alice", Credentials = alice.Credentials }, Ct);
            DfsVolume av = aliceClient.GetVolume(Vol);
            int chunk = adminClient.Capabilities.ChunkSize;
            DfsIdentity aliceId = DfsIdentity.User("alice");

            await admin.Directories.CreateDirectoryAsync("/t", Ct);
            await admin.Directories.SetAccessControlAsync("/t", new DfsSecurityDescriptor(DfsIdentity.User("admin"),
            [
                Allow(DfsIdentity.Administrators, DfsFileSystemRights.FullControl, DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit),
                Allow(aliceId, DfsFileSystemRights.FullControl & ~DfsFileSystemRights.ReadData, DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit),
            ], AreAccessRulesProtected: true), Ct);
            byte[] big = TestData.Bytes(3101, 2 * chunk + 123), small = TestData.Bytes(3102, 1000), stream = TestData.Bytes(3103, 3 * chunk + 7);
            await admin.Files.WriteAllBytesAsync("/t/big.bin", big, Ct);
            await admin.Files.WriteAllBytesAsync("/t/small.bin", small, Ct);
            await admin.Files.WriteAllBytesAsync("/t/big.bin:s", stream, Ct);
            var expected = new Dictionary<string, byte[]> { ["/t/big.bin"] = big, ["/t/small.bin"] = small, ["/t/big.bin:s"] = stream };

            // The SDK path is denied.
            foreach (string path in expected.Keys)
            {
                await AssertDeniedAsync(() => av.Files.ReadAllBytesAsync(path, Ct), $"SDK ReadAllBytes({path})");
                await AssertDeniedAsync(() => av.Files.OpenReadAsync(path, Ct), $"SDK OpenRead({path})");
            }

            // Raw read-path requests disclose nothing to alice.
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using (RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(alice.Credentials));
                foreach ((string path, byte[] bytes) in expected)
                {
                    RawReadAttempt attempt = await raw.TryReadRangeAsync(Vol, path, 0, bytes.Length);
                    Assert.Equal(DfsErrorCode.AccessDenied, attempt.Code);
                    Assert.False(attempt.DisclosedAnything, $"raw read of {path} disclosed {attempt.Objects.Count} object ids, {attempt.Capabilities.Count} capabilities, {attempt.InlineBytes?.Length ?? 0} inline bytes");
                }
            }

            // Every object of the file and its stream, on every holder: no capability and a stolen capability are refused.
            FileInspection file = (await cluster.Inspect.GetFileAsync(Vol, "/t/big.bin"))!;
            Assert.NotEmpty(file.Extents);
            StreamInspection named = Assert.Single(file.Streams, s => s.Name == "s");
            var extents = file.Extents.Select(e => (Extent: e, Source: big)).Concat(named.Extents.Select(e => (Extent: e, Source: stream))).ToArray();

            var stolen = new Dictionary<ObjectId, ReadOnlyMemory<byte>>();
            await using (RawProtocolClient adminRaw = await cluster.ConnectRawAsync(leader.Name, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await adminRaw.AuthenticateAsync(cluster.AdminCredentials));
                foreach ((string path, byte[] bytes) in new[] { ("/t/big.bin", big), ("/t/big.bin:s", stream) })
                {
                    RawReadAttempt granted = await adminRaw.TryReadRangeAsync(Vol, path, 0, bytes.Length);
                    Assert.Equal(DfsErrorCode.None, granted.Code);
                    Assert.NotEmpty(granted.Objects);
                    Assert.Equal(granted.Objects.Count, granted.Capabilities.Count);
                    for (int i = 0; i < granted.Objects.Count; i++)
                        stolen[granted.Objects[i]] = granted.Capabilities[i];
                }
            }

            Assert.All(extents, e => Assert.True(stolen.ContainsKey(e.Extent.Object), $"the allowed raw read returned no capability for {e.Extent.Object}"));

            async Task CheckHoldersAsync(string phase)
            {
                foreach ((ExtentInspection extent, byte[] source) in extents)
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(extent.Object);
                    Assert.NotEmpty(holders);
                    foreach (string holder in holders)
                    {
                        await using RawProtocolClient asAlice = await cluster.ConnectRawAsync(holder, Ct);
                        Assert.Equal(DfsErrorCode.None, await asAlice.AuthenticateAsync(alice.Credentials));
                        Assert.Equal(DfsErrorCode.AccessDenied, await asAlice.GetObjectDirectAsync(Vol, extent.Object));
                        RawObjectRead theft = await asAlice.GetObjectWithCapabilityAsync(Vol, extent.Object, stolen[extent.Object]);
                        Assert.True(theft.Code == DfsErrorCode.AccessDenied && theft.Data is null,
                            $"{phase}: alice read {extent.Object} on {holder} with the admin's capability ({theft.Code}, {theft.Data?.Length ?? 0} bytes)");

                        await using RawProtocolClient asAdmin = await cluster.ConnectRawAsync(holder, Ct);
                        Assert.Equal(DfsErrorCode.None, await asAdmin.AuthenticateAsync(cluster.AdminCredentials));
                        RawObjectRead own = await asAdmin.GetObjectWithCapabilityAsync(Vol, extent.Object, stolen[extent.Object]);
                        Assert.Equal(DfsErrorCode.None, own.Code);
                        Assert.NotNull(own.Data);
                        AssertEx.BytesEqual(source.AsSpan((int)extent.Offset, (int)extent.Length), own.Data.AsSpan((int)extent.ObjectOffset, (int)extent.Length),
                            $"{phase}: positive control, {extent.Object} on {holder}");
                    }
                }
            }

            await CheckHoldersAsync("live");

            // A holder restarted from its disk still refuses alice.
            TestNode restarted = cluster.Node((await cluster.Inspect.GetObjectHoldersAsync(extents[0].Extent.Object))[0]);
            await restarted.CrashAsync(CrashMode.LoseUnflushed);
            await restarted.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromMinutes(1), Ct);
            stolen.Clear();
            leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using (RawProtocolClient adminRaw = await cluster.ConnectRawAsync(leader.Name, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await adminRaw.AuthenticateAsync(cluster.AdminCredentials));
                foreach ((string path, byte[] bytes) in new[] { ("/t/big.bin", big), ("/t/big.bin:s", stream) })
                {
                    RawReadAttempt granted = await adminRaw.TryReadRangeAsync(Vol, path, 0, bytes.Length);
                    Assert.Equal(DfsErrorCode.None, granted.Code);
                    for (int i = 0; i < granted.Objects.Count; i++)
                        stolen[granted.Objects[i]] = granted.Capabilities[i];
                }
            }

            await CheckHoldersAsync("after a holder restart");

            // Granting ReadData makes the same raw path work for alice (the denial was the ACL, nothing else).
            await admin.Files.SetAccessControlAsync("/t/big.bin", new DfsSecurityDescriptor(DfsIdentity.User("admin"), [Allow(aliceId, DfsFileSystemRights.ReadData)]), Ct);
            byte[] afterGrant = await av.Files.ReadAllBytesAsync("/t/big.bin", Ct);
            AssertEx.BytesEqual(big, afterGrant, "SDK read after the grant");
            await using (RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(alice.Credentials));
                RawReadAttempt allowed = await raw.TryReadRangeAsync(Vol, "/t/big.bin", 0, big.Length);
                Assert.Equal(DfsErrorCode.None, allowed.Code);
                Assert.NotEmpty(allowed.Capabilities);
                Assert.Equal(DfsErrorCode.AccessDenied, (await raw.TryReadRangeAsync(Vol, "/t/small.bin", 0, small.Length)).Code);
            }
        });
}
