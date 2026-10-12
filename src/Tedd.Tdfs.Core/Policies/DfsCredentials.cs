namespace Tedd.Tdfs;

/// <summary>Principal credentials. <see cref="ToString"/> never reveals the secret (SDK-016, SEC-016).</summary>
/// <param name="Principal">Principal name.</param>
/// <param name="Secret">Shared secret.</param>
public sealed record DfsCredentials(string Principal, string Secret)
{
    /// <summary>Returns the principal with the secret redacted.</summary>
    public override string ToString() => $"DfsCredentials {{ Principal = {Principal}, Secret = *** }}";
}
