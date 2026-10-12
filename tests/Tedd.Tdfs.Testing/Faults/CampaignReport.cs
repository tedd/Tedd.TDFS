namespace Tedd.Tdfs.Testing;

/// <summary>Fault-campaign coverage report (test plan §8.4).</summary>
/// <param name="Campaign">Campaign name.</param>
/// <param name="Generated">Cases generated.</param>
/// <param name="Applicable">Cases applicable to the configuration.</param>
/// <param name="Reached">Cases whose fault point was reached.</param>
/// <param name="Completed">Cases that ran to completion.</param>
/// <param name="Passed">Cases that passed.</param>
/// <param name="Failed">Cases that failed.</param>
/// <param name="Excluded">Cases excluded with a reason.</param>
/// <param name="Notes">Exclusion reasons and other notes.</param>
public sealed record CampaignReport(string Campaign, int Generated, int Applicable, int Reached, int Completed, int Passed, int Failed,
    int Excluded, IReadOnlyList<string> Notes)
{
    /// <summary>Applicable cases whose fault point was never reached (never counted as exercised).</summary>
    public int Unreached => Math.Max(0, Applicable - Reached);

    /// <summary>True when every applicable case was reached, completed and passed.</summary>
    public bool IsComplete => Applicable == Reached && Reached == Completed && Completed == Passed && Failed == 0;
}
