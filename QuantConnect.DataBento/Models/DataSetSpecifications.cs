/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2026 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

namespace QuantConnect.Lean.DataSource.DataBento.Models;

/// <summary>
/// Provides pre-defined historical dataset specifications for easy access.
/// </summary>
public static class PredefinedDataSets
{
    /// <summary>
    /// Historical dataset: XEUR.EOBI
    /// </summary>
    public static readonly DataSetSpecifications XEUR_EOBI = new(
        "XEUR.EOBI",
        "15 mins",
        "Next day 00:00 CET/CEST",
        "https://databento.com/catalog/xeur/XEUR.EOBI"
    );

    /// <summary>
    /// Historical dataset: GLBX.MDP3
    /// </summary>
    public static readonly DataSetSpecifications GLBX_MDP3 = new(
        "GLBX.MDP3",
        "20 minutes",
        "8 hours",
        "https://databento.com/catalog/cme/GLBX.MDP3"
    );

    /// <summary>
    /// Historical dataset: IFUS.IMPACT
    /// </summary>
    public static readonly DataSetSpecifications IFUS_IMPACT = new(
        "IFUS.IMPACT",
        "15 minutes",
        "Same day 18:05 EST/EDT",
        "https://databento.com/catalog/ifus/IFUS.IMPACT");
}

/// <summary>
/// Represents specifications and metadata for a data set, including historical delay and access details.
/// </summary>
public class DataSetSpecifications
{
    /// <summary>
    /// Internal flag to ensure the delay warning message is only generated once.
    /// </summary>
    private bool _delayWarningFired;

    /// <summary>
    /// The unique identifier or name of the dataset.
    /// </summary>
    public string DataSetID { get; }

    /// <summary>
    /// User-friendly historical delay string for datasets requiring a license.
    /// Examples: "15 mins", "Delayed 20 minutes".
    /// </summary>
    public string HistoricalDelayWithLicense { get; }

    /// <summary>
    /// User-friendly historical delay string for datasets NOT requiring a license.
    /// Examples: "8 hours", "Next day 00:00 CET/CEST".
    /// </summary>
    public string HistoricalDelayWithoutLicense { get; }

    /// <summary>
    /// Optional link to the dataset specifications page.
    /// </summary>
    public string Link { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataSetSpecifications"/> class
    /// with dataset name and historical delay information for users with and without a license.
    /// </summary>
    /// <param name="dataSetID">The unique identifier or name of the dataset.</param>
    /// <param name="historicalDelayWithLicense">
    /// The historical delay for users who have a live license. 
    /// Example: "15 minutes".
    /// </param>
    /// <param name="historicalDelayWithoutLicense">
    /// The historical delay for users who do NOT have a license. 
    /// Example: "8 hours" or "Next day 00:00 CET/CEST".
    /// </param>
    public DataSetSpecifications(string dataSetID, string historicalDelayWithLicense, string historicalDelayWithoutLicense, string link)
    {
        DataSetID = dataSetID;
        HistoricalDelayWithLicense = historicalDelayWithLicense;
        HistoricalDelayWithoutLicense = historicalDelayWithoutLicense;
        Link = link;
    }

    /// <summary>
    /// Attempts to generate a user-friendly delay warning message.
    /// The message will only be returned the first time this method is called.
    /// </summary>
    /// <param name="message">Outputs the generated warning message if not previously fired; otherwise null.</param>
    /// <returns>True if the message was generated; false if it was already generated before.</returns>
    public bool TryGetDelayWarningMessage(out string? message)
    {
        message = null;
        if (_delayWarningFired)
        {
            return false;
        }
        message = $"Dataset [{DataSetID}] historical data information:\n" +
            $"- Users with a live license: delayed by approximately {HistoricalDelayWithLicense}." +
            $"For access to more recent data, use the intraday replay feature of the live data client.\n" +
            $"- Users without a license: delayed by {HistoricalDelayWithoutLicense}.\n" +
            $"- More information: {Link} (go to the 'Specifications' tab)";

        _delayWarningFired = true;
        return true;
    }
}
