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
*/

namespace QuantConnect.Lean.DataSource.DataBento.Models;

/// <summary>
/// Represents a level-one market data update containing best bid and ask information.
/// </summary>
public sealed class LevelOneData : MarketDataBase
{
    /// <summary>
    /// The capture-server-received timestamp expressed as the number of nanoseconds since the UNIX epoch.
    /// </summary>
    public ulong? TsRecv { get; set; }

    /// <summary>
    /// Gets the event timestamp converted to a UTC <see cref="DateTime"/>,
    /// or <c>null</c> when the timestamp is undefined.
    /// </summary>
    /// <remarks>
    /// The value is derived from <see cref="TsRecv"/>, which represents the number of
    /// nanoseconds since the UNIX epoch.
    /// <para/>
    /// A value of <see cref="ulong.MaxValue"/> (UNDEF_TIMESTAMP = 18446744073709551615)
    /// indicates a null or undefined timestamp and results in <c>null</c>.
    /// <para/>
    /// See DataBento timestamp conventions:
    /// <see href="https://databento.com/docs/standards-and-conventions/common-fields-enums-types#timestamps"/>
    /// <para/>
    /// Use for <see cref="Api.HistoricalAPIClient.BBO1mSchema"/> and <seealso cref="Api.HistoricalAPIClient.BBO1sSchema"/> data only.
    /// </remarks>
    public DateTime? UtcDateTime
    {
        get => TsRecv == ulong.MaxValue
            ? null
            : Time.UnixNanosecondTimeStampToDateTime(Convert.ToInt64(TsRecv));
    }

    /// <summary>
    /// The event type or order book operation. Can be Add, Cancel, Modify, cleaR book, Trade, Fill, or None.
    /// </summary>
    public char Action { get; set; }

    /// <summary>
    /// Side of the book affected by the update.
    /// </summary>
    public char Side { get; set; }

    /// <summary>
    /// Book depth level affected by this update.
    /// </summary>
    public int Depth { get; set; }

    /// <summary>
    /// Price associated with the update.
    /// </summary>
    public decimal? Price { get; set; }

    /// <summary>
    /// The side that initiates the event.
    /// </summary>
    /// <remarks>
    /// Can be:
    /// - Ask for a sell order (or sell aggressor in a trade);
    /// - Bid for a buy order (or buy aggressor in a trade);
    /// - None where no side is specified by the original source.
    /// </remarks>
    public int Size { get; set; }

    /// <summary>
    /// A bit field indicating event end, message characteristics, and data quality.
    /// </summary>
    public int Flags { get; set; }

    /// <summary>
    /// Snapshot of level-one bid and ask data.
    /// </summary>
    public IReadOnlyList<LevelOneBookLevel> Levels { get; set; } = [];
}