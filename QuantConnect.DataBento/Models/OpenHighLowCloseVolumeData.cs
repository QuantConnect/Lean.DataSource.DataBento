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
/// Open-High-Low-Close-Volume (OHLCV) bar representing aggregated market data
/// for a specific instrument and time interval.
/// </summary>
public sealed class OpenHighLowCloseVolumeData : MarketDataBase
{
    /// <summary>
    /// Opening price of the bar.
    /// </summary>
    public decimal Open { get; set; }

    /// <summary>
    /// Highest traded price during the bar interval.
    /// </summary>
    public decimal High { get; set; }

    /// <summary>
    /// Lowest traded price during the bar interval.
    /// </summary>
    public decimal Low { get; set; }

    /// <summary>
    /// Closing price of the bar.
    /// </summary>
    public decimal Close { get; set; }

    /// <summary>
    /// Total traded volume during the bar interval.
    /// </summary>
    public decimal Volume { get; set; }
}
