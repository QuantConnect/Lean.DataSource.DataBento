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

using QuantConnect.Lean.DataSource.DataBento.Models.Enums;

namespace QuantConnect.Lean.DataSource.DataBento.Models;

public abstract class LevelOneDataBase : MarketDataBase
{
    /// <summary>
    /// The side that initiates the event.
    /// </summary>
    public Side Side { get; set; }

    /// <summary>
    /// The order price.
    /// </summary>
    public decimal? Price { get; set; }

    /// <summary>
    /// The order quantity.
    /// </summary>
    public int Size { get; set; }

    /// <summary>
    /// A bit field indicating event end, message characteristics, and data quality.
    /// </summary>
    public Flags Flags { get; set; }

    /// <summary>
    /// Snapshot of level-one bid and ask data.
    /// </summary>
    public LevelOneBookLevel? LevelOne { get; set; }
}
