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

public sealed class LevelOneBookLevel
{
    /// <summary>
    /// Bid price at this book level.
    /// </summary>
    public decimal? BidPx { get; set; }

    /// <summary>
    /// Ask price at this book level.
    /// </summary>
    public decimal? AskPx { get; set; }

    /// <summary>
    /// Total bid size at this level.
    /// </summary>
    public int BidSz { get; set; }

    /// <summary>
    /// Total ask size at this level.
    /// </summary>
    public int AskSz { get; set; }

    /// <summary>
    /// Number of bid orders at this level.
    /// </summary>
    public int BidCt { get; set; }

    /// <summary>
    /// Number of ask orders at this level.
    /// </summary>
    public int AskCt { get; set; }
}
