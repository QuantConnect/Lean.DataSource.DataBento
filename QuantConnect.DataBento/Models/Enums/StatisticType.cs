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

namespace QuantConnect.Lean.DataSource.DataBento.Models.Enums;

/// <summary>
/// Identifies the type of statistical market data reported for an instrument.
/// </summary>
/// <remarks>
/// The statistic type defines how <c>price</c>, <c>quantity</c>, <c>stat_flags</c>,
/// and <c>ts_ref</c> should be interpreted for a given statistics record.
/// </remarks>
public enum StatisticType
{
    /// <summary>
    /// The price of the first trade of an instrument.
    /// </summary>
    OpeningPrice = 1,

    /// <summary>
    /// The probable price and quantity of the first trade of an instrument,
    /// published during the pre-open phase.
    /// </summary>
    IndicativeOpeningPrice = 2,

    /// <summary>
    /// The settlement price of an instrument.
    /// </summary>
    /// <remarks>
    /// <c>stat_flags</c> indicate whether the settlement price is final or preliminary,
    /// and whether it is actual or theoretical.
    /// </remarks>
    SettlementPrice = 3,

    /// <summary>
    /// The lowest trade price of an instrument during the trading session.
    /// </summary>
    TradingSessionLowPrice = 4,

    /// <summary>
    /// The highest trade price of an instrument during the trading session.
    /// </summary>
    TradingSessionHighPrice = 5,

    /// <summary>
    /// The number of contracts cleared for an instrument on the previous trading date.
    /// </summary>
    ClearedVolume = 6,

    /// <summary>
    /// The lowest offer price for an instrument during the trading session.
    /// </summary>
    LowestOffer = 7,

    /// <summary>
    /// The highest bid price for an instrument during the trading session.
    /// </summary>
    HighestBid = 8,

    /// <summary>
    /// The current number of outstanding contracts of an instrument.
    /// </summary>
    OpenInterest = 9,

    /// <summary>
    /// The volume-weighted average price (VWAP) for a fixing period.
    /// </summary>
    FixingPrice = 10,

    /// <summary>
    /// The last trade price and quantity during a trading session.
    /// </summary>
    ClosePrice = 11,

    /// <summary>
    /// The change in price from the previous session's close price
    /// to the most recent close price.
    /// </summary>
    NetChange = 12,

    /// <summary>
    /// The volume-weighted average price (VWAP) during the trading session.
    /// </summary>
    VolumeWeightedAveragePrice = 13,

    /// <summary>
    /// The implied volatility associated with the settlement price.
    /// </summary>
    Volatility = 14,

    /// <summary>
    /// The options delta associated with the settlement price.
    /// </summary>
    Delta = 15,

    /// <summary>
    /// The auction uncrossing price and quantity.
    /// </summary>
    /// <remarks>
    /// Used for auctions that are neither the official opening auction
    /// nor the official closing auction.
    /// </remarks>
    UncrossingPrice = 16
}

