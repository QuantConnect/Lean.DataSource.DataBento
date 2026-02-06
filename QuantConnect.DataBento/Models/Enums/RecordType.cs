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

namespace QuantConnect.Lean.DataSource.DataBento.Models.Enums;

/// <summary>
/// Record type identifier (rtype) used in market data messages.
/// </summary>
public enum RecordType : byte
{
    /// <summary>Market-by-price record with book depth 0 (trades).</summary>
    MarketByPriceDepth0 = 0,

    /// <summary>Market-by-price record with book depth 1 (TBBO, MBP-1).</summary>
    MarketByPriceDepth1 = 1,

    /// <summary>Market-by-price record with book depth 10.</summary>
    MarketByPriceDepth10 = 10,

    /// <summary>Exchange status record.</summary>
    Status = 18,

    /// <summary>Instrument definition record.</summary>
    Definition = 19,

    /// <summary>Order imbalance record.</summary>
    Imbalance = 20,

    /// <summary>Error record from the live gateway.</summary>
    Error = 21,

    /// <summary>Symbol mapping record from the live gateway.</summary>
    SymbolMapping = 22,

    /// <summary>System record from the live gateway (e.g. heartbeat).</summary>
    System = 23,

    /// <summary>Statistics record from the publisher.</summary>
    Statistics = 24,

    /// <summary>OHLCV record at 1-second cadence.</summary>
    OpenHighLowCloseVolume1Second = 32,

    /// <summary>OHLCV record at 1-minute cadence.</summary>
    OpenHighLowCloseVolume1Minute = 33,

    /// <summary>OHLCV record at hourly cadence.</summary>
    OpenHighLowCloseVolume1Hour = 34,

    /// <summary>OHLCV record at daily cadence.</summary>
    OpenHighLowCloseVolume1Day = 35,

    /// <summary>Market-by-order record.</summary>
    MarketByOrder = 160,

    /// <summary>Consolidated market-by-price record with book depth 1.</summary>
    ConsolidatedMarketByPriceDepth1 = 177,

    /// <summary>Consolidated BBO at 1-second cadence.</summary>
    ConsolidatedBestBidAndOffer1Second = 192,

    /// <summary>Consolidated BBO at 1-minute cadence.</summary>
    ConsolidatedBestBidAndOffer1Minute = 193,

    /// <summary>Consolidated BBO with trades only.</summary>
    TradeWithConsolidatedBestBidAndOffer = 194,

    /// <summary>Market-by-price BBO at 1-second cadence.</summary>
    BBO1Second = 195,

    /// <summary>Market-by-price BBO at 1-minute cadence.</summary>
    BBO1Minute = 196
}
