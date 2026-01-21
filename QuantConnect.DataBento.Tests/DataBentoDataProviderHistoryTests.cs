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

using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Securities;
using System.Collections.Generic;
using QuantConnect.Logging;
using QuantConnect.Data.Market;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

[TestFixture]
public class DataBentoDataProviderHistoryTests
{
    private DataBentoProvider _historyDataProvider;

    [SetUp]
    public void SetUp()
    {
        _historyDataProvider = new DataBentoProvider();
    }

    [TearDown]
    public void TearDown()
    {
        _historyDataProvider?.Dispose();
    }

    internal static IEnumerable<TestCaseData> TestParameters
    {
        get
        {
            var es = Symbol.CreateFuture("ES", Market.CME, new DateTime(2026, 3, 20));

            yield return new TestCaseData(es, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(5), false);
            yield return new TestCaseData(es, Resolution.Hour, TickType.Trade, TimeSpan.FromDays(2), false);
            yield return new TestCaseData(es, Resolution.Minute, TickType.Trade, TimeSpan.FromHours(4), false);
            yield return new TestCaseData(es, Resolution.Second, TickType.Trade, TimeSpan.FromHours(4), false);
            yield return new TestCaseData(es, Resolution.Tick, TickType.Quote, TimeSpan.FromMinutes(15), true);
        }
    }

    [Test, TestCaseSource(nameof(TestParameters))]
    public void GetsHistory(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period, bool expectsNoData)
    {
        var request = GetHistoryRequest(resolution, tickType, symbol, period);

        var history = _historyDataProvider.GetHistory(request);

        Assert.IsNotNull(history);

        foreach (var point in history)
        {
            Assert.AreEqual(symbol, point.Symbol);

            if (point is TradeBar bar)
            {
                Assert.Greater(bar.Close, 0);
                Assert.GreaterOrEqual(bar.Volume, 0);
            }

            if (point is Tick tick && tickType == TickType.Quote)
            {
                Assert.IsTrue(tick.BidPrice > 0 || tick.AskPrice > 0);
            }
        }
    }

    private static HistoryRequest GetHistoryRequest(
        Resolution resolution,
        TickType tickType,
        Symbol symbol,
        TimeSpan period)
    {
        var endUtc = new DateTime(2026, 1, 22);
        var startUtc = endUtc - period;

        var dataType = LeanData.GetDataType(resolution, tickType);
        var marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

        var exchangeHours = marketHoursDatabase.GetExchangeHours(
            symbol.ID.Market, symbol, symbol.SecurityType);

        var dataTimeZone = marketHoursDatabase.GetDataTimeZone(
            symbol.ID.Market, symbol, symbol.SecurityType);

        return new HistoryRequest(
            startTimeUtc: startUtc,
            endTimeUtc: endUtc,
            dataType: dataType,
            symbol: symbol,
            resolution: resolution,
            exchangeHours: exchangeHours,
            dataTimeZone: dataTimeZone,
            fillForwardResolution: resolution,
            includeExtendedMarketHours: true,
            isCustomData: false,
            DataNormalizationMode.Raw,
            tickType: tickType
        );
    }
}
