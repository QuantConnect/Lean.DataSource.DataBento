/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
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
using QuantConnect.Tests;
using QuantConnect.Lean.DataSource.DataBento;
using QuantConnect.Securities;
using System.Collections.Generic;
using QuantConnect.Logging;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class DataBentoDataProviderHistoryTests
    {
        private DataBentoProvider _historyDataProvider;

        [SetUp]
        public void SetUp()
        {
            TestSetup.GlobalSetup();
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
                TestGlobals.Initialize();

                // DataBento futures
                var esMini = Symbol.Create("ESM3", SecurityType.Future, Market.CME);
                var znNote = Symbol.Create("ZNM3", SecurityType.Future, Market.CBOT);
                var gcGold = Symbol.Create("GCM3", SecurityType.Future, Market.COMEX);

                // test cases for supported futures
                yield return new TestCaseData(esMini, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(5), false)
                    .SetDescription("Valid ES futures - Daily resolution, 5 days period")
                    .SetCategory("Valid");

                yield return new TestCaseData(esMini, Resolution.Hour, TickType.Trade, TimeSpan.FromDays(2), false)
                    .SetDescription("Valid ES futures - Hour resolution, 2 days period")
                    .SetCategory("Valid");

                yield return new TestCaseData(esMini, Resolution.Minute, TickType.Trade, TimeSpan.FromHours(4), false)
                    .SetDescription("Valid ES futures - Minute resolution, 4 hours period")
                    .SetCategory("Valid");

                yield return new TestCaseData(znNote, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(3), false)
                    .SetDescription("Valid ZN futures - Daily resolution, 3 days period")
                    .SetCategory("Valid");

                yield return new TestCaseData(gcGold, Resolution.Hour, TickType.Trade, TimeSpan.FromDays(1), false)
                    .SetDescription("Valid GC futures - Hour resolution, 1 day period")
                    .SetCategory("Valid");

                // Test cases for quote data (may require advanced subscription)
                yield return new TestCaseData(esMini, Resolution.Tick, TickType.Quote, TimeSpan.FromMinutes(15), false)
                    .SetDescription("ES futures quote data - Tick resolution")
                    .SetCategory("Quote");

                // Unsupported security types
                var equity = Symbol.Create("SPY", SecurityType.Equity, Market.USA);
                var option = Symbol.Create("SPY", SecurityType.Option, Market.USA);

                yield return new TestCaseData(equity, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(5), true)
                    .SetDescription("Invalid - Equity not supported by DataBento")
                    .SetCategory("Invalid");

                yield return new TestCaseData(option, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(5), true)
                    .SetDescription("Invalid - Option not supported by DataBento")
                    .SetCategory("Invalid");
            }
        }

        [Test, TestCaseSource(nameof(TestParameters))]
        [Explicit("This test requires a configured DataBento API key")]
        public void GetsHistory(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period, bool expectsNoData)
        {
            var request = GetHistoryRequest(resolution, tickType, symbol, period);

            try
            {
                var slices = _historyDataProvider.GetHistory(new[] { request }, TimeZones.Utc)?.ToList();

                if (expectsNoData)
                {
                    Assert.IsTrue(slices == null || !slices.Any(),
                        $"Expected no data for unsupported symbol/security type: {symbol}");
                }
                else
                {
                    Assert.IsNotNull(slices, "Expected to receive history data");

                    if (slices.Any())
                    {
                        Log.Trace($"Received {slices.Count} slices for {symbol} at {resolution} resolution");

                        foreach (var slice in slices.Take(5)) // Check first 5 slices
                        {
                            Assert.IsNotNull(slice, "Slice should not be null");
                            Assert.IsTrue(slice.Time >= request.StartTimeUtc && slice.Time <= request.EndTimeUtc,
                                "Slice time should be within requested range");

                            if (slice.Bars.ContainsKey(symbol))
                            {
                                var bar = slice.Bars[symbol];
                                Assert.Greater(bar.Close, 0, "Bar close price should be positive");
                                Assert.GreaterOrEqual(bar.Volume, 0, "Bar volume should be non-negative");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Error getting history for {symbol}: {ex.Message}");

                if (!expectsNoData)
                {
                    throw;
                }
            }
        }

        [Test]
        [Explicit("This test requires a configured DataBento API key")]
        public void GetHistoryWithMultipleSymbols()
        {
            var symbol1 = Symbol.Create("ESM3", SecurityType.Future, Market.CME);
            var symbol2 = Symbol.Create("ZNM3", SecurityType.Future, Market.CBOT);

            var request1 = GetHistoryRequest(Resolution.Daily, TickType.Trade, symbol1, TimeSpan.FromDays(3));
            var request2 = GetHistoryRequest(Resolution.Daily, TickType.Trade, symbol2, TimeSpan.FromDays(3));

            var slices = _historyDataProvider.GetHistory(new[] { request1, request2 }, TimeZones.Utc)?.ToList();

            Assert.IsNotNull(slices, "Expected to receive history data for multiple symbols");

            if (slices.Any())
            {
                Log.Trace($"Received {slices.Count} slices for multiple symbols");

                var hasSymbol1Data = slices.Any(s => s.Bars.ContainsKey(symbol1));
                var hasSymbol2Data = slices.Any(s => s.Bars.ContainsKey(symbol2));

                Assert.IsTrue(hasSymbol1Data || hasSymbol2Data,
                    "Expected data for at least one of the requested symbols");
            }
        }

        internal static HistoryRequest GetHistoryRequest(Resolution resolution, TickType tickType, Symbol symbol, TimeSpan period)
        {
            var utcNow = DateTime.UtcNow;
            var dataType = LeanData.GetDataType(resolution, tickType);
            var marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

            var exchangeHours = marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            return new HistoryRequest(
                startTimeUtc: utcNow.Add(-period),
                endTimeUtc: utcNow,
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
}
