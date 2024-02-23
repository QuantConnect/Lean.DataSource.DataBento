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
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.DateBento;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Tests;
using QuantConnect.Util;

namespace QuantConnect.DataBento.Tests;

[TestFixture]
public class DataBentoHistoryProviderTests
{
    private readonly string _apiKey = Config.Get("databento-api-key");
    private DataBentoHistoryProvider _historyProvider;

    [SetUp]
    public void SetUp()
    {
        _historyProvider = new DataBentoHistoryProvider(_apiKey, DataBentoApi.DataBentoPublishers.XCIS);
        _historyProvider.Initialize(
            new HistoryProviderInitializeParameters(null, null, null, null, null, null, null, false, null, null));
    }

    [TearDown]
    public void TearDown()
    {
        _historyProvider.Dispose();
    }

    internal static TestCaseData[] HistoricalDataTestCases
    {
        get
        {
            var equitySymbol = Symbols.SPY;
            var optionSymbol = Symbol.CreateOption(Symbols.SPY, Market.USA, OptionStyle.American, OptionRight.Call,
                469m, new DateTime(2023, 12, 15));
            var symbols = new[] { equitySymbol }; //, optionSymbol };
            var tickTypes = new[] { TickType.Trade }; //, TickType.Quote };

            return symbols
                .Select(symbol => new[]
                {
                    // Trades
                    new TestCaseData(symbol, Resolution.Tick, TimeSpan.FromMinutes(5), TickType.Trade),
                    new TestCaseData(symbol, Resolution.Second, TimeSpan.FromMinutes(60), TickType.Trade),
                    new TestCaseData(symbol, Resolution.Minute, TimeSpan.FromHours(24), TickType.Trade),
                    new TestCaseData(symbol, Resolution.Hour, TimeSpan.FromDays(7), TickType.Trade),
                    new TestCaseData(symbol, Resolution.Daily, TimeSpan.FromDays(14), TickType.Trade),

                    // Quotes (Only Tick and Second resolutions are supported)
                    new TestCaseData(symbol, Resolution.Tick, TimeSpan.FromMinutes(5), TickType.Quote),
                    new TestCaseData(symbol, Resolution.Second, TimeSpan.FromMinutes(5), TickType.Quote),
                })
                .SelectMany(x => x)
                .ToArray();
        }
    }

    [TestCaseSource(nameof(HistoricalDataTestCases))]
    [Explicit("This tests require a databento api key, requires internet and do cost money.")]
    public void GetsHistoricalData(Symbol symbol, Resolution resolution, TimeSpan period, TickType tickType)
    {
        var requests = new List<HistoryRequest> { CreateHistoryRequest(symbol, resolution, tickType, period) };

        var history = _historyProvider.GetHistory(requests, TimeZones.Utc).ToList();

        Log.Trace("Data points retrieved: " + history.Count);

        AssertHistoricalDataResults(history.Select(x => x.AllData).SelectMany(x => x).ToList(), resolution,
            _historyProvider.DataPointCount);
    }

    internal static void AssertHistoricalDataResults(List<BaseData> history, Resolution resolution,
        int? expectedCount = null)
    {
        if (expectedCount.HasValue)
        {
            Assert.That(history.Count, Is.EqualTo(expectedCount));
        }
        else
        {
            // Assert that we got some data
            Assert.That(history, Is.Not.Empty);
        }

        if (resolution > Resolution.Tick)
        {
            // No repeating bars
            var timesArray = history.Select(x => x.Time).ToList();
            Assert.That(timesArray.Distinct().Count(), Is.EqualTo(timesArray.Count));

            // Resolution is respected
            var timeSpan = resolution.ToTimeSpan();
            Assert.That(history, Is.All.Matches<BaseData>(x => x.EndTime - x.Time == timeSpan),
                $"All bars periods should be equal to {timeSpan} ({resolution})");
        }
        else
        {
            // All data in the slice are ticks
            Assert.That(history, Is.All.Matches<BaseData>(tick => tick.GetType() == typeof(Tick)));
        }
    }

    internal static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType,
        TimeSpan period)
    {
        var end = new DateTime(2023, 12, 15, 16, 0, 0);
        if (resolution == Resolution.Daily)
        {
            end = end.Date.AddDays(1);
        }

        var dataType = LeanData.GetDataType(resolution, tickType);

        return new HistoryRequest(end.Subtract(period),
            end,
            dataType,
            symbol,
            resolution,
            SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
            TimeZones.NewYork,
            null,
            true,
            false,
            DataNormalizationMode.Adjusted,
            tickType);
    }
}