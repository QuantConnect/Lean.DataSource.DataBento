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
using QuantConnect.Util;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Data.Market;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

[TestFixture]
public class DataBentoDataDownloaderTests
{
    private DataBentoDataDownloader _downloader;

    private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

    [SetUp]
    public void SetUp()
    {
        _downloader = new DataBentoDataDownloader();
    }

    [TearDown]
    public void TearDown()
    {
        _downloader?.DisposeSafely();
    }

    [TestCase(Resolution.Daily)]
    [TestCase(Resolution.Hour)]
    [TestCase(Resolution.Minute)]
    [TestCase(Resolution.Second)]
    [TestCase(Resolution.Tick)]
    public void DownloadsTradeDataForLeanFuture(Resolution resolution)
    {
        var symbol = Symbol.CreateFuture("ES", Market.CME, new DateTime(2026, 3, 20));
        var exchangeTimeZone = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType).TimeZone;

        var startUtc = new DateTime(2026, 1, 18, 0, 0, 0, DateTimeKind.Utc);
        var endUtc = new DateTime(2026, 1, 20, 0, 0, 0, DateTimeKind.Utc);

        if (resolution == Resolution.Tick)
        {
            startUtc = new DateTime(2026, 1, 21, 9, 30, 0, DateTimeKind.Utc);
            endUtc = startUtc.AddMinutes(15);
        }

        var parameters = new DataDownloaderGetParameters(
            symbol,
            resolution,
            startUtc,
            endUtc,
            TickType.Trade
        );

        var data = _downloader.Get(parameters).ToList();

        Log.Trace($"Downloaded {data.Count} trade points for {symbol} @ {resolution}");

        Assert.IsNotEmpty(data);

        var startExchange = startUtc.ConvertFromUtc(exchangeTimeZone);
        var endExchange = endUtc.ConvertFromUtc(exchangeTimeZone);

        foreach (var point in data)
        {
            Assert.AreEqual(symbol, point.Symbol);
            Assert.That(point.Time, Is.InRange(startExchange, endExchange));

            switch (point)
            {
                case TradeBar bar:
                    Assert.Greater(bar.Open, 0);
                    Assert.Greater(bar.High, 0);
                    Assert.Greater(bar.Low, 0);
                    Assert.Greater(bar.Close, 0);
                    Assert.GreaterOrEqual(bar.Volume, 0);
                    Assert.GreaterOrEqual(bar.High, bar.Low);
                    break;

                case Tick tick:
                    Assert.Greater(tick.Value, 0);
                    Assert.GreaterOrEqual(tick.Quantity, 0);
                    break;

                default:
                    Assert.Fail($"Unexpected data type {point.GetType()}");
                    break;
            }
        }
    }

    [Test]
    public void DownloadsQuoteTicksForLeanFuture()
    {
        var symbol = Symbol.CreateFuture("ES", Market.CME, new DateTime(2026, 3, 20));
        var exchangeTimeZone = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType).TimeZone;

        var startUtc = new DateTime(2026, 1, 20, 9, 30, 0, DateTimeKind.Utc);
        var endUtc = startUtc.AddMinutes(15);

        var parameters = new DataDownloaderGetParameters(
            symbol,
            Resolution.Tick,
            startUtc,
            endUtc,
            TickType.Quote
        );

        var data = _downloader.Get(parameters).ToList();

        Log.Trace($"Downloaded {data.Count} quote ticks for {symbol}");

        Assert.IsNotEmpty(data);

        var startExchange = startUtc.ConvertFromUtc(exchangeTimeZone);
        var endExchange = endUtc.ConvertFromUtc(exchangeTimeZone);

        foreach (var point in data)
        {
            Assert.AreEqual(symbol, point.Symbol);
            Assert.That(point.Time, Is.InRange(startExchange, endExchange));

            if (point is Tick tick)
            {
                Assert.AreEqual(TickType.Quote, tick.TickType);
                Assert.IsTrue(
                    tick.BidPrice > 0 || tick.AskPrice > 0,
                    "Quote tick must have bid or ask"
                );
            }
            else if (point is QuoteBar bar)
            {
                Assert.IsTrue(bar.Bid != null || bar.Ask != null);
            }
        }
    }

    [Test]
    public void DataIsSortedByTime()
    {
        var symbol = Symbol.CreateFuture("ES", Market.CME, new DateTime(2026, 3, 20));

        var startUtc = new DateTime(2026, 1, 20, 0, 0, 0, DateTimeKind.Utc);
        var endUtc = new DateTime(2024, 1, 21, 0, 0, 0, DateTimeKind.Utc);

        var parameters = new DataDownloaderGetParameters(
            symbol,
            Resolution.Minute,
            startUtc,
            endUtc,
            TickType.Trade
        );

        var data = _downloader.Get(parameters).ToList();

        Assert.IsNotEmpty(data);

        for (int i = 1; i < data.Count; i++)
        {
            Assert.GreaterOrEqual(
                data[i].Time,
                data[i - 1].Time,
                $"Data not sorted at index {i}"
            );
        }
    }
}
