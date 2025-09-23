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
using QuantConnect.Data.Market;
using QuantConnect.Lean.DataSource.DataBento;
using QuantConnect.Logging;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class DataBentoDataDownloaderTests
    {
        private DataBentoDataDownloader _downloader;

        [SetUp]
        public void SetUp()
        {
            TestSetup.GlobalSetup();
            _downloader = new DataBentoDataDownloader();
        }

        [TearDown]
        public void TearDown()
        {
            _downloader?.Dispose();
        }

        [Test]
        [TestCase("ESM3", SecurityType.Future, Market.CME, Resolution.Daily, TickType.Trade)]
        [TestCase("ESM3", SecurityType.Future, Market.CME, Resolution.Hour, TickType.Trade)]
        [TestCase("ESM3", SecurityType.Future, Market.CME, Resolution.Minute, TickType.Trade)]
        [TestCase("ESM3", SecurityType.Future, Market.CME, Resolution.Second, TickType.Trade)]
        [TestCase("ESM3", SecurityType.Future, Market.CME, Resolution.Tick, TickType.Trade)]
        [Explicit("This test requires a configured DataBento API key")]
        public void DownloadsHistoricalData(string ticker, SecurityType securityType, string market, Resolution resolution, TickType tickType)
        {
            var symbol = Symbol.Create(ticker, securityType, market);
            var startTime = new DateTime(2024, 1, 15);
            var endTime = new DateTime(2024, 1, 16);
            var param = new DataDownloaderGetParameters(symbol, resolution, startTime, endTime, tickType);

            var downloadResponse = _downloader.Get(param).ToList();

            Log.Trace($"Downloaded {downloadResponse.Count} data points for {symbol} at {resolution} resolution");

            Assert.IsTrue(downloadResponse.Any(), "Expected to download at least one data point");

            foreach (var data in downloadResponse)
            {
                Assert.IsNotNull(data, "Data point should not be null");
                Assert.AreEqual(symbol, data.Symbol, "Symbol should match requested symbol");
                Assert.IsTrue(data.Time >= startTime && data.Time <= endTime, "Data time should be within requested range");

                if (data is TradeBar tradeBar)
                {
                    Assert.Greater(tradeBar.Close, 0, "Close price should be positive");
                    Assert.GreaterOrEqual(tradeBar.Volume, 0, "Volume should be non-negative");
                    Assert.Greater(tradeBar.High, 0, "High price should be positive");
                    Assert.Greater(tradeBar.Low, 0, "Low price should be positive");
                    Assert.Greater(tradeBar.Open, 0, "Open price should be positive");
                    Assert.GreaterOrEqual(tradeBar.High, tradeBar.Low, "High should be >= Low");
                    Assert.GreaterOrEqual(tradeBar.High, tradeBar.Open, "High should be >= Open");
                    Assert.GreaterOrEqual(tradeBar.High, tradeBar.Close, "High should be >= Close");
                    Assert.LessOrEqual(tradeBar.Low, tradeBar.Open, "Low should be <= Open");
                    Assert.LessOrEqual(tradeBar.Low, tradeBar.Close, "Low should be <= Close");
                }
                else if (data is QuoteBar quoteBar)
                {
                    Assert.Greater(quoteBar.Close, 0, "Quote close price should be positive");
                    if (quoteBar.Bid != null)
                    {
                        Assert.Greater(quoteBar.Bid.Close, 0, "Bid price should be positive");
                    }
                    if (quoteBar.Ask != null)
                    {
                        Assert.Greater(quoteBar.Ask.Close, 0, "Ask price should be positive");
                    }
                }
                else if (data is Tick tick)
                {
                    Assert.Greater(tick.Value, 0, "Tick value should be positive");
                    Assert.GreaterOrEqual(tick.Quantity, 0, "Tick quantity should be non-negative");
                }
                else if (data is DataBentoDataType dataBentoData)
                {
                    Assert.Greater(dataBentoData.Close, 0, "DataBento close price should be positive");
                    Assert.GreaterOrEqual(dataBentoData.Volume, 0, "DataBento volume should be non-negative");
                    Assert.Greater(dataBentoData.High, 0, "DataBento high price should be positive");
                    Assert.Greater(dataBentoData.Low, 0, "DataBento low price should be positive");
                    Assert.Greater(dataBentoData.Open, 0, "DataBento open price should be positive");
                    Assert.IsNotNull(dataBentoData.RawSymbol, "RawSymbol should not be null");
                }
            }
        }

        [Test]
        [TestCase("ZNM3", SecurityType.Future, Market.CBOT, Resolution.Daily, TickType.Trade)]
        [TestCase("ZNM3", SecurityType.Future, Market.CBOT, Resolution.Hour, TickType.Trade)]
        [Explicit("This test requires a configured DataBento API key")]
        public void DownloadsFuturesHistoricalData(string ticker, SecurityType securityType, string market, Resolution resolution, TickType tickType)
        {
            var symbol = Symbol.Create(ticker, securityType, market);
            var startTime = new DateTime(2024, 1, 15);
            var endTime = new DateTime(2024, 1, 16);
            var param = new DataDownloaderGetParameters(symbol, resolution, startTime, endTime, tickType);

            var downloadResponse = _downloader.Get(param).ToList();

            Log.Trace($"Downloaded {downloadResponse.Count} data points for futures {symbol}");

            Assert.IsTrue(downloadResponse.Any(), "Expected to download futures data");

            foreach (var data in downloadResponse)
            {
                Assert.AreEqual(symbol, data.Symbol, "Symbol should match requested futures symbol");
                Assert.Greater(data.Value, 0, "Data value should be positive");
            }
        }

        [Test]
        [TestCase("ESM3", SecurityType.Future, Market.CME, Resolution.Tick, TickType.Quote)]
        [Explicit("This test requires a configured DataBento API key and advanced subscription")]
        public void DownloadsQuoteData(string ticker, SecurityType securityType, string market, Resolution resolution, TickType tickType)
        {
            var symbol = Symbol.Create(ticker, securityType, market);
            var startTime = new DateTime(2024, 1, 15, 9, 30, 0);
            var endTime = new DateTime(2024, 1, 15, 9, 45, 0);
            var param = new DataDownloaderGetParameters(symbol, resolution, startTime, endTime, tickType);

            var downloadResponse = _downloader.Get(param).ToList();

            Log.Trace($"Downloaded {downloadResponse.Count} quote data points for {symbol}");

            Assert.IsTrue(downloadResponse.Any(), "Expected to download quote data");

            foreach (var data in downloadResponse)
            {
                Assert.AreEqual(symbol, data.Symbol, "Symbol should match requested symbol");
                if (data is QuoteBar quoteBar)
                {
                    Assert.IsTrue(quoteBar.Bid != null || quoteBar.Ask != null, "Quote should have bid or ask data");
                }
            }
        }

        [Test]
        [Explicit("This test requires a configured DataBento API key")]
        public void DataIsSortedByTime()
        {
            var symbol = Symbol.Create("ESM3", SecurityType.Future, Market.CME);
            var startTime = new DateTime(2024, 1, 15);
            var endTime = new DateTime(2024, 1, 16);
            var param = new DataDownloaderGetParameters(symbol, Resolution.Minute, startTime, endTime, TickType.Trade);

            var downloadResponse = _downloader.Get(param).ToList();

            Assert.IsTrue(downloadResponse.Any(), "Expected to download data for time sorting test");

            for (int i = 1; i < downloadResponse.Count; i++)
            {
                Assert.GreaterOrEqual(downloadResponse[i].Time, downloadResponse[i - 1].Time,
                    $"Data should be sorted by time. Item {i} time {downloadResponse[i].Time} should be >= item {i-1} time {downloadResponse[i-1].Time}");
            }
        }

        [Test]
        public void DisposesCorrectly()
        {
            var downloader = new DataBentoDataDownloader();
            Assert.DoesNotThrow(() => downloader.Dispose(), "Dispose should not throw");
            Assert.DoesNotThrow(() => downloader.Dispose(), "Multiple dispose calls should not throw");
        }
    }
}
