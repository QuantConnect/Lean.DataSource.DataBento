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

using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Logging;
using System.Collections.Generic;
using QuantConnect.Configuration;
using QuantConnect.Lean.DataSource.DataBento.Api;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

[TestFixture]
public class DataBentoHistoricalApiClientTests
{
    private HistoricalAPIClient _client;

    /// <summary>
    /// Dataset for CME Globex futures
    /// https://databento.com/docs/venues-and-datasets has more information on datasets through DataBento
    /// </summary>
    /// <remarks>
    /// TODO: Hard coded for now. Later on can add equities and options with different mapping
    /// </remarks>
    private const string Dataset = "GLBX.MDP3";

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        var apiKey = Config.Get("databento-api-key");
        if (string.IsNullOrEmpty(apiKey))
        {
            Assert.Inconclusive("Please set the 'databento-api-key' in your configuration to enable these tests.");
        }

        _client = new HistoricalAPIClient(apiKey);
    }

    private static IEnumerable<TestCaseData> GetTestCases
    {
        get
        {
            var todayDate = DateTime.UtcNow.Date;
            var tomorrowDate = todayDate.AddDays(1);

            var ESH6 = Securities.Futures.Indices.SP500EMini + "H6"; // March 2026 future contract symbol

            yield return new TestCaseData(ESH6, new DateTime(2010, 01, 11), tomorrowDate, Resolution.Daily, true)
                .SetArgDisplayNames("Invalid: Data Start/End Before/After Available Start/End");
            yield return new TestCaseData(ESH6, new DateTime(2010, 01, 01), new DateTime(2015, 01, 01), Resolution.Daily, false)
                .SetArgDisplayNames("Invalid: Data Start Before Available Start");
            yield return new TestCaseData(ESH6, new DateTime(2010, 06, 06), tomorrowDate, Resolution.Daily, true)
                .SetArgDisplayNames("Invalid: Data End After Available End");
            yield return new TestCaseData(ESH6, new DateTime(2020, 01, 11), new DateTime(2026, 01, 20), Resolution.Hour, true)
                .SetArgDisplayNames("Valid: Hourly Data Within Available Range (2020/01/11 - 2026/01/20)");
            yield return new TestCaseData(ESH6, new DateTime(2026, 01, 11), new DateTime(2026, 01, 20), Resolution.Minute, true)
                .SetArgDisplayNames("Valid: Minute Data Within Available Range");
            yield return new TestCaseData(ESH6, new DateTime(2025, 01, 11), new DateTime(2026, 01, 20), Resolution.Second, true)
                .SetArgDisplayNames("Valid: Second Data Within Available Range");
            yield return new TestCaseData(ESH6, new DateTime(2026, 01, 24, 2, 10, 0), new DateTime(2026, 01, 24, 2, 12, 00), Resolution.Minute, false)
                .SetArgDisplayNames("Invalid: Minute Data at Saturday");
            yield return new TestCaseData(ESH6, new DateTime(2015, 01, 01), new DateTime(2026, 01, 29), Resolution.Minute, true)
                .SetArgDisplayNames("Valid: Minute Data within 11 years range");
            yield return new TestCaseData(ESH6, new DateTime(2015, 01, 01), new DateTime(2026, 01, 29), Resolution.Second, true)
                .SetArgDisplayNames("Valid: Second Data within 11 years range");

            var ESH6_C6875 = Securities.Futures.Indices.SP500EMini + "H6 C6875"; // March 2026 future contract call option symbol
            yield return new TestCaseData(ESH6_C6875, new DateTime(2010, 01, 11), new DateTime(2026, 01, 20), Resolution.Daily, true)
                .SetArgDisplayNames("Invalid: Data Start/End Before/After Available Start/End");
        }
    }

    [TestCaseSource(nameof(GetTestCases))]
    public void GetHistoricalOHLCVBars(string ticker, DateTime startDate, DateTime endDate, Resolution resolution, bool isDataReceived)
    {
        var dataCounter = 0;
        var previousEndTime = DateTime.MinValue;
        var bars = _client.GetHistoricalOhlcvBars(ticker, startDate, endDate, resolution, Dataset).ToList();
        foreach (var data in bars)
        {
            Assert.IsNotNull(data);

            Assert.Greater(data.Open, 0m);
            Assert.Greater(data.High, 0m);
            Assert.Greater(data.Low, 0m);
            Assert.Greater(data.Close, 0m);
            Assert.Greater(data.Volume, 0m);
            Assert.AreNotEqual(default(DateTime), data.Header.UtcTime);

            Assert.IsTrue(data.Header.UtcTime > previousEndTime,
                $"Bar at {data.Header.UtcTime:o} is not after previous bar at {previousEndTime:o}");
            previousEndTime = data.Header.UtcTime;

            dataCounter++;
        }

        Log.Trace($"{nameof(GetHistoricalOHLCVBars)}: {ticker} | [{startDate} - {endDate}] | {resolution} = {dataCounter} (bars)");
        if (isDataReceived)
        {
            Assert.Greater(dataCounter, 0);
        }
        else
        {
            Assert.AreEqual(0, dataCounter);
        }
    }

    [TestCase("ESH6", "2026/01/11", "2026/01/20", Resolution.Minute)]
    public void GetLevelOneDataWithDifferentResolutions(string ticker, DateTime startDate, DateTime endDate, Resolution resolution)
    {
        var dataCounter = 0;
        var previousEndTime = DateTime.MinValue;
        foreach (var data in _client.GetLevelOneData(ticker, startDate, endDate, resolution, Dataset))
        {
            Assert.IsNotNull(data);
            Assert.Greater(data.Price, 0m);
            Assert.Greater(data.Size, 0);
            Assert.AreNotEqual(default(DateTime), data.Header.UtcTime);
            Assert.IsTrue(data.Header.UtcTime > previousEndTime,
                $"Bar at {data.Header.UtcTime:o} is not after previous bar at {previousEndTime:o}");
            previousEndTime = data.Header.UtcTime;
            dataCounter++;
        }
        Log.Trace($"{nameof(GetLevelOneDataWithDifferentResolutions)}: {ticker} | [{startDate} - {endDate}] | {resolution} = {dataCounter} (bars)");
        Assert.Greater(dataCounter, 0);
    }

    [TestCase("ESH6 C6875", "2026/01/11", "2026/01/20", Resolution.Daily)]
    public void ShouldFetchOpenInterest(string ticker, DateTime startDate, DateTime endDate, Resolution resolution)
    {
        var dataCounter = 0;
        var previousEndTime = DateTime.MinValue;
        foreach (var data in _client.GetOpenInterest(ticker, startDate, endDate, Dataset))
        {
            Assert.IsNotNull(data);

            Assert.Greater(data.Quantity, 0m);
            Assert.AreNotEqual(default(DateTime), data.Header.UtcTime);

            Assert.IsTrue(data.Header.UtcTime > previousEndTime,
                $"Bar at {data.Header.UtcTime:o} is not after previous bar at {previousEndTime:o}");
            previousEndTime = data.Header.UtcTime;

            dataCounter++;
        }

        Log.Trace($"{nameof(GetHistoricalOHLCVBars)}: {ticker} | [{startDate} - {endDate}] | {resolution} = {dataCounter} (bars)");
        Assert.Greater(dataCounter, 0);
    }

    [TestCase("ESH6", "2025/01/11", "2026/01/20", Resolution.Tick)]
    public void ShouldFetchTicks(string ticker, DateTime startDate, DateTime endDate, Resolution resolution)
    {
        var dataCounter = 0;
        var previousEndTime = DateTime.MinValue;
        foreach (var data in _client.GetLevelOneData(ticker, startDate, endDate, resolution, Dataset))
        {
            Assert.IsNotNull(data);
            Assert.Greater(data.Price, 0m);
            Assert.Greater(data.Size, 0);
            Assert.AreNotEqual(default(DateTime), data.Header.UtcTime);
            previousEndTime = data.Header.UtcTime;
            dataCounter++;
        }
        Log.Trace($"{nameof(ShouldFetchTicks)}: {ticker} | [{startDate} - {endDate}] | {resolution} = {dataCounter} (ticks)");
        Assert.Greater(dataCounter, 0);
    }
}
