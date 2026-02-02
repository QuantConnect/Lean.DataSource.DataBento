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
using NodaTime;
using NUnit.Framework;
using QuantConnect.Util;
using QuantConnect.Data;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

[TestFixture]
public class DataBentoHistoryProviderTests
{
    private DataBentoProvider _historyDataProvider;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        var apiKey = Config.Get("databento-api-key");

        if (string.IsNullOrEmpty(apiKey))
        {
            Assert.Fail("DataBento API key is not set. Please set 'databento-api-key' in the configuration to run these tests.");
        }

        _historyDataProvider = new DataBentoProvider(apiKey);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _historyDataProvider?.Dispose();
    }

    private static IEnumerable<TestCaseData> HistoryTestParameters
    {
        get
        {
            var sp500EMiniMarch = Symbol.CreateFuture(Futures.Indices.SP500EMini, Market.CME, new(2026, 03, 20));

            // New York DateTimeZone
            yield return new TestCaseData(sp500EMiniMarch, Resolution.Hour, TickType.Quote, new DateTime(2026, 1, 23, 9, 0, 0), new DateTime(2026, 1, 27, 16, 0, 0), false).SetArgDisplayNames($"{sp500EMiniMarch.Value}|Quote|Hour|2026/01/23 9:00 - 2026/01/27 16:00");
            yield return new TestCaseData(sp500EMiniMarch, Resolution.Daily, TickType.Quote, new DateTime(2026, 1, 23), new DateTime(2026, 1, 27), false).SetArgDisplayNames($"{sp500EMiniMarch.Value}|Quote|Daily|2026/01/23 - 2026/01/27");
            yield return new TestCaseData(sp500EMiniMarch, Resolution.Daily, TickType.OpenInterest, new DateTime(2026, 1, 23), new DateTime(2026, 1, 27), false).SetArgDisplayNames($"{sp500EMiniMarch.Value}|OpenInterest|2026/01/26 - 2026/01/26");
        }
    }

    [TestCaseSource(nameof(HistoryTestParameters))]
    public void GetsHistory(Symbol symbol, Resolution resolution, TickType tickType, DateTime startDateTime, DateTime endDateTime, bool isNullResult)
    {
        var historyRequest = CreateHistoryRequest(symbol, resolution, tickType, startDateTime, endDateTime);

        var history = _historyDataProvider.GetHistory(historyRequest);

        if (isNullResult)
        {
            Assert.IsNull(history);
        }
        else
        {
            Assert.IsNotNull(history);
            Assert.IsNotEmpty(history);

            var index = 0;
            var previousTime = DateTime.MinValue;
            foreach (var bar in history)
            {
                LogBaseData(index, bar);
                Assert.IsNotNull(bar);
                Assert.IsTrue(bar.Time > previousTime, $"Bar at {bar.Time:o} is not after previous bar at {previousTime:o}");
                previousTime = bar.Time;
                index += 1;
            }
        }
    }

    private void LogBaseData(int index, BaseData data)
    {
        switch (data)
        {
            case OpenInterest oi:
                Log.Trace($"#{index} DataType: {oi.DataType} {oi.TickType} | " + oi.ToString() + $" Time: {oi.Time}, EndTime: {oi.EndTime}, FF: {oi.IsFillForward}");
                break;
            case QuoteBar qb:
                Log.Trace($"#{index} Data Type: {qb.DataType} | " + qb.ToString() + $" Time: {qb.Time}, EndTime: {qb.EndTime}, FF: {qb.IsFillForward}");
                break;
        }
    }

    private static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, DateTime startDateTime, DateTime endDateTime, SecurityExchangeHours exchangeHours = null, DateTimeZone dataTimeZone = null)
    {
        if (exchangeHours == null)
        {
            exchangeHours = SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork);
        }

        if (dataTimeZone == null)
        {
            dataTimeZone = TimeZones.NewYork;
        }

        var dataType = LeanData.GetDataType(resolution, tickType);
        return new HistoryRequest(
            startDateTime,
            endDateTime,
            dataType,
            symbol,
            resolution,
            exchangeHours,
            dataTimeZone,
            null,
            true,
            false,
            DataNormalizationMode.Adjusted,
            tickType
            );
    }
}
