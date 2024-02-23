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
 */

using System;
using System.Linq;
using System.Collections.Generic;
using NUnit.Framework;
using QuantConnect.Configuration;
using QuantConnect.DateBento;
using QuantConnect.Util;

namespace QuantConnect.DataBento.Tests;

public class DataBentoAdditionalTests
{
}

public class DataBentoDataDownloaderTests
{
    private DataBentoDataDownloader _downloader;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _downloader = new DataBentoDataDownloader();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _downloader.DisposeSafely();
    }

    private static IEnumerable<TestCaseData> HistoricalDataTestCases =>
        DataBentoHistoryProviderTests.HistoricalDataTestCases;

    [TestCaseSource(nameof(HistoricalDataTestCases))]
    [Explicit("This tests require a databento api key, requires internet and do cost money.")]
    public void DownloadsHistoricalData(Symbol symbol, Resolution resolution, TimeSpan period, TickType tickType)
    {
        var request = DataBentoHistoryProviderTests.CreateHistoryRequest(symbol, resolution, tickType, period);

        var parameters =
            new DataDownloaderGetParameters(symbol, resolution, request.StartTimeUtc, request.EndTimeUtc, tickType);
        var downloadResponse = _downloader.Get(parameters).ToList();


        DataBentoHistoryProviderTests.AssertHistoricalDataResults(downloadResponse, resolution);
    }
}