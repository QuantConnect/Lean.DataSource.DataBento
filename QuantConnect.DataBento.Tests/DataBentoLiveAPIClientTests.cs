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
using NUnit.Framework;
using System.Threading;
using QuantConnect.Configuration;
using QuantConnect.Lean.DataSource.DataBento.Api;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

[TestFixture]
public class DataBentoLiveAPIClientTests
{
    /// <summary>
    /// Dataset for CME Globex futures
    /// https://databento.com/docs/venues-and-datasets has more information on datasets through DataBento
    /// </summary>
    /// <remarks>
    /// TODO: Hard coded for now. Later on can add equities and options with different mapping
    /// </remarks>
    private const string Dataset = "GLBX.MDP3";

    private LiveAPIClient _live;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        var apiKey = Config.Get("databento-api-key");
        if (string.IsNullOrEmpty(apiKey))
        {
            Assert.Inconclusive("Please set the 'databento-api-key' in your configuration to enable these tests.");
        }

        _live = new LiveAPIClient(apiKey);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _live.Dispose();
    }

    [Test]
    public void TestExample()
    {
        var dataAvailableEvent = new AutoResetEvent(false);

        _live.Start(Dataset);

        dataAvailableEvent.WaitOne(TimeSpan.FromSeconds(60));
    }
}
