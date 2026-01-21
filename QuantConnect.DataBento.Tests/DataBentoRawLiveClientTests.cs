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
using System.Threading;
using NUnit.Framework;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.DataSource.DataBento;
using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.DataBento.Tests
{
    [TestFixture]
    public class DataBentoRawLiveClientSyncTests
    {
        private DataBentoRawLiveClient _client;
        protected readonly string ApiKey = Config.Get("databento-api-key");

        private static Symbol CreateEsFuture()
        {
            var expiration = new DateTime(2026, 3, 20);
            return Symbol.CreateFuture("ES", Market.CME, expiration);
        }

        [SetUp]
        public void SetUp()
        {
            Log.Trace("DataBentoLiveClientTests: Using API Key: " + ApiKey);
            _client = new DataBentoRawLiveClient(ApiKey);
        }

        [TearDown]
        public void TearDown()
        {
            _client?.Dispose();
        }

        [Test]
        public void Connects()
        {
            var connected = _client.Connect();

            Assert.IsTrue(connected);
            Assert.IsTrue(_client.IsConnected);

            Log.Trace("Connected successfully");
        }

        [Test]
        public void SubscribesToLeanFutureSymbol()
        {
            Assert.IsTrue(_client.Connect());

            var symbol = CreateEsFuture();

            Assert.IsTrue(_client.Subscribe(symbol, TickType.Trade));
            Assert.IsTrue(_client.StartSession());

            Thread.Sleep(1000);

            Assert.IsTrue(_client.Unsubscribe(symbol));
        }

        [Test]
        public void ReceivesTradeOrQuoteTicks()
        {
            var receivedEvent = new ManualResetEventSlim(false);
            BaseData received = null;

            _client.DataReceived += (_, data) =>
            {
                received = data;
                receivedEvent.Set();
            };

            Assert.IsTrue(_client.Connect());

            var symbol = CreateEsFuture();

            Assert.IsTrue(_client.Subscribe(symbol, TickType.Trade));
            Assert.IsTrue(_client.StartSession());

            var gotData = receivedEvent.Wait(TimeSpan.FromMinutes(2));

            if (!gotData)
            {
                Assert.Inconclusive("No data received (likely outside market hours)");
                return;
            }

            Assert.NotNull(received);
            Assert.AreEqual(symbol, received.Symbol);

            if (received is Tick tick)
            {
                Assert.Greater(tick.Time, DateTime.MinValue);
                Assert.Greater(tick.Value, 0);
            }
            else if (received is TradeBar bar)
            {
                Assert.Greater(bar.Close, 0);
            }
            else
            {
                Assert.Fail($"Unexpected data type: {received.GetType()}");
            }
        }

        [Test]
        public void DisposeIsIdempotent()
        {
            var client = new DataBentoRawLiveClient(ApiKey);
            Assert.DoesNotThrow(client.Dispose);
            Assert.DoesNotThrow(client.Dispose);
        }

        [Test]
        public void SymbolMappingDoesNotThrow()
        {
            Assert.IsTrue(_client.Connect());

            var symbol = CreateEsFuture();

            Assert.DoesNotThrow(() =>
            {
                _client.Subscribe(symbol, TickType.Trade);
                _client.StartSession();
                Thread.Sleep(500);
                _client.Unsubscribe(symbol);
            });
        }
    }
}
