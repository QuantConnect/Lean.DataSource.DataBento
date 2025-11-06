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
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Interfaces;
using System.Collections.Generic;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using System.Threading.Tasks;
using QuantConnect.Data.Market;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// Implementation of Custom Data Provider
    /// </summary>
    public class DataBentoProvider : IDataQueueHandler
    {
        private readonly IDataAggregator _dataAggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
            Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"), forceTypeNameOnExisting: false);
        private EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager = null!;
        private readonly List<SubscriptionDataConfig> _activeSubscriptionConfigs = new();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<Symbol, SubscriptionDataConfig> _subscriptionConfigs = new();
        private DatabentoRawClient _client = null!;
        private readonly string _apiKey;
        private readonly DataBentoDataDownloader _dataDownloader;
        private bool _unsupportedSecurityTypeMessageLogged;
        private bool _unsupportedDataTypeMessageLogged;
        private bool _potentialUnsupportedResolutionMessageLogged;
        private bool _sessionStarted = false;
        private readonly object _sessionLock = new object();
        private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new();
        
        /// <summary>
        /// Returns true if we're currently connected to the Data Provider
        /// </summary>
        public bool IsConnected => _client?.IsConnected == true;

        /// <summary>
        /// Initializes a new instance of the DataBentoProvider
        /// </summary>
        public DataBentoProvider()
        {
            _apiKey = Config.Get("databento-api-key");
            if (string.IsNullOrEmpty(_apiKey))
            {
                throw new ArgumentException("DataBento API key is required. Set 'databento-api-key' in configuration.");
            }

            _dataDownloader = new DataBentoDataDownloader(_apiKey);
            Initialize();
        }

        /// <summary>
        /// Initializes a new instance of the DataBentoProvider with custom API key
        /// </summary>
        /// <param name="apiKey">DataBento API key</param>
        public DataBentoProvider(string apiKey)
        {
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            _dataDownloader = new DataBentoDataDownloader(_apiKey);
            Initialize();
        }

        /// <summary>
        /// Common initialization logic
        /// </summary>
        private void Initialize()
        {
            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl = (symbols, tickType) =>
            {
                foreach (var symbol in symbols)
                {
                    if (!_subscriptionConfigs.TryGetValue(symbol, out var config))
                    {
                        continue;
                    }
                    if (_client?.IsConnected != true)
                    {
                        continue;
                    }
                    Task.Run(async () =>
                    {
                        // If the requested resolution is higher than tick, we subscribe to ticks and let the aggregator handle it.
                        var resolutionToSubscribe = config.Resolution > Resolution.Tick ? Resolution.Tick : config.Resolution;
                        var success = _client.Subscribe(config.Symbol, resolutionToSubscribe, config.TickType);
                        if (!success)
                        {
                            Log.Error($"DataBentoProvider.SubscribeImpl(): Failed to subscribe to live data for {config.Symbol}");
                            return;
                        }

                        // Start session once after first successful subscription
                        lock (_sessionLock)
                        {
                            if (!_sessionStarted)
                            {
                                _sessionStarted = _client.StartSession();
                            }
                        }
                    });
                }
                return true;
            };

            _subscriptionManager.UnsubscribeImpl = (symbols, tickType) =>
            {
                foreach (var symbol in symbols)
                {
                    if (_client?.IsConnected == true)
                    {
                        Task.Run(() =>
                        {
                            _client.Unsubscribe(symbol);
                        });
                    }
                }

                return true;
            };

            // Initialize the live client
            _client = new DatabentoRawClient(_apiKey);
            _client.DataReceived += OnDataReceived;
            _client.ConnectionStatusChanged += OnConnectionStatusChanged;

            // Connect to live gateway
            Task.Run(async () =>
            {
                var connected = await _client.ConnectAsync();
            });
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData>? Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig)){
                return null;
            }

            _subscriptionConfigs[dataConfig.Symbol] = dataConfig;
            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);
            _activeSubscriptionConfigs.Add(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionConfigs.TryRemove(dataConfig.Symbol, out _);
            _subscriptionManager.Unsubscribe(dataConfig);
            var toRemove = _activeSubscriptionConfigs.FirstOrDefault(c => c.Symbol == dataConfig.Symbol && c.TickType == dataConfig.TickType);
            if (toRemove != null)
            {
                _activeSubscriptionConfigs.Remove(toRemove);
            }
            _dataAggregator.Remove(dataConfig);
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
            // No action required for DataBento since the job details are not used in the subscription process.
        }

        /// <summary>
        /// Dispose of unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _dataAggregator?.DisposeSafely();
            _subscriptionManager?.DisposeSafely();
            _client?.Dispose();
            _dataDownloader?.Dispose();
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of BaseData points</returns>
        public IEnumerable<BaseData>? GetHistory(Data.HistoryRequest request)
        {
            if (!CanSubscribe(request.Symbol))
            {
                return null;
            }

            try
            {
                // Use the data downloader to get historical data
                var parameters = new DataDownloaderGetParameters(
                    request.Symbol,
                    request.Resolution,
                    request.StartTimeUtc,
                    request.EndTimeUtc,
                    request.TickType);

                return _dataDownloader.Get(parameters);
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoProvider.GetHistory(): Failed to get history for {request.Symbol}: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Checks if this Data provider supports the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>returns true if Data Provider supports the specified symbol; otherwise false</returns>
        private bool CanSubscribe(Symbol symbol)
        {
            return !symbol.Value.Contains("universe", StringComparison.InvariantCultureIgnoreCase) &&
                   !symbol.IsCanonical() &&
                   IsSecurityTypeSupported(symbol.SecurityType);
        }

        /// <summary>
        /// Determines whether or not the specified config can be subscribed to
        /// </summary>
        private bool CanSubscribe(SubscriptionDataConfig config)
        {
            return CanSubscribe(config.Symbol) &&
                   IsSupported(config.SecurityType, config.Type, config.TickType, config.Resolution);
        }

        /// <summary>
        /// Checks if the security type is supported
        /// </summary>
        /// <param name="securityType">Security type to check</param>
        /// <returns>True if supported</returns>
        private bool IsSecurityTypeSupported(SecurityType securityType)
        {
            // DataBento primarily supports futures, but also has equity and option coverage
            return securityType == SecurityType.Future;
        }

        /// <summary>
        /// Determines if the specified subscription is supported
        /// </summary>
        private bool IsSupported(SecurityType securityType, Type dataType, TickType tickType, Resolution resolution)
        {
            // Check supported security types
            if (!IsSecurityTypeSupported(securityType))
            {
                if (!_unsupportedSecurityTypeMessageLogged)
                {
                    _unsupportedSecurityTypeMessageLogged = true;
                }
                return false;
            }

            // Check supported data types
            if (dataType != typeof(TradeBar) &&
                dataType != typeof(QuoteBar) &&
                dataType != typeof(Tick) &&
                dataType != typeof(OpenInterest))
            {
                if (!_unsupportedDataTypeMessageLogged)
                {
                    _unsupportedDataTypeMessageLogged = true;
                }
                return false;
            }

            // Warn about potential limitations for tick data
            // I'm mimicing polygon implementation with this
            if (!_potentialUnsupportedResolutionMessageLogged)
            {
                _potentialUnsupportedResolutionMessageLogged = true;
            }

            return true;
        }

        /// <summary>
        /// Converts the given UTC time into the symbol security exchange time zone
        /// </summary>
        private DateTime GetTickTime(Symbol symbol, DateTime utcTime)
        {
            var exchangeTimeZone = _symbolExchangeTimeZones.GetOrAdd(symbol, sym =>
            {
                if (_marketHoursDatabase.TryGetEntry(sym.ID.Market, sym, sym.SecurityType, out var entry))
                {
                    return entry.ExchangeHours.TimeZone;
                }
                // Futures default to Chicago
                return TimeZones.Chicago;
            });

            return utcTime.ConvertFromUtc(exchangeTimeZone);
        }

        // <summary>
        /// Handles data received from the live client
        /// </summary>
        private void OnDataReceived(object? sender, BaseData data)
        {
            try
            {
                if (data is Tick tick)
                {
                    tick.Time = GetTickTime(tick.Symbol, tick.Time);
                    _dataAggregator.Update(tick);
                }
                else if (data is TradeBar tradeBar)
                {
                    tradeBar.Time = GetTickTime(tradeBar.Symbol, tradeBar.Time);
                    tradeBar.EndTime = GetTickTime(tradeBar.Symbol, tradeBar.EndTime);
                    _dataAggregator.Update(tradeBar);
                }
                else
                {
                    data.Time = GetTickTime(data.Symbol, data.Time);
                    _dataAggregator.Update(data);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"DataBentoProvider.OnDataReceived(): Error updating data aggregator: {ex.Message}\n{ex.StackTrace}");
            }
        }

        /// <summary>
        /// Handles connection status changes from the live client
        /// </summary>
        private void OnConnectionStatusChanged(object? sender, bool isConnected)
        {
            if (isConnected)
            {
                // Reset session flag on reconnection
                lock (_sessionLock)
                {
                    _sessionStarted = false;
                }
                
                // Resubscribe to all active subscriptions
                Task.Run(() =>
                {
                    foreach (var config in _activeSubscriptionConfigs)
                    {
                        _client.Subscribe(config.Symbol, config.Resolution, config.TickType);
                    }
                    
                    // Start session after resubscribing
                    if (_activeSubscriptionConfigs.Any())
                    {
                        lock (_sessionLock)
                        {
                            if (!_sessionStarted)
                            {
                                _sessionStarted = _client.StartSession();
                            }
                        }
                    }
                });
            }
        }
    }
}
