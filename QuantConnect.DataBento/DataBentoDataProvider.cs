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
        /// <summary>
        /// <inheritdoc cref="IDataAggregator"/>
        /// </summary>
        private readonly IDataAggregator _dataAggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
            Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"), forceTypeNameOnExisting: false);

        /// <summary>
        /// <inheritdoc cref="EventBasedDataQueueHandlerSubscriptionManager"/>
        /// </summary>
        private EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager = null!;

        private readonly List<SubscriptionDataConfig> _activeSubscriptionConfigs = new();

        private readonly System.Collections.Concurrent.ConcurrentDictionary<Symbol, SubscriptionDataConfig> _subscriptionConfigs = new();

        /// <summary>
        /// <inheritdoc cref="DatabentoRawClient"/>
        /// </summary>
        private DatabentoRawClient _client = null!;

        /// <summary>
        /// DataBento API key
        /// </summary>
        private readonly string _apiKey;

        /// <summary>
        /// DataBento historical data downloader
        /// </summary>
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
            Log.Trace("From Plugin DataBentoProvider.DataBentoProvider() being initialized 1");
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
            Log.Trace("From Plugin DataBentoProvider.DataBentoProvider() being initialized 2");
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            _dataDownloader = new DataBentoDataDownloader(_apiKey);
            Initialize();
        }

        /// <summary>
        /// Common initialization logic
        /// </summary>
        private void Initialize()
        {
            Log.Trace("DataBentoProvider.Initialize(): Starting initialization");

            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl = (symbols, tickType) =>
            {
                Log.Trace($"DataBentoProvider.SubscribeImpl(): Received subscription request for {symbols.Count()} symbols, TickType={tickType}");

                foreach (var symbol in symbols)
                {
                    Log.Trace($"DataBentoProvider.SubscribeImpl(): Processing symbol {symbol}");

                    if (_subscriptionConfigs.TryGetValue(symbol, out var config))
                    {
                        Log.Trace($"DataBentoProvider.SubscribeImpl(): Found config for {symbol}, Resolution={config.Resolution}, TickType={config.TickType}");

                        if (_client?.IsConnected == true)
                        {
                            Log.Trace($"DataBentoProvider.SubscribeImpl(): Client is connected, attempting async subscribe for {symbol}");
                            Task.Run(async () =>
                            {
                                var success = _client.Subscribe(config.Symbol, config.Resolution, config.TickType);
                                if (success)
                                {
                                    Log.Trace($"DataBentoProvider.SubscribeImpl(): Successfully subscribed to {config.Symbol}");
                                    
                                    // Start session after first successful subscription
                                    lock (_sessionLock)
                                    {
                                        if (!_sessionStarted)
                                        {
                                            Log.Trace("DataBentoProvider.SubscribeImpl(): Starting DataBento session to receive data");
                                            _sessionStarted = _client.StartSession();
                                            if (_sessionStarted)
                                            {
                                                Log.Trace("DataBentoProvider.SubscribeImpl(): Session started successfully - data should begin flowing");
                                            }
                                            else
                                            {
                                                Log.Error("DataBentoProvider.SubscribeImpl(): Failed to start session");
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    Log.Error($"DataBentoProvider.SubscribeImpl(): Failed to subscribe to live data for {config.Symbol}");
                                }
                            });
                        }
                        else
                        {
                            Log.Trace($"DataBentoProvider.SubscribeImpl(): Client not connected, skipping subscription for {symbol}");
                        }
                    }
                    else
                    {
                        Log.Trace($"DataBentoProvider.SubscribeImpl(): No config found for {symbol}, skipping");
                    }
                }

                return true;
            };

            _subscriptionManager.UnsubscribeImpl = (symbols, tickType) =>
            {
                Log.Trace($"DataBentoProvider.UnsubscribeImpl(): Received unsubscribe request for {symbols.Count()} symbols, TickType={tickType}");

                foreach (var symbol in symbols)
                {
                    Log.Trace($"DataBentoProvider.UnsubscribeImpl(): Processing symbol {symbol}");

                    if (_client?.IsConnected == true)
                    {
                        Log.Trace($"DataBentoProvider.UnsubscribeImpl(): Client is connected, unsubscribing from {symbol}");
                        Task.Run(() =>
                        {
                            _client.Unsubscribe(symbol);
                            Log.Trace($"DataBentoProvider.UnsubscribeImpl(): Unsubscribed from {symbol}");
                        });
                    }
                    else
                    {
                        Log.Trace($"DataBentoProvider.UnsubscribeImpl(): Client not connected, skipping unsubscribe for {symbol}");
                    }
                }

                return true;
            };

            // Initialize the live client
            Log.Trace("DataBentoProvider.Initialize(): Creating DatabentoRawClient");
            _client = new DatabentoRawClient(_apiKey);
            _client.DataReceived += OnDataReceived;
            _client.ConnectionStatusChanged += OnConnectionStatusChanged;

            // Connect to live gateway
            Log.Trace("DataBentoProvider.Initialize(): Attempting async connection to DataBento live gateway");
            Task.Run(async () =>
            {
                var connected = await _client.ConnectAsync();
                Log.Trace($"DataBentoProvider.Initialize(): ConnectAsync() returned {connected}");

                if (connected)
                {
                    Log.Trace("DataBentoProvider.Initialize(): Successfully connected to DataBento live gateway");
                }
                else
                {
                    Log.Error("DataBentoProvider.Initialize(): Failed to connect to DataBento live gateway");
                }
            });

            Log.Trace("DataBentoProvider.Initialize(): Initialization complete");
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData>? Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            Log.Trace("From Plugin Subscribed ENTER");
            if (!CanSubscribe(dataConfig)){
                return null;
            }

            _subscriptionConfigs[dataConfig.Symbol] = dataConfig;
            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);
            _activeSubscriptionConfigs.Add(dataConfig);

            Log.Trace("From Plugin Subscribed DONE");
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
            // Not sure what to do in here.
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
                    Log.Trace($"DataBentoDataProvider.IsSupported(): Unsupported security type: {securityType}");
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
                    Log.Trace($"DataBentoDataProvider.IsSupported(): Unsupported data type: {dataType}");
                }
                return false;
            }

            // Warn about potential limitations for tick data
            // I'm mimicing polygon implementation with this
            if (!_potentialUnsupportedResolutionMessageLogged)
            {
                _potentialUnsupportedResolutionMessageLogged = true;
                Log.Trace("DataBentoDataProvider.IsSupported(): " +
                    $"Subscription for {securityType}-{dataType}-{tickType}-{resolution} will be attempted. " +
                    $"An Advanced DataBento subscription plan is required to stream tick data.");
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
                    
                    Log.Trace($"DataBentoProvider.OnDataReceived(): Updated tick - Symbol: {tick.Symbol}, " +
                            $"TickType: {tick.TickType}, Price: {tick.Value}, Quantity: {tick.Quantity}");
                }
                else if (data is TradeBar tradeBar)
                {
                    tradeBar.Time = GetTickTime(tradeBar.Symbol, tradeBar.Time);
                    tradeBar.EndTime = GetTickTime(tradeBar.Symbol, tradeBar.EndTime);
                    _dataAggregator.Update(tradeBar);
                    
                    Log.Trace($"DataBentoProvider.OnDataReceived(): Updated TradeBar - Symbol: {tradeBar.Symbol}, " +
                            $"O:{tradeBar.Open} H:{tradeBar.High} L:{tradeBar.Low} C:{tradeBar.Close} V:{tradeBar.Volume}");
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
            Log.Trace($"DataBentoProvider.OnConnectionStatusChanged(): Connection status changed to: {isConnected}");

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
                                Log.Trace("DataBentoProvider.OnConnectionStatusChanged(): Starting session after reconnection");
                                _sessionStarted = _client.StartSession();
                            }
                        }
                    }
                });
            }
        }
    }
}
