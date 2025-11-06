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
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Logging;
using QuantConnect.Util;
using QuantConnect.Lean.DataSource.DataBento;
using QuantConnect.Interfaces;
using System.Collections.Generic;
using QuantConnect.Configuration;
using QuantConnect.Securities;
using System.Threading.Tasks;
using QuantConnect.Data.Consolidators;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// DataBento implementation of <see cref="IHistoryProvider"/>
    /// </summary>
    public partial class DataBentoHistoryProvider : SynchronizingHistoryProvider
    {
        private int _dataPointCount;
        private DataBentoDataDownloader _dataDownloader;
        private volatile bool _invalidStartTimeErrorFired;
        private volatile bool _invalidTickTypeAndResolutionErrorFired;
        private volatile bool _unsupportedTickTypeMessagedLogged;
        private MarketHoursDatabase _marketHoursDatabase;
        private bool _unsupportedSecurityTypeMessageLogged;
        private bool _unsupportedDataTypeMessageLogged;
        private bool _potentialUnsupportedResolutionMessageLogged;
        
        /// <summary>
        /// Gets the total number of data points emitted by this history provider
        /// </summary>
        public override int DataPointCount => _dataPointCount;

        /// <summary>
        /// Initializes this history provider to work for the specified job
        /// </summary>
        /// <param name="parameters">The initialization parameters</param>
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        {
            _dataDownloader = new DataBentoDataDownloader();
            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        }

        /// <summary>
        /// Gets the history for the requested securities
        /// </summary>
        /// <param name="requests">The historical data requests</param>
        /// <param name="sliceTimeZone">The time zone used when time stamping the slice instances</param>
        /// <returns>An enumerable of the slices of data covering the span specified in each request</returns>
        public override IEnumerable<Slice>? GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            var subscriptions = new List<Subscription>();
            foreach (var request in requests)
            {
                var history = GetHistory(request);
                if (history == null)
                {
                    continue;
                }

                var subscription = CreateSubscription(request, history);
                if (!subscription.MoveNext())
                {
                    continue;
                }

                subscriptions.Add(subscription);
            }

            if (subscriptions.Count == 0)
            {
                return null;
            }
            return CreateSliceEnumerableFromSubscriptions(subscriptions, sliceTimeZone);
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of BaseData points</returns>
        public IEnumerable<BaseData>? GetHistory(HistoryRequest request)
        {
            if (request.Symbol.IsCanonical() ||
                !IsSupported(request.Symbol.SecurityType, request.DataType, request.TickType, request.Resolution))
            {
                // It is Logged in IsSupported(...)
                return null;
            }

            if (request.TickType == TickType.OpenInterest)
            {
                if (!_unsupportedTickTypeMessagedLogged)
                {
                    _unsupportedTickTypeMessagedLogged = true;
                    Log.Trace($"DataBentoHistoryProvider.GetHistory(): Unsupported tick type: {TickType.OpenInterest}");
                }
                return null;
            }

            if (request.EndTimeUtc < request.StartTimeUtc)
            {
                if (!_invalidStartTimeErrorFired)
                {
                    _invalidStartTimeErrorFired = true;
                    Log.Error($"{nameof(DataBentoHistoryProvider)}.{nameof(GetHistory)}:InvalidDateRange. The history request start date must precede the end date, no history returned");
                }
                return null;
            }


            // Use the trade aggregates API for resolutions above tick for fastest results
            if (request.TickType == TickType.Trade && request.Resolution > Resolution.Tick)
            {
                var data = GetAggregates(request);

                if (data == null)
                {
                    return null;
                }

                return data;
            }

            return GetHistoryThroughDataConsolidator(request);
        }

        private IEnumerable<BaseData>? GetHistoryThroughDataConsolidator(HistoryRequest request)
        {
            IDataConsolidator consolidator;
            IEnumerable<BaseData> history;

            if (request.TickType == TickType.Trade)
            {
                consolidator = request.Resolution != Resolution.Tick
                    ? new TickConsolidator(request.Resolution.ToTimeSpan())
                    : FilteredIdentityDataConsolidator.ForTickType(request.TickType);
                history = GetTrades(request);
            }
            else
            {
                consolidator = request.Resolution != Resolution.Tick
                    ? new TickQuoteBarConsolidator(request.Resolution.ToTimeSpan())
                    : FilteredIdentityDataConsolidator.ForTickType(request.TickType);
                history = GetQuotes(request);
            }

            BaseData? consolidatedData = null;
            DataConsolidatedHandler onDataConsolidated = (s, e) =>
            {
                consolidatedData = (BaseData)e;
            };
            consolidator.DataConsolidated += onDataConsolidated;

            foreach (var data in history)
            {
                consolidator.Update(data);
                if (consolidatedData != null)
                {
                    Interlocked.Increment(ref _dataPointCount);
                    yield return consolidatedData;
                    consolidatedData = null;
                }
            }

            consolidator.DataConsolidated -= onDataConsolidated;
            consolidator.DisposeSafely();
        }

        /// <summary>
        /// Gets the trade bars for the specified history request
        /// </summary>
        private IEnumerable<TradeBar> GetAggregates(HistoryRequest request)
        {
            var resolutionTimeSpan = request.Resolution.ToTimeSpan();
                        foreach (var date in Time.EachDay(request.StartTimeUtc, request.EndTimeUtc))
            {
                var start = date;
                var end = date + Time.OneDay;

                var parameters = new DataDownloaderGetParameters(request.Symbol, request.Resolution, start, end, request.TickType);
                var data = _dataDownloader.Get(parameters);
                if (data == null) continue;

                foreach (var bar in data)
                {
                    var tradeBar = (TradeBar)bar;
                    if (tradeBar.Time >= request.StartTimeUtc && tradeBar.EndTime <= request.EndTimeUtc)
                    {
                        yield return tradeBar;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the trade ticks that will potentially be aggregated for the specified history request
        /// </summary>
        private IEnumerable<BaseData> GetTrades(HistoryRequest request)
        {
            var parameters = new DataDownloaderGetParameters(request.Symbol, Resolution.Tick, request.StartTimeUtc, request.EndTimeUtc, request.TickType);
            return _dataDownloader.Get(parameters);
        }

        /// <summary>
        /// Gets the quote ticks that will potentially be aggregated for the specified history request
        /// </summary>
        private IEnumerable<BaseData> GetQuotes(HistoryRequest request)
        {
            var parameters = new DataDownloaderGetParameters(request.Symbol, Resolution.Tick, request.StartTimeUtc, request.EndTimeUtc, request.TickType);
            return _dataDownloader.Get(parameters);
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
    }
}
