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

using NodaTime;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using QuantConnect.Data.Consolidators;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.HistoricalData;

namespace QuantConnect.Lean.DataSource.DataBento;

/// <summary>
/// Implements a history provider for DataBento historical data.
/// Uses consolidators to produce the requested resolution when necessary.
/// </summary>
public partial class DataBentoProvider : SynchronizingHistoryProvider
{
    private static int _dataPointCount;

    /// <summary>
    /// Indicates whether a error for an invalid start time has been fired, where the start time is greater than or equal to the end time in UTC.
    /// </summary>
    private volatile bool _invalidStartTimeErrorFired;

    /// <summary>
    /// Indicates whether the warning for invalid <see cref="SecurityType"/> has been fired.
    /// </summary>
    private volatile bool _invalidSecurityTypeWarningFired;

    /// <summary>
    /// Indicates whether a DataBento dataset error has already been logged.
    /// </summary>
    private bool _dataBentoDatasetErrorFired;

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
            var history = request.SplitHistoryRequestWithUpdatedMappedSymbol(_mapFileProvider).SelectMany(x => GetHistory(x) ?? []);

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
    /// <param name="historyRequest">The historical data request</param>
    /// <returns>An enumerable of BaseData points</returns>
    public IEnumerable<BaseData>? GetHistory(HistoryRequest historyRequest)
    {
        if (!CanSubscribe(historyRequest.Symbol))
        {
            if (!_invalidSecurityTypeWarningFired)
            {
                _invalidSecurityTypeWarningFired = true;
                LogTrace(nameof(GetHistory), $"Unsupported SecurityType '{historyRequest.Symbol.SecurityType}' for symbol '{historyRequest.Symbol}'.");
            }
            return null;
        }

        if (historyRequest.EndTimeUtc < historyRequest.StartTimeUtc)
        {
            if (!_invalidStartTimeErrorFired)
            {
                _invalidStartTimeErrorFired = true;
                Log.Error($"{nameof(DataBentoProvider)}.{nameof(GetHistory)}: Invalid date range: the start date must be earlier than the end date.");
            }
            return null;
        }

        if (!TryGetDataBentoDataSet(historyRequest.Symbol, out var dataSet) || dataSet == null)
        {
            if (!_dataBentoDatasetErrorFired)
            {
                _dataBentoDatasetErrorFired = true;
                Log.Error($"{nameof(DataBentoProvider)}.{nameof(GetHistory)}: " +
                    $"DataBento dataset not found for symbol '{historyRequest.Symbol.Value}, Market = {historyRequest.Symbol.ID.Market}."
                );
            }
            return null;
        }

        var history = default(IEnumerable<BaseData>);
        var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol);
        switch (historyRequest.TickType)
        {
            case TickType.Trade when historyRequest.Resolution == Resolution.Tick:
                history = GetHistoryThroughDataConsolidator(historyRequest, brokerageSymbol, dataSet);
                break;
            case TickType.Trade:
                history = GetAggregatedTradeBars(historyRequest, brokerageSymbol, dataSet);
                break;
            case TickType.Quote:
                history = GetHistoryThroughDataConsolidator(historyRequest, brokerageSymbol, dataSet);
                break;
            case TickType.OpenInterest:
                history = GetOpenInterestBars(historyRequest, brokerageSymbol, dataSet);
                break;
            default:
                throw new ArgumentException("");
        }

        if (history == null)
        {
            return null;
        }

        return FilterHistory(history, historyRequest, historyRequest.StartTimeLocal, historyRequest.EndTimeLocal);
    }

    private static IEnumerable<BaseData> FilterHistory(IEnumerable<BaseData> history, HistoryRequest request, DateTime startTimeLocal, DateTime endTimeLocal)
    {
        // cleaning the data before returning it back to user
        foreach (var bar in history)
        {
            if (bar.Time >= startTimeLocal && bar.EndTime <= endTimeLocal)
            {
                if (request.ExchangeHours.IsOpen(bar.Time, bar.EndTime, request.IncludeExtendedMarketHours))
                {
                    Interlocked.Increment(ref _dataPointCount);
                    yield return bar;
                }
            }
        }
    }

    private IEnumerable<BaseData> GetOpenInterestBars(HistoryRequest request, string brokerageSymbol, string dataBentoDataSet)
    {
        foreach (var oi in _historicalApiClient.GetOpenInterest(brokerageSymbol, request.StartTimeUtc, request.EndTimeUtc, dataBentoDataSet))
        {
            yield return new OpenInterest(oi.Header.UtcTime.ConvertFromUtc(request.DataTimeZone), request.Symbol, oi.Quantity);
        }
    }

    private IEnumerable<BaseData> GetAggregatedTradeBars(HistoryRequest request, string brokerageSymbol, string dataBentoDataSet)
    {
        var period = request.Resolution.ToTimeSpan();
        foreach (var b in _historicalApiClient.GetHistoricalOhlcvBars(brokerageSymbol, request.StartTimeUtc, request.EndTimeUtc, request.Resolution, dataBentoDataSet))
        {
            yield return new TradeBar(b.Header.UtcTime.ConvertFromUtc(request.DataTimeZone), request.Symbol, b.Open, b.High, b.Low, b.Close, b.Volume, period);
        }
    }

    private IEnumerable<BaseData>? GetHistoryThroughDataConsolidator(HistoryRequest request, string brokerageSymbol, string dataBentoDataSet)
    {
        IDataConsolidator consolidator;
        IEnumerable<BaseData> history;

        if (request.TickType == TickType.Trade)
        {
            consolidator = request.Resolution != Resolution.Tick
                ? new TickConsolidator(request.Resolution.ToTimeSpan())
                : FilteredIdentityDataConsolidator.ForTickType(request.TickType);
            history = GetTrades(request, brokerageSymbol, dataBentoDataSet);
        }
        else
        {
            consolidator = request.Resolution != Resolution.Tick
                ? new TickQuoteBarConsolidator(request.Resolution.ToTimeSpan())
                : FilteredIdentityDataConsolidator.ForTickType(request.TickType);
            history = GetQuotes(request, brokerageSymbol, dataBentoDataSet);
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
                yield return consolidatedData;
                consolidatedData = null;
            }
        }

        consolidator.DataConsolidated -= onDataConsolidated;
        consolidator.DisposeSafely();
    }

    /// <summary>
    /// Gets the trade ticks that will potentially be aggregated for the specified history request
    /// </summary>
    private IEnumerable<BaseData> GetTrades(HistoryRequest request, string brokerageSymbol, string dataBentoDataSet)
    {
        foreach (var t in _historicalApiClient.GetTickBars(brokerageSymbol, request.StartTimeUtc, request.EndTimeUtc, dataBentoDataSet))
        {
            yield return new Tick(t.Header.UtcTime.ConvertFromUtc(request.DataTimeZone), request.Symbol, "", "", t.Size, t.Price);
        }
    }

    /// <summary>
    /// Gets the quote ticks that will potentially be aggregated for the specified history request
    /// </summary>
    private IEnumerable<BaseData> GetQuotes(HistoryRequest request, string brokerageSymbol, string dataBentoDataSet)
    {
        foreach (var quoteBar in _historicalApiClient.GetTickBars(brokerageSymbol, request.StartTimeUtc, request.EndTimeUtc, dataBentoDataSet))
        {
            var time = quoteBar.Header.UtcTime.ConvertFromUtc(request.DataTimeZone);
            foreach (var level in quoteBar.Levels)
            {
                yield return new Tick(time, request.Symbol, level.BidSz, level.BidPx, level.AskSz, level.AskPx);
            }
        }
    }

    private static void LogTrace(string methodName, string message)
    {
        Log.Trace($"{nameof(DataBentoProvider)}.{methodName}: {message}");
    }
}
