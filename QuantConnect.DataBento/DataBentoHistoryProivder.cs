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

using QuantConnect.Data;
using QuantConnect.Logging;
using QuantConnect.Data.Market;
using QuantConnect.Data.Consolidators;
using QuantConnect.Lean.Engine.HistoricalData;

namespace QuantConnect.Lean.DataSource.DataBento;

/// <summary>
/// Implements a history provider for DataBento historical data.
/// Uses consolidators to produce the requested resolution when necessary.
/// </summary>
public partial class DataBentoProvider : MappedSynchronizingHistoryProvider
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
    /// Gets the history for the requested security
    /// </summary>
    /// <param name="historyRequest">The historical data request</param>
    /// <returns>An enumerable of BaseData points</returns>
    public override IEnumerable<BaseData>? GetHistory(HistoryRequest historyRequest)
    {
        if (!CanSubscribe(historyRequest.Symbol))
        {
            if (!_invalidSecurityTypeWarningFired)
            {
                _invalidSecurityTypeWarningFired = true;
                Log.Trace($"{nameof(DataBentoProvider)}.{nameof(GetHistory)}:" +
                    $"Unsupported SecurityType '{historyRequest.Symbol.SecurityType}' for symbol '{historyRequest.Symbol}'.");
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

        if (!_symbolMapper.DataBentoDataSetByLeanMarket.TryGetValue(historyRequest.Symbol.ID.Market, out var dataSetSpecifications) || dataSetSpecifications == null)
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

        if (dataSetSpecifications.TryGetDelayWarningMessage(out var message))
        {
            Log.Trace(message);
        }
        var dataSet = dataSetSpecifications.DataSetID;

        var history = default(IEnumerable<BaseData>);
        var brokerageSymbol = _symbolMapper.GetBrokerageSymbol(historyRequest.Symbol);
        switch (historyRequest.TickType)
        {
            case TickType.Trade when historyRequest.Resolution == Resolution.Tick:
                history = GetTradeTicks(historyRequest, brokerageSymbol, dataSet);
                break;
            case TickType.Trade:
                history = GetAggregatedTradeBars(historyRequest, brokerageSymbol, dataSet);
                break;
            case TickType.Quote when historyRequest.Resolution == Resolution.Tick:
                history = GetQuoteTicks(historyRequest, brokerageSymbol, dataSet);
                break;
            case TickType.Quote when historyRequest is { Resolution: Resolution.Second or Resolution.Minute }:
                history = GetIntraDayQuoteBars(historyRequest, historyRequest.Resolution, brokerageSymbol, dataSet);
                break;
            case TickType.Quote:
                history = GetInterDayQuoteBars(historyRequest, brokerageSymbol, dataSet);
                break;
            case TickType.OpenInterest:
                history = GetOpenInterestBars(historyRequest, brokerageSymbol, dataSet);
                break;
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

    /// <summary>
    /// Gets the trade ticks that will potentially be aggregated for the specified history request
    /// </summary>
    private IEnumerable<BaseData> GetTradeTicks(HistoryRequest request, string brokerageSymbol, string dataBentoDataSet)
    {
        foreach (var t in _historicalApiClient.GetLevelOneData(brokerageSymbol, request.StartTimeUtc, request.EndTimeUtc, Resolution.Tick, dataBentoDataSet))
        {
            if (t.Price.HasValue)
            {
                yield return new Tick(t.Header.UtcTime.ConvertFromUtc(request.DataTimeZone), request.Symbol, "", "", t.Size, t.Price.Value);
            }
        }
    }

    private IEnumerable<BaseData>? GetInterDayQuoteBars(HistoryRequest request, string brokerageSymbol, string dataBentoDataSet)
    {
        using var consolidator = new QuoteBarConsolidator(request.Resolution.ToTimeSpan());

        var consolidatedData = default(BaseData);
        void OnConsolidated(object? _, QuoteBar bar)
        {
            consolidatedData = bar;
        }
        consolidator.DataConsolidated += OnConsolidated;

        try
        {
            foreach (var data in GetIntraDayQuoteBars(request, Resolution.Minute, brokerageSymbol, dataBentoDataSet))
            {
                consolidator.Update(data);
                if (consolidatedData != null)
                {
                    yield return consolidatedData;
                    consolidatedData = null;
                }
            }
        }
        finally
        {
            consolidator.DataConsolidated -= OnConsolidated;
        }
    }

    /// <summary>
    /// Gets the quote ticks that will potentially be aggregated for the specified history request
    /// </summary>
    private IEnumerable<BaseData> GetIntraDayQuoteBars(HistoryRequest request, Resolution resolution, string brokerageSymbol, string dataBentoDataSet)
    {
        var period = resolution.ToTimeSpan();
        foreach (var q in _historicalApiClient.GetLevelOneData(brokerageSymbol, request.StartTimeUtc, request.EndTimeUtc, resolution, dataBentoDataSet))
        {
            var time = q.Header.UtcTime.ConvertFromUtc(request.DataTimeZone);
            var topLevel = q.Levels.Single();

            if (!topLevel.HasBidOrAskPrice())
            {
                continue;
            }

            var bar = new QuoteBar(time, request.Symbol, bid: null, lastBidSize: decimal.Zero, ask: null, lastAskSize: decimal.Zero, period);

            if (topLevel.BidPx.HasValue)
            {
                bar.UpdateBid(topLevel.BidPx.Value, topLevel.BidSz);
            }

            if (topLevel.AskPx.HasValue)
            {
                bar.UpdateAsk(topLevel.AskPx.Value, topLevel.AskSz);

            }

            yield return bar;
        }
    }

    private IEnumerable<BaseData> GetQuoteTicks(HistoryRequest request, string brokerageSymbol, string dataBentoDataSet)
    {
        foreach (var q in _historicalApiClient.GetLevelOneData(brokerageSymbol, request.StartTimeUtc, request.EndTimeUtc, Resolution.Tick, dataBentoDataSet))
        {
            var time = q.Header.UtcTime.ConvertFromUtc(request.DataTimeZone);
            var topLevel = q.Levels.Single();

            if (!topLevel.HasBidOrAskPrice())
            {
                continue;
            }

            yield return new Tick(time, request.Symbol, topLevel.BidSz, topLevel.BidPx ?? 0m, topLevel.AskSz, topLevel.AskPx ?? 0m);
        }
    }
}
