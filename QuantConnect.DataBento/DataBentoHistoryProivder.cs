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
using QuantConnect.Util;
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

        return history;
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
                if (level.BidPx == null && level.AskPx == null)
                {
                    continue;
                }

                yield return new Tick(time, request.Symbol, level.BidSz, level.BidPx ?? 0m, level.AskSz, level.AskPx ?? 0m);
            }
        }
    }

    private static void LogTrace(string methodName, string message)
    {
        Log.Trace($"{nameof(DataBentoProvider)}.{methodName}: {message}");
    }
}
