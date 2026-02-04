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

using System.Net;
using System.Text;
using QuantConnect.Util;
using QuantConnect.Logging;
using System.Net.Http.Headers;
using QuantConnect.Lean.DataSource.DataBento.Models;

namespace QuantConnect.Lean.DataSource.DataBento.Api;

public class HistoricalAPIClient : IDisposable
{
    /// <summary>
    /// OHLCV bars aggregated to 1-second intervals.
    /// </summary>
    /// <remarks>Docs: <see href="https://databento.com/docs/schemas-and-data-formats/ohlcv"/></remarks>
    private const string OHLCV1sSchema = "ohlcv-1s";

    /// <summary>
    /// OHLCV bars aggregated to 1-minute intervals.
    /// </summary>
    /// <remarks>Docs: <see href="https://databento.com/docs/schemas-and-data-formats/ohlcv"/></remarks>
    private const string OHLCV1mSchema = "ohlcv-1m";

    /// <summary>
    /// OHLCV bars aggregated to 1-hour intervals.
    /// </summary>
    /// <remarks>Docs: <see href="https://databento.com/docs/schemas-and-data-formats/ohlcv"/></remarks>
    private const string OHLCV1hSchema = "ohlcv-1h";

    /// <summary>
    /// OHLCV bars aggregated to 1-day intervals.
    /// </summary>
    /// <remarks>Docs: <see href="https://databento.com/docs/schemas-and-data-formats/ohlcv"/></remarks>
    private const string OHLCV1dSchema = "ohlcv-1d";

    /// <summary>
    /// Market-by-price data with top-of-book depth (MBP-1).
    /// </summary>
    /// <remarks>Docs: <see href="https://databento.com/docs/schemas-and-data-formats/mbp-1"/></remarks>
    private const string MBP1Schema = "mbp-1";

    /// <summary>
    /// BBO on interval (BBO) provides the last best bid, best offer, and sale at 1-second. This is a subset of MBP-1.
    /// </summary>
    /// <remarks>https://databento.com/docs/schemas-and-data-formats/bbo</remarks>
    private const string BBO1sSchema = "bbo-1s";

    /// <summary>
    /// BBO on interval (BBO) provides the last best bid, best offer, and sale at 1-minute intervals. This is a subset of MBP-1.
    /// </summary>
    /// <remarks>https://databento.com/docs/schemas-and-data-formats/bbo</remarks>
    private const string BBO1mSchema = "bbo-1m";

    private bool _dataStartBeforeAvailableStartErrorFired;
    private bool _dataEndAfterAvailableEndErrorFired;
    private bool _dataTimeRangeStartOnOrAfterEndErrorFired;
    private bool _dataStartAfterAvailableEndErrorFired;

    /// <summary>
    /// Market statistics data (e.g. volume, trades, session statistics).
    /// </summary>
    /// <remarks>Docs: <see href="https://databento.com/docs/schemas-and-data-formats/statistics"/></remarks>
    private const string StatisticsSchema = "statistics";

    private readonly HttpClient _httpClient = new()
    {
        BaseAddress = new Uri("https://hist.databento.com")
    };

    public HistoricalAPIClient(string apiKey)
    {
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            AuthenticationSchemes.Basic.ToString(),
            // Basic Auth expects "username:password". Using ":" means API key with an empty password.
            Convert.ToBase64String(Encoding.UTF8.GetBytes($"{apiKey}:"))
            );
    }

    public IEnumerable<OpenHighLowCloseVolumeData> GetHistoricalOhlcvBars(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, Resolution resolution, string dataSet)
    {
        string schema;
        switch (resolution)
        {
            case Resolution.Second:
                schema = OHLCV1sSchema;
                break;
            case Resolution.Minute:
                schema = OHLCV1mSchema;
                break;
            case Resolution.Hour:
                schema = OHLCV1hSchema;
                break;
            case Resolution.Daily:
                schema = OHLCV1dSchema;
                break;
            default:
                throw new ArgumentException($"Unsupported resolution {resolution} in GetHistoricalOhlcvBars.");
        }

        return GetRange<OpenHighLowCloseVolumeData>(symbol, startDateTimeUtc, endDateTimeUtc, schema, dataSet);
    }

    public IEnumerable<LevelOneData> GetLevelOneData(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, string dataSet)
    {
        return GetRange<LevelOneData>(symbol, startDateTimeUtc, endDateTimeUtc, MBP1Schema, dataSet);
    }

    public IEnumerable<BestBidOfferInterval> GetBestBidOfferIntervals(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, Resolution resolution, string dataSet)
    {
        var schema = default(string);
        switch (resolution)
        {
            case Resolution.Second:
                schema = BBO1sSchema;
                break;
            case Resolution.Minute:
                schema = BBO1mSchema;
                break;
            default:
                throw new ArgumentException($"Unsupported resolution {resolution} in GetBestBidOfferIntervals.");
        }
        return GetRange<BestBidOfferInterval>(symbol, startDateTimeUtc, endDateTimeUtc, schema, dataSet);
    }

    public IEnumerable<StatisticsData> GetOpenInterest(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, string dataSet)
    {
        foreach (var statistics in GetRange<StatisticsData>(symbol, startDateTimeUtc, endDateTimeUtc, StatisticsSchema, dataSet))
        {
            if (statistics.StatType == Models.Enums.StatisticType.OpenInterest)
            {
                yield return statistics;
            }
        }
    }

    private IEnumerable<T> GetRange<T>(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, string schema, string dataSet) where T : MarketDataBase
    {
        var formData = new Dictionary<string, string>
        {
            { "dataset", dataSet },
            { "symbols", symbol },
            { "schema", schema },
            { "encoding", "json" },
            { "stype_in", "raw_symbol" },
            { "pretty_px", "true" },
        };

        // Prevent HTTP client timeouts for large historical range requests.
        // Explicitly cap the number of returned records per request based on the schema
        switch (schema)
        {
            case MBP1Schema:
                formData["limit"] = "10000";
                break;
            case OHLCV1sSchema:
                formData["limit"] = "432000"; // 5 days 
                break;
        }

        var start = startDateTimeUtc;
        var end = endDateTimeUtc;
        var httpStatusCode = default(HttpStatusCode);
        do
        {
            formData["start"] = Time.DateTimeToUnixTimeStampNanoseconds(start).ToStringInvariant();
            formData["end"] = Time.DateTimeToUnixTimeStampNanoseconds(end).ToStringInvariant();

            using var content = new FormUrlEncodedContent(formData);

            using var requestMessage = new HttpRequestMessage(HttpMethod.Post, "/v0/timeseries.get_range")
            {
                Content = content
            };

            using var response = _httpClient.Send(requestMessage);

            LogWarnings(response);

            using var stream = response.Content.ReadAsStream();

            if (stream.Length == 0)
            {
                yield break;
            }

            using var reader = new StreamReader(stream);

            var line = default(string);
            if (response.StatusCode == HttpStatusCode.UnprocessableContent)
            {
                line = reader.ReadLine();
                if (line == null)
                {
                    yield break;
                }

                var error = line.DeserializeObject<ErrorResponse>();

                switch (error.Detail.Case)
                {
                    case ErrorCases.DataStartBeforeAvailableStart:
                        start = error.Detail.Payload.AvailableStart.UtcDateTime;
                        if (!_dataStartBeforeAvailableStartErrorFired)
                        {
                            _dataStartBeforeAvailableStartErrorFired = true;
                            LogUnprocessableContentError(dataSet, symbol, schema,
                                $"Message: {error.Detail.Message} Adjust[Start]: Requested = {startDateTimeUtc:O}, New = {start:O}");
                        }
                        continue;
                    case ErrorCases.DataEndAfterAvailableEnd:
                        end = error.Detail.Payload.AvailableEnd.UtcDateTime;
                        if (!_dataEndAfterAvailableEndErrorFired)
                        {
                            _dataEndAfterAvailableEndErrorFired = true;
                            LogUnprocessableContentError(dataSet, symbol, schema,
                                $"Message: {error.Detail.Message} Adjust[End]: Requested = {endDateTimeUtc:O}, New = {end:O}");
                        }
                        continue;
                    case ErrorCases.DataTimeRangeStartOnOrAfterEnd:
                        if (!_dataTimeRangeStartOnOrAfterEndErrorFired)
                        {
                            _dataTimeRangeStartOnOrAfterEndErrorFired = true;
                            LogUnprocessableContentError(dataSet, symbol, schema, error.Detail.Message);
                        }
                        yield break;
                    case ErrorCases.DataStartAfterAvailableEnd:
                        if (!_dataStartAfterAvailableEndErrorFired)
                        {
                            _dataStartAfterAvailableEndErrorFired = true;
                            LogUnprocessableContentError(dataSet, symbol, schema, error.Detail.Message);
                        }
                        yield break;
                    default:
                        LogDetailError("UnprocessableContent", line, response, formData);
                        yield break;
                }
            }

            httpStatusCode = response.EnsureSuccessStatusCode().StatusCode;

            var lastEmitted = default(T);
            while ((line = reader.ReadLine()) != null)
            {
                lastEmitted = line.DeserializeObject<T>();

                if (lastEmitted == null)
                {
                    continue;
                }

                yield return lastEmitted;
            }
            // Advance start by one tick to move the time window forward without duplication.
            // The API range is inclusive, so this ensures the next request starts
            // strictly after the last emitted record and avoids re-fetching it.
            var lastEmittedTime = (lastEmitted as BestBidOfferInterval)?.UtcDateTime ?? lastEmitted?.Header.UtcDateTime;
            if (!lastEmittedTime.HasValue)
            {
                LogDetailError("AdvanceStartByOneTick", line, response, formData);
                yield break;
            }
            start = lastEmittedTime.Value.AddTicks(1);
        } while (httpStatusCode != HttpStatusCode.OK);
    }

    public void Dispose()
    {
        _httpClient?.DisposeSafely();
    }

    private static void LogWarnings(HttpResponseMessage response)
    {
        if (response.Headers.TryGetValues("x-warning", out var warnings))
        {
            foreach (var warning in warnings)
            {
                Log.Trace($"{nameof(HistoricalAPIClient)}.{nameof(LogWarnings)}: {warning}");
            }
        }
    }

    /// <summary>
    /// Common Logging for Unprocessable Content errors
    /// </summary>
    /// <param name="dataSet">The DataBento DataSet Id name</param>
    /// <param name="symbol">The requested symbol</param>
    /// <param name="schema">The requested schema</param>
    /// <param name="message">The message which API provided or custom one</param>
    private static void LogUnprocessableContentError(string dataSet, string symbol, string schema, string message)
    {
        Log.Error($"HistoricalAPIClient.GetRange [{dataSet}|{symbol}|{schema}]: {message}");
    }

    private static void LogDetailError(string reason, string line, HttpResponseMessage response, Dictionary<string, string> payload)
    {
        Log.Error($"HistoricalAPIClient.GetRange: Reason: {reason}. Response: {line}. " +
            $"Request: [{response.RequestMessage?.Method}]({response.RequestMessage?.RequestUri}), " +
            $"Payload: {string.Join(", ", payload.Select(kvp => $"{kvp.Key}: {kvp.Value}"))}");
    }
}
