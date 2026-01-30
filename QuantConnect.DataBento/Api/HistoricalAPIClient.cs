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
                schema = "ohlcv-1s";
                break;
            case Resolution.Minute:
                schema = "ohlcv-1m";
                break;
            case Resolution.Hour:
                schema = "ohlcv-1h";
                break;
            case Resolution.Daily:
                schema = "ohlcv-1d";
                break;
            default:
                throw new ArgumentException($"Unsupported resolution {resolution} for OHLCV data.");
        }

        return GetRange<OpenHighLowCloseVolumeData>(symbol, startDateTimeUtc, endDateTimeUtc, schema, dataSet);
    }

    public IEnumerable<LevelOneData> GetTickBars(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, string dataSet)
    {
        return GetRange<LevelOneData>(symbol, startDateTimeUtc, endDateTimeUtc, "mbp-1", dataSet, useLimit: true);
    }

    public IEnumerable<StatisticsData> GetOpenInterest(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, string dataSet)
    {
        foreach (var statistics in GetRange<StatisticsData>(symbol, startDateTimeUtc, endDateTimeUtc, "statistics", dataSet))
        {
            if (statistics.StatType == Models.Enums.StatisticType.OpenInterest)
            {
                yield return statistics;
            }
        }
    }

    private IEnumerable<T> GetRange<T>(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, string schema, string dataSet, bool useLimit = false) where T : MarketDataBase
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

        if (useLimit)
        {
            formData["limit"] = "10000";
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

                switch (error?.Detail?.Case)
                {
                    case ErrorCases.DataStartBeforeAvailableStart:
                        start = error.Detail.Payload.AvailableStart.UtcDateTime;
                        if (end > error.Detail.Payload.AvailableEnd.UtcDateTime)
                        {
                            end = error.Detail.Payload.AvailableEnd.UtcDateTime;

                        }
                        Log.Trace($"{nameof(HistoricalAPIClient)}.{nameof(GetRange)}: {ErrorCases.DataStartBeforeAvailableStart}, " +
                            $"Start {startDateTimeUtc:O}->{start:O}, End {endDateTimeUtc:O}->{end:O}");
                        continue;
                    case ErrorCases.DataEndAfterAvailableEnd:
                        end = error.Detail.Payload.AvailableEnd.UtcDateTime;
                        var startBound = end - endDateTimeUtc.Subtract(startDateTimeUtc);
                        start = startBound < error.Detail.Payload.AvailableStart.UtcDateTime
                            ? error.Detail.Payload.AvailableStart.UtcDateTime
                            : startBound;
                        Log.Trace($"{nameof(HistoricalAPIClient)}.{nameof(GetRange)}: {ErrorCases.DataEndAfterAvailableEnd}, " +
                            $"Start {startDateTimeUtc:O}->{start:O}, End {endDateTimeUtc:O}->{end:O}");
                        continue;
                    case ErrorCases.DataTimeRangeStartOnOrAfterEnd:
                        Log.Error($"{nameof(HistoricalAPIClient)}.{nameof(GetRange)}: {error.Detail.Message}");
                        yield break;
                    case ErrorCases.DataStartAfterAvailableEnd:
                        end = error.Detail.Payload.AvailableEnd.UtcDateTime;
                        start = end - endDateTimeUtc.Subtract(startDateTimeUtc);
                        Log.Trace($"{nameof(HistoricalAPIClient)}.{nameof(GetRange)}: {ErrorCases.DataStartAfterAvailableEnd}, " +
                            $"Start {startDateTimeUtc:O}->{start:O}, End {endDateTimeUtc:O}->{end:O}");
                        continue;
                    default:
                        Log.Trace($"{nameof(HistoricalAPIClient)}.{nameof(GetRange)}.Response: {line}. " +
                            $"Request: [{response.RequestMessage?.Method}]({response.RequestMessage?.RequestUri}), " +
                            $"Payload: {string.Join(", ", formData.Select(kvp => $"{kvp.Key}: {kvp.Value}"))}");
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
            start = lastEmitted!.Header.UtcTime.AddTicks(1);
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
}
