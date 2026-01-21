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
    //private const string

    /// <summary>
    /// Dataset for CME Globex futures
    /// https://databento.com/docs/venues-and-datasets has more information on datasets through DataBento
    /// </summary>
    /// <remarks>
    /// TODO: Hard coded for now. Later on can add equities and options with different mapping
    /// </remarks>
    private const string Dataset = "GLBX.MDP3";

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

    public IEnumerable<OhlcvBar> GetHistoricalOhlcvBars(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, Resolution resolution, TickType tickType)
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

        return GetRange<OhlcvBar>(symbol, startDateTimeUtc, endDateTimeUtc, schema);
    }

    public IEnumerable<LevelOneData> GetTickBars(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc)
    {
        return GetRange<LevelOneData>(symbol, startDateTimeUtc, endDateTimeUtc, "mbp-1", useLimit: true);
    }

    public IEnumerable<StatisticsData> GetOpenInterest(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc)
    {
        foreach (var statistics in GetRange<StatisticsData>(symbol, startDateTimeUtc, endDateTimeUtc, "statistics"))
        {
            if (statistics.StatType == Models.Enums.StatisticType.OpenInterest)
            {
                yield return statistics;
            }
        }
    }

    private IEnumerable<T> GetRange<T>(string symbol, DateTime startDateTimeUtc, DateTime endDateTimeUtc, string schema, bool useLimit = false) where T : MarketDataRecord
    {
        var formData = new Dictionary<string, string>
        {
            { "dataset", Dataset },
            { "end", Time.DateTimeToUnixTimeStampNanoseconds(endDateTimeUtc).ToStringInvariant() },
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
        var httpStatusCode = default(HttpStatusCode);
        do
        {
            formData["start"] = Time.DateTimeToUnixTimeStampNanoseconds(start).ToStringInvariant();

            using var content = new FormUrlEncodedContent(formData);

            using var requestMessage = new HttpRequestMessage(HttpMethod.Post, "/v0/timeseries.get_range")
            {
                Content = content
            };

            using var response = _httpClient.Send(requestMessage);

            if (response.Headers.TryGetValues("x-warning", out var warnings))
            {
                foreach (var warning in warnings)
                {
                    Log.Trace($"{nameof(HistoricalAPIClient)}.{nameof(GetRange)}: {warning}");
                }
            }

            using var stream = response.Content.ReadAsStream();

            if (stream.Length == 0)
            {
                continue;
            }

            using var reader = new StreamReader(stream);

            var line = default(string);
            if (response.StatusCode == HttpStatusCode.UnprocessableContent)
            {
                line = reader.ReadLine();
                Log.Trace($"{nameof(HistoricalAPIClient)}.{nameof(GetRange)}.Response: {line}. " +
                    $"Request: [{response.RequestMessage?.Method}]({response.RequestMessage?.RequestUri}), " +
                    $"Payload: {string.Join(", ", formData.Select(kvp => $"{kvp.Key}: {kvp.Value}"))}");
                yield break;
            }

            httpStatusCode = response.EnsureSuccessStatusCode().StatusCode;

            var data = default(T);
            while ((line = reader.ReadLine()) != null)
            {
                data = line.DeserializeKebabCase<T>();
                yield return data;
            }
            start = data.Header.UtcTime.AddTicks(1);
        } while (httpStatusCode == HttpStatusCode.PartialContent);
    }

    public void Dispose()
    {
        _httpClient?.DisposeSafely();
    }
}
