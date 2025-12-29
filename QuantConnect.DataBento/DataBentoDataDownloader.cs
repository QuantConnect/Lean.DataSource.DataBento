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
using System.Collections.Generic;
using System.Globalization;
using NodaTime;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using CsvHelper;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Lean.DataSource.DataBento.Models;
using QuantConnect.Util;
using QuantConnect.Securities;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// Data downloader for historical data from DataBento's Raw HTTP API
    /// Converts DataBento data to Lean data types
    /// </summary>
    public class DataBentoDataDownloader : IDataDownloader, IDisposable
    {
        private readonly HttpClient _httpClient = new();
        private readonly string _apiKey;
        private readonly DataBentoSymbolMapper _symbolMapper;
        private readonly MarketHoursDatabase _marketHoursDatabase;
        private readonly Dictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="DataBentoDataDownloader"/>
        /// </summary>
        /// <param name="apiKey">The DataBento API key.</param>
        public DataBentoDataDownloader(string apiKey, MarketHoursDatabase marketHoursDatabase)
        {
            _marketHoursDatabase = marketHoursDatabase;
            _apiKey = apiKey;
            _httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_apiKey}:")));
            _symbolMapper = new DataBentoSymbolMapper();
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="parameters">Parameters for the historical data request</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        /// <exception cref="NotImplementedException"></exception>
        public IEnumerable<BaseData> Get(DataDownloaderGetParameters parameters)
        {
            var symbol = parameters.Symbol;
            var resolution = parameters.Resolution;
            var tickType = parameters.TickType;

            /// <summary>
            /// Dataset for CME Globex futures
            /// https://databento.com/docs/venues-and-datasets has more information on datasets through DataBento
            /// </summary>
            const string dataset = "GLBX.MDP3"; // hard coded for now. Later on can add equities and options with different mapping
            var schema = GetSchema(resolution, tickType);
            var databentoSymbol = _symbolMapper.GetBrokerageSymbol(symbol);

            // prepare body for Raw HTTP request
            var body = new StringBuilder()
                .Append($"dataset={dataset}")
                .Append($"&symbols={databentoSymbol}")
                .Append($"&schema={schema}")
                .Append($"&start={parameters.StartUtc:yyyy-MM-ddTHH:mm}")
                .Append($"&end={parameters.EndUtc:yyyy-MM-ddTHH:mm}")
                .Append("&stype_in=parent")
                .Append("&encoding=csv")
                .ToString();

            using var request = new HttpRequestMessage(HttpMethod.Post,
                "https://hist.databento.com/v0/timeseries.get_range")
            {
                Content = new StringContent(body, Encoding.UTF8, "application/x-www-form-urlencoded")
            };

            // send the request with the get range url
            var response = _httpClient.Send(request);

            // Add error handling to see the actual error message
            if (!response.IsSuccessStatusCode)
            {
                var errorContent = response.Content.ReadAsStringAsync().SynchronouslyAwaitTaskResult();
                throw new HttpRequestException($"DataBento API error ({response.StatusCode}): {errorContent}");
            }

            using var csv = new CsvReader(
                new StreamReader(response.Content.ReadAsStream()),
                CultureInfo.InvariantCulture
            );

            return (tickType, resolution) switch
            {
                (TickType.Trade, Resolution.Tick) =>
                    csv.ForEach<DatabentoTrade>(dt =>
                        new Tick(
                            GetTickTime(symbol, dt.Timestamp),
                            symbol,
                            string.Empty,
                            string.Empty,
                            dt.Size,
                            dt.Price
                        )
                    ),

                (TickType.Trade, _) =>
                    csv.ForEach<DatabentoBar>(bar =>
                        new TradeBar(
                            GetTickTime(symbol, bar.Timestamp),
                            symbol,
                            bar.Open,
                            bar.High,
                            bar.Low,
                            bar.Close,
                            bar.Volume
                        )
                    ),

                (TickType.Quote, Resolution.Tick) =>
                    csv.ForEach<DatabentoQuote>(q =>
                        new Tick(
                            GetTickTime(symbol, q.Timestamp),
                            symbol,
                            bidPrice:  q.BidPrice,
                            askPrice:  q.AskPrice,
                            bidSize:   q.BidSize,
                            askSize:   q.AskSize
                        )
                        {
                            TickType = TickType.Quote
                        }
                    ),

                (TickType.Quote, _) =>
                    csv.ForEach<DatabentoQuote>(q =>
                        new QuoteBar(
                            GetTickTime(symbol, q.Timestamp),
                            symbol,
                            new Bar(q.BidPrice, q.BidPrice, q.BidPrice, q.BidPrice), q.BidSize,
                            new Bar(q.AskPrice, q.AskPrice, q.AskPrice, q.AskPrice), q.AskSize
                        )
                    ),

                _ => throw new NotSupportedException(
                    $"Unsupported tickType={tickType} resolution={resolution}")
            };
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _httpClient?.DisposeSafely();
        }

        /// <summary>
        /// Pick Databento schema from Lean resolution/ticktype
        /// </summary>
        private static string GetSchema(Resolution resolution, TickType tickType)
        {
            return (tickType, resolution) switch
            {
                (TickType.Trade, Resolution.Tick)   => "mbp-1",
                (TickType.Trade, Resolution.Second) => "ohlcv-1s",
                (TickType.Trade, Resolution.Minute) => "ohlcv-1m",
                (TickType.Trade, Resolution.Hour)   => "ohlcv-1h",
                (TickType.Trade, Resolution.Daily)  => "ohlcv-1d",

                (TickType.Quote, _) => "mbp-1",

                _ => throw new NotSupportedException(
                    $"Unsupported resolution {resolution} / {tickType}"
                )
            };
        }

        /// <summary>
        /// Converts the given UTC time into the symbol security exchange time zone
        /// </summary>
        private DateTime GetTickTime(Symbol symbol, DateTime utcTime)
        {
            DateTimeZone exchangeTimeZone;
            lock (_symbolExchangeTimeZones)
            {
                if (!_symbolExchangeTimeZones.TryGetValue(symbol, out exchangeTimeZone))
                {
                    // read the exchange time zone from market-hours-database
                    if (_marketHoursDatabase.TryGetEntry(symbol.ID.Market, symbol, symbol.SecurityType, out var entry))
                    {
                        exchangeTimeZone = entry.ExchangeHours.TimeZone;
                    }
                    // If there is no entry for the given Symbol, default to New York
                    else
                    {
                        exchangeTimeZone = TimeZones.NewYork;
                    }

                    _symbolExchangeTimeZones.Add(symbol, exchangeTimeZone);
                }
            }

            return utcTime.ConvertFromUtc(exchangeTimeZone);
        }
    }
}

public static class CsvReaderExtensions
{
    public static IEnumerable<BaseData> ForEach<T>(
        this CsvReader csv,
        Func<T, BaseData> map)
    {
        return csv.GetRecords<T>().Select(map).ToList();
    }
}
