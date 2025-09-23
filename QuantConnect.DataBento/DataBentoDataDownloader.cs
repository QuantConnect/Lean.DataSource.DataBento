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
using System.IO;
using System.Text;
using System.Net.Http;
using System.Globalization;
using System.Collections.Generic;
using CsvHelper;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Util;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// Data downloader class for pulling data from Data Provider
    /// </summary>
    public class DataBentoDataDownloader : IDataProvider
    {
        /// <inheritdoc cref="HttpClient"/>
        private readonly HttpClient _httpClient;

        /// <inheritdoc cref="DataBentoProvider"/>
        private readonly DataBentoProvider _DataBentoProvider;

        /// <inheritdoc cref="MarketHoursDatabase" />
        private readonly MarketHoursDatabase _marketHoursDatabase;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataBentoDataDownloader"/>
        /// </summary>
        public DataBentoDataDownloader(string apiKey)
        {
            _httpClient = new HttpClient();
            _DataBentoProvider = new DataBentoProvider(apiKey);
            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        }

        public DataBentoDataDownloader()
            : this(Config.Get("databento-api-key"))
        {
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="dataDownloaderGetParameters">Parameters for the historical data request</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        /// <exception cref="NotImplementedException"></exception>
        public IEnumerable<BaseData> Get(DataDownloaderGetParameters parameters)
        {
            var symbol = parameters.Symbol;
            var resolution = parameters.Resolution;
            var startUtc = parameters.StartUtc;
            var endUtc = parameters.EndUtc;
            var tickType = parameters.TickType;

            var dataset = "GLBX.MDP3"; // hard coded for now. Later on can add equities and options with different mapping
            var schema = GetSchema(resolution, tickType);
            var dbSymbol = MapSymbol(symbol);

            // prepare body for Raw HTTP request
            var body = new StringBuilder();
            body.Append($"dataset={dataset}");
            body.Append($"&symbols={dbSymbol}");
            body.Append($"&schema={schema}");
            body.Append($"&start={startUtc:yyyy-MM-ddTHH:mm:ssZ}");
            body.Append($"&end={endUtc:yyyy-MM-ddTHH:mm:ssZ}");
            body.Append("&stype_in=continuous");
            body.Append("&encoding=csv");

            var request = new HttpRequestMessage(
            HttpMethod.Post,
            "https://hist.databento.com/v0/timeseries.get_range")
            {
                Content = new StringContent(body.ToString(), Encoding.UTF8, "application/x-www-form-urlencoded")
            };

            // Add API key authentication
            var apiKey = Config.Get("databento-api-key");
            request.Headers.Add("Authorization", $"Bearer {apiKey}");

            // send the request with the get range url
            var response = _httpClient.Send(request);
            response.EnsureSuccessStatusCode();

            using var stream = response.Content.ReadAsStream();
            using var reader = new StreamReader(stream);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

            // base data conversion
            foreach (var record in csv.GetRecords<DatabentoTrade>())
            {
                if (resolution == Resolution.Tick)
                {
                    yield return new Tick
                    {
                        Time = record.Timestamp,
                        Symbol = symbol,
                        Value = record.Price,
                        Quantity = record.Size
                    };
                }
                else
                {
                    yield return new DataBentoDataType
                    {
                        Symbol = symbol,
                        Time = record.Timestamp,
                        Open = record.Open,
                        High = record.High,
                        Low = record.Low,
                        Close = record.Close,
                        Volume = record.Volume,
                        Value = record.Close
                    };
                }
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _DataBentoProvider?.DisposeSafely();
        }

        /// <summary>
        /// Pick Databento schema from Lean resolution/ticktype
        /// </summary>
        private string GetSchema(Resolution resolution, TickType tickType)
        {
            if (resolution == Resolution.Tick && tickType == TickType.Trade)
                return "trades";

            if (resolution == Resolution.Tick && tickType == TickType.Quote)
                return "mbp-1"; // top of book

            if (resolution == Resolution.Second)
                return "ohlcv-1s";

            if (resolution == Resolution.Minute)
                return "ohlcv-1m";

            if (resolution == Resolution.Hour)
                return "ohlcv-1h";

            if (resolution == Resolution.Daily)
                return "ohlcv-1d";

            throw new NotSupportedException($"Unsupported resolution {resolution} / {tickType}");
        }

        /// <summary>
        /// Map Lean Symbol to Databento symbol string for continous
        /// </summary>
        private string MapSymbol(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Future)
            {
                return $"{symbol.ID.Symbol}.v.0";
            }

            return symbol.Value;
        }


        /// Class for parsing trade data from Databento
        private class DatabentoTrade
        {
            public DateTime Timestamp { get; set; }
            public decimal Price { get; set; }
            public int Size { get; set; }
            public decimal Open { get; set; }
            public decimal High { get; set; }
            public decimal Low { get; set; }
            public decimal Close { get; set; }
            public decimal Volume { get; set; }
        }
    }
}

