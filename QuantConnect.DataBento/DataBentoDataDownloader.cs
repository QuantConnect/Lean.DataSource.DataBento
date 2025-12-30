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
using System.Net.Http.Headers;
using System.Globalization;
using System.Collections.Generic;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Util;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Securities;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// Data downloader for historical data from DataBento's Raw HTTP API
    /// Converts DataBento data to Lean data types
    /// </summary>
    public class DataBentoDataDownloader : IDataDownloader, IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiKey;
        private const decimal PriceScaleFactor = 1e-9m;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataBentoDataDownloader"/>
        /// </summary>
        public DataBentoDataDownloader(string apiKey)
        {
            _apiKey = apiKey;
            _httpClient = new HttpClient();

            // Set up HTTP Basic Authentication
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{_apiKey}:"));
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
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
            var tickType = parameters.TickType;

            var dataset = "GLBX.MDP3"; // hard coded for now. Later on can add equities and options with different mapping
            var schema = GetSchema(resolution, tickType);
            var dbSymbol = MapSymbolToDataBento(symbol);

            // prepare body for Raw HTTP request
            var body = new StringBuilder();
            body.Append($"dataset={dataset}");
            body.Append($"&symbols={dbSymbol}");
            body.Append($"&schema={schema}");
            body.Append($"&start={parameters.StartUtc:yyyy-MM-ddTHH:mm}");
            body.Append($"&end={parameters.EndUtc:yyyy-MM-ddTHH:mm}");
            body.Append("&stype_in=parent");
            body.Append("&encoding=csv");

            var request = new HttpRequestMessage(
                HttpMethod.Post,
                "https://hist.databento.com/v0/timeseries.get_range")
            {
                Content = new StringContent(body.ToString(), Encoding.UTF8, "application/x-www-form-urlencoded")
            };

            // send the request with the get range url
            var response = _httpClient.Send(request);

            // Add error handling to see the actual error message
            if (!response.IsSuccessStatusCode)
            {
                var errorContent = response.Content.ReadAsStringAsync().Result;
                throw new HttpRequestException($"DataBento API error ({response.StatusCode}): {errorContent}");
            }

            response.EnsureSuccessStatusCode();

            using var stream = response.Content.ReadAsStream();
            using var reader = new StreamReader(stream);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

            if (tickType == TickType.Trade)
            {
                if (resolution == Resolution.Tick)
                {
                    // For tick data, use the trades schema which returns individual trades
                    foreach (var record in csv.GetRecords<DatabentoTrade>())
                    {
                        yield return new Tick
                        {
                            Time = record.Timestamp,
                            Symbol = symbol,
                            Value = record.Price,
                            Quantity = record.Size
                        };
                    }
                }
                else
                {
                    // For aggregated data, use the ohlcv schema which returns bars
                    foreach (var record in csv.GetRecords<DatabentoBar>())
                    {
                        yield return new TradeBar
                        {
                            Symbol = symbol,
                            Time = record.Timestamp,
                            Open = record.Open,
                            High = record.High,
                            Low = record.Low,
                            Close = record.Close,
                            Volume = record.Volume
                        };
                    }
                }
            }
            else if (tickType == TickType.Quote)
            {
                foreach (var record in csv.GetRecords<DatabentoQuote>())
                {
                    var bidPrice = record.BidPrice * PriceScaleFactor;
                    var askPrice = record.AskPrice * PriceScaleFactor;

                    if (resolution == Resolution.Tick)
                    {
                        yield return new Tick
                        {
                            Time = record.Timestamp,
                            Symbol = symbol,
                            AskPrice = askPrice,
                            BidPrice = bidPrice,
                            AskSize = record.AskSize,
                            BidSize = record.BidSize,
                            TickType = TickType.Quote
                        };
                    }
                    else
                    {
                        var bidBar = new Bar(bidPrice, bidPrice, bidPrice, bidPrice);
                        var askBar = new Bar(askPrice, askPrice, askPrice, askPrice);
                        yield return new QuoteBar(
                            record.Timestamp,
                            symbol,
                            bidBar,
                            record.BidSize,
                            askBar,
                            record.AskSize
                        );
                    }
                }
            }
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
        private string GetSchema(Resolution resolution, TickType tickType)
        {
            if (tickType == TickType.Trade)
            {
                if (resolution == Resolution.Tick)
                    return "trades";
                if (resolution == Resolution.Second)
                    return "ohlcv-1s";
                if (resolution == Resolution.Minute)
                    return "ohlcv-1m";
                if (resolution == Resolution.Hour)
                    return "ohlcv-1h";
                if (resolution == Resolution.Daily)
                    return "ohlcv-1d";
            }
            else if (tickType == TickType.Quote)
            {
                // top of book
                if (resolution == Resolution.Tick || resolution == Resolution.Second || resolution == Resolution.Minute || resolution == Resolution.Hour || resolution == Resolution.Daily)
                    return "mbp-1";
            }

            throw new NotSupportedException($"Unsupported resolution {resolution} / {tickType}");
        }

        /// <summary>
        /// Maps a LEAN symbol to DataBento symbol format
        /// </summary>
        private string MapSymbolToDataBento(Symbol symbol)
        {
            if (symbol.SecurityType == SecurityType.Future)
            {
                // For DataBento, use the root symbol with .FUT suffix for parent subscription
                // ES19Z25 -> ES.FUT
                var value = symbol.Value;

                // Extract root by removing digits and month codes
                var root = new string(value.TakeWhile(c => !char.IsDigit(c)).ToArray());

                return $"{root}.FUT";
            }

            return symbol.Value;
        }

        /// Class for parsing OHLCV bar data from Databento
        /// Databento returns prices as fixed-point integers scaled by 10^9
        private class DatabentoBar
        {
            [Name("ts_event")]
            public long TimestampNanos { get; set; }

            public DateTime Timestamp => DateTimeOffset.FromUnixTimeSeconds(TimestampNanos / 1_000_000_000)
                .AddTicks((TimestampNanos % 1_000_000_000) / 100).UtcDateTime;

            [Name("open")]
            public long OpenRaw { get; set; }
            public decimal Open => OpenRaw * PriceScaleFactor;

            [Name("high")]
            public long HighRaw { get; set; }
            public decimal High => HighRaw * PriceScaleFactor;

            [Name("low")]
            public long LowRaw { get; set; }
            public decimal Low => LowRaw * PriceScaleFactor;

            [Name("close")]
            public long CloseRaw { get; set; }
            public decimal Close => CloseRaw * PriceScaleFactor;

            [Name("volume")]
            public long Volume { get; set; }
        }

        private class DatabentoTrade
        {
            [Name("ts_event")]
            public long TimestampNanos { get; set; }

            public DateTime Timestamp => DateTimeOffset.FromUnixTimeSeconds(TimestampNanos / 1_000_000_000)
                .AddTicks((TimestampNanos % 1_000_000_000) / 100).UtcDateTime;

            [Name("price")]
            public long PriceRaw { get; set; }

            public decimal Price => PriceRaw * PriceScaleFactor;

            [Name("size")]
            public int Size { get; set; }
        }

        private class DatabentoQuote
        {
            [Name("ts_event")]
            public long TimestampNanos { get; set; }

            public DateTime Timestamp => DateTimeOffset.FromUnixTimeSeconds(TimestampNanos / 1_000_000_000)
                .AddTicks((TimestampNanos % 1_000_000_000) / 100).UtcDateTime;

            [Name("bid_px_00")]
            public long BidPrice { get; set; }

            [Name("bid_sz_00")]
            public int BidSize { get; set; }

            [Name("ask_px_00")]
            public long AskPrice { get; set; }

            [Name("ask_sz_00")]
            public int AskSize { get; set; }
        }
    }
}
