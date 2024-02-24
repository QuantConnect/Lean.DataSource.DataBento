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

using System.Globalization;
using Newtonsoft.Json.Linq;
using QuantConnect.Logging;
using QuantConnect.Util;
using RestSharp;
using RestSharp.Authenticators;

namespace QuantConnect.DateBento;

public class DataBentoApi
{
    private readonly string _baseUrl = "https://hist.databento.com/v0";
    private readonly RestClient _restClient;

    protected virtual RateGate RateLimiter { get; } = new(100, TimeSpan.FromSeconds(1));

    public DataBentoApi(string apiKey)
    {
        _restClient = new RestClient(_baseUrl)
        {
            Authenticator = new HttpBasicAuthenticator(apiKey, string.Empty),
        };
    }


    public void ResolveSymbology()
    {
        //todo probably not needed
        var request = new RestRequest("/symbology.resolve", Method.POST);

        request.AddParameter("dataset", "DBEQ.BASIC");
        request.AddParameter("symbols", "SPY");
        request.AddParameter("stype_in", "raw_symbol");
        request.AddParameter("stype_out", "instrument_id ");
        request.AddParameter("start_date", DateTime.UtcNow.AddDays(-365).Date.ToString("O"));
        request.AddParameter("end_date", DateTime.UtcNow.Date.AddDays(-2).ToString("O"));

        var resp = _restClient.Execute(request);
    }


    public IEnumerable<TradeEvent> GetTrades(string ticker, DateTime start, DateTime end)
    {
        var lines = GetData(ticker, "trades", start, end);
        foreach (var line in lines)
        {
            var tradeEvent = new TradeEvent();
            PopulateTradeEventFromLine(tradeEvent, line);
            yield return tradeEvent;
        }
    }

    public IEnumerable<DataBentoCandle> GetCandleData(string ticker, Resolution resolution, DateTime start,
        DateTime end, int publisherId)
    {
        var schemaTimeframe = resolution switch
        {
            Resolution.Second => "s",
            Resolution.Minute => "m",
            Resolution.Hour => "h",
            Resolution.Daily => "d",
            _ => throw new ArgumentOutOfRangeException(nameof(resolution), resolution, null)
        };
        var schema = $"OHLCV-1{schemaTimeframe}";

        var lines = GetData(ticker, schema, start, end);


        foreach (var line in lines)
        {
            var candle = new DataBentoCandle
            {
                Time = DateTime.Parse(line[0]),
                RType = int.Parse(line[1], CultureInfo.InvariantCulture),
                PublisherId = int.Parse(line[2], CultureInfo.InvariantCulture),
                Open = decimal.Parse(line[3], CultureInfo.InvariantCulture),
                High = decimal.Parse(line[4], CultureInfo.InvariantCulture),
                Low = decimal.Parse(line[5], CultureInfo.InvariantCulture),
                Close = decimal.Parse(line[6], CultureInfo.InvariantCulture),
                Volume = decimal.Parse(line[7], CultureInfo.InvariantCulture),
            };
            if (candle.PublisherId == publisherId)
                yield return
                    candle; //todo DB offers data from multiple publishers. There doesn't seem to be a way to just request a single pub
        }
        //todo error handling
    }

    public IEnumerable<TradeBBOEvent> GetTBBOBookChanges(string ticker,
        DateTime start,
        DateTime end,
        int? publisherId)
    {
        var schema = "tbbo";
        var lines = GetData(ticker, schema, start, end);
        foreach (var line in lines)
        {
            var tradeEvent = new TradeBBOEvent();
            PopulateTradeEventFromLine(tradeEvent, line);
            if (publisherId.HasValue && publisherId.Value != tradeEvent.PublisherId) continue;

            //ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence,bid_px_00,ask_px_00,bid_sz_00,ask_sz_00,bid_ct_00,ask_ct_00

            tradeEvent.BidPrice = decimal.Parse(line[13], CultureInfo.InvariantCulture);
            tradeEvent.AskPrice = decimal.Parse(line[14], CultureInfo.InvariantCulture);

            tradeEvent.BidSize = uint.Parse(line[15], CultureInfo.InvariantCulture);
            tradeEvent.AskSize = uint.Parse(line[16], CultureInfo.InvariantCulture);
            tradeEvent.BidOrders = uint.Parse(line[17], CultureInfo.InvariantCulture);
            tradeEvent.AskOrders = uint.Parse(line[18], CultureInfo.InvariantCulture);

            yield return tradeEvent;
        }
    }

    private void PopulateTradeEventFromLine(TradeEvent existingObj, string[] line)
    {
        //ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence

        existingObj.ReceiveTimestamp = DateTime.Parse(line[0], CultureInfo.InvariantCulture);
        existingObj.EventTimestamp = DateTime.Parse(line[1], CultureInfo.InvariantCulture);
        existingObj.PublisherId = ushort.Parse(line[3], CultureInfo.InvariantCulture);
        existingObj.Price = decimal.Parse(line[8], CultureInfo.InvariantCulture);
        existingObj.Size = uint.Parse(line[9], CultureInfo.InvariantCulture);
        existingObj.Depth = byte.Parse(line[7], CultureInfo.InvariantCulture);
        existingObj.Action = 'T';
    }

    public IEnumerable<string[]> GetData(string ticker, string schema, DateTime start, DateTime end)
    {
        if (RateLimiter.IsRateLimited)
        {
            Log.Trace($"{nameof(DataBentoApi)}.{nameof(GetData)}(): Rest API calls are limited; waiting to proceed.");
        }

        RateLimiter.WaitToProceed();


        var request = new RestRequest("/timeseries.get_range", Method.POST);
        request.AddParameter("dataset", "DBEQ.BASIC");
        request.AddParameter("start", start.ToString("O"));
        request.AddParameter("end", end.ToString("O"));
        request.AddParameter("schema", schema);
        request.AddParameter("symbols", ticker);
        request.AddParameter("encoding", "csv");
        request.AddParameter("pretty_px", true);
        request.AddParameter("pretty_ts", true);

        var res = _restClient.Execute(request);

        if (!res.IsSuccessful)
        {
            var errorContent = JObject.Parse(res.Content);
            var error = errorContent.Value<string>("detail") ?? "Unknown error";

            Log.Trace($"{nameof(DataBentoApi)}.{nameof(GetData)}(): Response does not indicate success: {error}.");

            yield break;
        }

        foreach (var line in res.Content.Split("\n")[1..].Where(x => !string.IsNullOrEmpty(x)))
        {
            yield return line.Split(',');
        }
    }


    public class TradeBBOEvent : TradeEvent
    {
        public decimal BidPrice { get; set; }
        public decimal AskPrice { get; set; }
        public uint BidSize { get; set; }
        public uint AskSize { get; set; }
        public uint BidOrders { get; set; }
        public uint AskOrders { get; set; }
    }

    /// <summary>
    /// Represents a trade event.
    /// </summary>
    public class TradeEvent
    {
        /// <summary>
        /// The publisher ID assigned by Databento, which denotes the dataset and venue.
        /// </summary>
        public ushort PublisherId { get; set; }

        /// <summary>
        /// The numeric instrument ID.
        /// </summary>
        public uint InstrumentId { get; set; }


        /// <summary>
        /// The matching-engine-received timestamp expressed as the number of nanoseconds since the UNIX epoch.
        /// </summary>
        public DateTime EventTimestamp { get; set; }

        /// <summary>
        /// The event action. Always Trade in the trades schema.
        /// </summary>
        public char Action { get; set; }

        /// <summary>
        /// The order price
        /// </summary>
        public decimal Price { get; set; }

        /// <summary>
        /// The order quantity.
        /// </summary>
        public uint Size { get; set; }

        /// <summary>
        /// The side of the aggressing order. Can be Ask, Bid, or None.
        /// </summary>
        public char Side { get; set; }

        /// <summary>
        /// A bit field indicating packet end, message characteristics, and data quality.
        /// </summary>
        public byte Flags { get; set; }

        /// <summary>
        /// The book level where the update event occurred.
        /// </summary>
        public byte Depth { get; set; }

        /// <summary>
        /// The capture-server-received timestamp expressed as the number of nanoseconds since the UNIX epoch.
        /// </summary>
        public DateTime ReceiveTimestamp { get; set; }

        /// <summary>
        /// The matching-engine-sending timestamp expressed as the number of nanoseconds before ts_recv.
        /// </summary>
        public int MatchingEngineSendTimestamp { get; set; }

        /// <summary>
        /// The message sequence number assigned at the venue.
        /// </summary>
        public uint SequenceNumber { get; set; }
    }

    public class DataBentoCandle
    {
        public DateTime Time { get; set; }
        public int RType { get; set; }
        public int PublisherId { get; set; }
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
        public decimal Volume { get; set; }
    }

    public enum DataBentoPublishers
    {
        /// <summary>
        /// DBEQ Basic - NYSE Chicago
        /// </summary>
        XCHI = 39,

        /// <summary>
        /// DBEQ Basic - NYSE National
        /// </summary>
        XCIS = 40,

        /// <summary>
        /// DBEQ Basic - IEX
        /// </summary>
        IEXG = 41,

        /// <summary>
        /// DBEQ Basic - MIAX Pearl
        /// </summary>
        ERPL = 42,

        /// <summary>
        /// DBEQ Basic - Consolidated
        /// </summary>
        DBEQ = 59
    }
}