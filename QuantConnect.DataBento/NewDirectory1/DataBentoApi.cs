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
using RestSharp;
using RestSharp.Authenticators;

namespace QuantConnect.DateBento.NewDirectory1;

public class DataBentoApi
{
    private readonly string _baseUrl = "https://hist.databento.com/v0";
    private readonly RestClient _restClient;

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
        request.AddParameter("symbols", "AAPL");
        request.AddParameter("stype_in", "raw_symbol");
        request.AddParameter("stype_out", "instrument_id ");
        request.AddParameter("start_date", DateTime.UtcNow.AddDays(-10).Date.ToString("O"));

        var resp = _restClient.Execute(request);
    }


    public IEnumerable<TradeEvent> GetTrades(string symbol, DateTime start, DateTime end)
    {
        var lines = GetData(symbol, "trades", start, end);
        foreach (var line in lines)
        {
            //ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence

            yield return new TradeEvent
            {
                ReceiveTimestamp = DateTime.Parse(line[0], CultureInfo.InvariantCulture),
                EventTimestamp = DateTime.Parse(line[1], CultureInfo.InvariantCulture),
                PublisherId = ushort.Parse(line[3], CultureInfo.InvariantCulture),
                Price = decimal.Parse(line[8], CultureInfo.InvariantCulture),
                Size = uint.Parse(line[9], CultureInfo.InvariantCulture),
            };
        }
    }

    public IEnumerable<DataBentoCandle> GetCandleData(string symbol, Resolution resolution, DateTime start,
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

        var lines = GetData(symbol, schema, start, end);


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


    public IEnumerable<string[]> GetData(string symbol, string schema, DateTime start, DateTime end)
    {
        var request = new RestRequest("/timeseries.get_range", Method.POST);
        request.AddParameter("dataset", "DBEQ.BASIC");
        request.AddParameter("start", start.ToString("O"));
        request.AddParameter("end", end.ToString("O"));
        request.AddParameter("schema", schema);
        request.AddParameter("symbols", symbol);
        request.AddParameter("encoding", "csv");
        request.AddParameter("pretty_px", true);
        request.AddParameter("pretty_ts", true);

        var res = _restClient.Execute(request);
        foreach (var line in res.Content.Split("\n")[1..].Where(x => !string.IsNullOrEmpty(x)))
        {
            yield return line.Split(',');
        }
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
        /// The order price
        /// </summary>
        public decimal Price { get; set; }

        /// <summary>
        /// The order quantity.
        /// </summary>
        public uint Size { get; set; }

        /// <summary>
        /// The event action. Always Trade in the trades schema.
        /// </summary>
        public char Action { get; set; }

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