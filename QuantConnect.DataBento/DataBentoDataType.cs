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
using NodaTime;
using ProtoBuf;
using System.IO;
using System.Globalization;
using QuantConnect.Data;
using System.Collections.Generic;

namespace QuantConnect.Lean.DataSource.DataBento
{
    /// <summary>
    /// DataBento data type - handles both bar data and tick data
    /// </summary>
    [ProtoContract(SkipConstructor = true)]
    public class DataBentoDataType : BaseData
    {
        /// <summary>
        /// Opening price for the bar
        /// </summary>
        [ProtoMember(2000)]
        public decimal Open { get; set; }

        /// <summary>
        /// High price for the bar
        /// </summary>
        [ProtoMember(2001)]
        public decimal High { get; set; }

        /// <summary>
        /// Low price for the bar
        /// </summary>
        [ProtoMember(2002)]
        public decimal Low { get; set; }

        /// <summary>
        /// Closing price for the bar
        /// </summary>
        [ProtoMember(2003)]
        public decimal Close { get; set; }

        /// <summary>
        /// Volume for the bar
        /// </summary>
        [ProtoMember(2004)]
        public decimal Volume { get; set; }

        /// <summary>
        /// Raw symbol string from DataBento
        /// </summary>
        [ProtoMember(2005)]
        public string RawSymbol { get; set; }

        /// <summary>
        /// Quantity for tick data
        /// </summary>
        [ProtoMember(2006)]
        public decimal Quantity { get; set; }

        /// <summary>
        /// Time passed between the date of the data and the time the data became available to us
        /// </summary>
        public TimeSpan Period { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + Period;

        /// <summary>
        /// Return the URL string source of the file. This will be converted to a stream
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "databento",
                    $"{config.Symbol.Value.ToLowerInvariant()}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses the data from the line provided and loads it into LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>New instance</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');

            var time = DateTime.ParseExact(csv[0], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
            var data = new DataBentoDataType
            {
                Symbol = config.Symbol,
                Time = time,
                Period = Period,
                Open = decimal.Parse(csv[1], CultureInfo.InvariantCulture),
                High = decimal.Parse(csv[2], CultureInfo.InvariantCulture),
                Low = decimal.Parse(csv[3], CultureInfo.InvariantCulture),
                Close = decimal.Parse(csv[4], CultureInfo.InvariantCulture),
                Volume = decimal.Parse(csv[5], CultureInfo.InvariantCulture),
                Value = decimal.Parse(csv[4], CultureInfo.InvariantCulture),
                RawSymbol = config.Symbol.Value
            };

            return data;
        }

        /// <summary>
        /// Clones the data
        /// </summary>
        /// <returns>A clone of the object</returns>
        public override BaseData Clone()
        {
            return new DataBentoDataType
            {
                Symbol = Symbol,
                Time = Time,
                Period = Period,
                Open = Open,
                High = High,
                Low = Low,
                Close = Close,
                Volume = Volume,
                Value = Value,
                Quantity = Quantity,
                RawSymbol = RawSymbol
            };
        }

        /// <summary>
        /// Indicates whether the data source is tied to an underlying symbol and requires that corporate events be applied to it as well, such as renames and delistings
        /// </summary>
        /// <returns>false</returns>
        public override bool RequiresMapping()
        {
            return true;
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files
        /// </summary>
        /// <returns>true</returns>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol} - O:{Open} H:{High} L:{Low} C:{Close} V:{Volume}";
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions() => new List<Resolution>
        {
            Resolution.Tick,
            Resolution.Second,
            Resolution.Minute,
            Resolution.Hour,
            Resolution.Daily
        };

        /// <summary>
        /// Specifies the data time zone for this data type. This is useful for custom data types
        /// </summary>
        /// <returns>The <see cref="T:NodaTime.DateTimeZone" /> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZone.Utc;
        }
    }
}
