using System;
using CsvHelper.Configuration.Attributes;
using QuantConnect;

namespace QuantConnect.Lean.DataSource.DataBento.Models
{
    /// <summary>
    /// Provides a constant for scaling price values from DataBento.
    /// </summary>
    public static class PriceScaling
    {
        /// <summary>
        /// price scale factor is needed to find the true price from the message
        /// Due to compression each "1 unit corresponds to 1e-9, i.e. 1/1,000,000,000 or 0.000000001"
        /// https://databento.com/docs/api-reference-live/basics/schemas-and-conventions?historical=raw&live=raw&reference=raw 
        /// </summary>
        public const decimal PriceScaleFactor = 1e-9m;
    }

    /// <summary>
    /// Represents a single bar of historical data from DataBento.
    /// This class is used to map CSV data from HTTP requests into a structured format.
    /// </summary>
    public class DatabentoBar
    {
        [Name("ts_event")]
        public long TimestampNanos { get; set; }

        public DateTime Timestamp => Time.UnixNanosecondTimeStampToDateTime(TimestampNanos);

        [Name("open")]
        public long RawOpen { get; set; }

        [Name("high")]
        public long RawHigh { get; set; }

        [Name("low")]
        public long RawLow { get; set; }



        [Name("close")]
        public long RawClose { get; set; }

        [Ignore]
        public decimal Open => RawOpen == long.MaxValue ? 0m : RawOpen * PriceScaling.PriceScaleFactor;

        [Ignore]
        public decimal High => RawHigh == long.MaxValue ? 0m : RawHigh * PriceScaling.PriceScaleFactor;

        [Ignore]
        public decimal Low => RawLow == long.MaxValue ? 0m : RawLow * PriceScaling.PriceScaleFactor;

        [Ignore]
        public decimal Close => RawClose == long.MaxValue ? 0m : RawClose * PriceScaling.PriceScaleFactor;

        [Name("volume")]
        public long RawVolume { get; set; }

        [Ignore]
        public decimal Volume => RawVolume == long.MaxValue ? 0m : RawVolume;
    }

    /// <summary>
    /// Represents a single trade event from DataBento.
    /// </summary>
    public class DatabentoTrade
    {
        [Name("ts_event")]
        public long TimestampNanos { get; set; }

        public DateTime Timestamp => Time.UnixNanosecondTimeStampToDateTime(TimestampNanos);

        [Name("price")]
        public long RawPrice { get; set; }

        [Ignore]
        public decimal Price => RawPrice == long.MaxValue ? 0m : RawPrice * PriceScaling.PriceScaleFactor;

        [Name("size")]
        public int Size { get; set; }
    }

    /// <summary>
    /// Represents a single quote from DataBento.
    /// </summary>
    public class DatabentoQuote
    {
        [Name("ts_event")]
        public long TimestampNanos { get; set; }

        public DateTime Timestamp => Time.UnixNanosecondTimeStampToDateTime(TimestampNanos);

        [Name("bid_px_00")]
        public long RawBidPrice { get; set; }

        [Ignore]
        public decimal BidPrice => RawBidPrice == long.MaxValue ? 0m : RawBidPrice * PriceScaling.PriceScaleFactor;

        [Name("bid_sz_00")]
        public int BidSize { get; set; }

        [Name("ask_px_00")]
        public long RawAskPrice { get; set; }

        [Ignore]
        public decimal AskPrice => RawAskPrice == long.MaxValue ? 0m : RawAskPrice * PriceScaling.PriceScaleFactor;

        [Name("ask_sz_00")]
        public int AskSize { get; set; }
    }
}
