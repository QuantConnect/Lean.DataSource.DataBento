using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Lean.DataSource.DataBento.Models;
using QuantConnect.Lean.DataSource.DataBento.Models.Enums;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

[TestFixture]
public class DataBentoJsonConverterTests
{
    [Test]
    public void DeserializeHistoricalOhlcvBar()
    {
        var json = @"{
    ""hd"": {
        ""ts_event"": ""1738281600000000000"",
        ""rtype"": 35,
        ""publisher_id"": 1,
        ""instrument_id"": 42140878
    },
    ""open"": ""6359.000000000"",
    ""high"": ""6359.000000000"",
    ""low"": ""6355.000000000"",
    ""close"": ""6355.000000000"",
    ""volume"": ""2""
}";
        var res = json.DeserializeKebabCase<OhlcvBar>();

        Assert.IsNotNull(res);

        Assert.AreEqual(1738281600000000000m, res.Header.TsEvent);
        Assert.AreEqual(35, res.Header.Rtype);
        Assert.AreEqual(1, res.Header.PublisherId);
        Assert.AreEqual(42140878, res.Header.InstrumentId);

        Assert.AreEqual(6359m, res.Open);
        Assert.AreEqual(6359m, res.High);
        Assert.AreEqual(6355m, res.Low);
        Assert.AreEqual(6355m, res.Close);
        Assert.AreEqual(2L, res.Volume);
    }

    [Test]
    public void DeserializeHistoricalLevelOneData()
    {
        var json = @"{
    ""ts_recv"": ""1768137063449660443"",
    ""hd"": {
        ""ts_event"": ""1768137063107829777"",
        ""rtype"": 1,
        ""publisher_id"": 1,
        ""instrument_id"": 42140878
    },
    ""action"": ""A"",
    ""side"": ""N"",
    ""depth"": 0,
    ""price"": ""7004.250000000"",
    ""size"": 15,
    ""flags"": 128,
    ""ts_in_delta"": 17537,
    ""sequence"": 811,
    ""levels"": [
        {
            ""bid_px"": ""7004.000000000"",
            ""ask_px"": ""7004.250000000"",
            ""bid_sz"": 11,
            ""ask_sz"": 15,
            ""bid_ct"": 1,
            ""ask_ct"": 1
        }
    ]
}";
        var res = json.DeserializeKebabCase<LevelOneData>();

        Assert.IsNotNull(res);

        Assert.AreEqual(1768137063449660443, res.TsRecv);

        Assert.AreEqual(1768137063107829777, res.Header.TsEvent);
        Assert.AreEqual(1, res.Header.Rtype);
        Assert.AreEqual(1, res.Header.PublisherId);
        Assert.AreEqual(42140878, res.Header.InstrumentId);

        Assert.AreEqual('A', res.Action);
        Assert.AreEqual('N', res.Side);
        Assert.AreEqual(0, res.Depth);
        Assert.AreEqual(7004.25m, res.Price);
        Assert.AreEqual(15, res.Size);
        Assert.AreEqual(128, res.Flags);
        Assert.IsNotNull(res.Levels);
        Assert.AreEqual(1, res.Levels.Count);
        var level = res.Levels[0];
        Assert.AreEqual(7004.0m, level.BidPx);
        Assert.AreEqual(7004.25m, level.AskPx);
        Assert.AreEqual(11, level.BidSz);
        Assert.AreEqual(15, level.AskSz);
        Assert.AreEqual(1, level.BidCt);
        Assert.AreEqual(1, level.AskCt);
    }

    [Test]
    public void DeserializeHistoricalStatisticsData()
    {
        var json = @"{
    ""ts_recv"": ""1768156232522711477"",
    ""hd"": {
        ""ts_event"": ""1768156232522476283"",
        ""rtype"": 24,
        ""publisher_id"": 1,
        ""instrument_id"": 42566722
    },
    ""ts_ref"": ""1767916800000000000"",
    ""price"": null,
    ""quantity"": 470,
    ""sequence"": 29232,
    ""ts_in_delta"": 12477,
    ""stat_type"": 9,
    ""channel_id"": 1,
    ""update_action"": 1,
    ""stat_flags"": 0
}";

        var res = json.DeserializeKebabCase<StatisticsData>();

        Assert.IsNotNull(res);


        Assert.AreEqual(1768156232522476283, res.Header.TsEvent);
        Assert.AreEqual(24, res.Header.Rtype);
        Assert.AreEqual(1, res.Header.PublisherId);
        Assert.AreEqual(42566722, res.Header.InstrumentId);
        Assert.AreEqual(470m, res.Quantity);
        Assert.AreEqual(StatisticType.OpenInterest, res.StatType);
    }
}
