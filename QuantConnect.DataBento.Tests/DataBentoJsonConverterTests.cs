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

using System;
using System.Linq;
using Newtonsoft.Json;
using NUnit.Framework;
using System.Collections.Generic;
using QuantConnect.Lean.DataSource.DataBento.Models;
using QuantConnect.Lean.DataSource.DataBento.Models.Live;
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
        var res = json.DeserializeObject<OpenHighLowCloseVolumeData>();

        Assert.IsNotNull(res);

        Assert.AreEqual(1738281600000000000m, res.Header.TsEvent);
        Assert.AreEqual(RecordType.OpenHighLowCloseVolume1Day, res.Header.Rtype);
        Assert.AreEqual(1, res.Header.PublisherId);
        Assert.AreEqual(42140878, res.Header.InstrumentId);

        Assert.AreEqual(6359m, res.Open);
        Assert.AreEqual(6359m, res.High);
        Assert.AreEqual(6355m, res.Low);
        Assert.AreEqual(6355m, res.Close);
        Assert.AreEqual(2L, res.Volume);
    }

    private static IEnumerable<TestCaseData> HistoricalLevelOneData
    {
        get
        {
            yield return new TestCaseData(@"{
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
}").SetArgDisplayNames("Normal");
            yield return new TestCaseData(@"{
    ""ts_recv"": ""1736722822572003465"",
    ""hd"": {
        ""ts_event"": ""1736722822571417935"",
        ""rtype"": 1,
        ""publisher_id"": 1,
        ""instrument_id"": 42140878
    },
    ""action"": ""A"",
    ""side"": ""N"",
    ""depth"": 0,
    ""price"": ""6100.000000000"",
    ""size"": 3,
    ""flags"": 130,
    ""ts_in_delta"": 13574,
    ""sequence"": 11624,
    ""levels"": [
        {
            ""bid_px"": null,
            ""ask_px"": ""6100.000000000"",
            ""bid_sz"": 0,
            ""ask_sz"": 3,
            ""bid_ct"": 0,
            ""ask_ct"": 1
        }
    ]
}").SetArgDisplayNames("BidPriceNull");
            yield return new TestCaseData(@"{
    ""ts_recv"": ""1736751414397721230"",
    ""hd"": {
        ""ts_event"": ""1736751414397599041"",
        ""rtype"": 1,
        ""publisher_id"": 1,
        ""instrument_id"": 42140878
    },
    ""action"": ""C"",
    ""side"": ""A"",
    ""depth"": 0,
    ""price"": ""6087.250000000"",
    ""size"": 3,
    ""flags"": 130,
    ""ts_in_delta"": 13718,
    ""sequence"": 934949,
    ""levels"": [
        {
            ""bid_px"": null,
            ""ask_px"": null,
            ""bid_sz"": 0,
            ""ask_sz"": 0,
            ""bid_ct"": 0,
            ""ask_ct"": 0
        }
    ]
}").SetArgDisplayNames("BidAndAskPriceNull");
            yield return new TestCaseData(@"{
    ""ts_recv"": ""1768137120000000000"",
    ""hd"": {
        ""ts_event"": ""18446744073709551615"",
        ""rtype"": 196,
        ""publisher_id"": 1,
        ""instrument_id"": 42140878
    },
    ""side"": ""N"",
    ""price"": null,
    ""size"": 0,
    ""flags"": 128,
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
}").SetArgDisplayNames("BBO1Minute.hd.ts_event == ulong MaxValue");
            yield return new TestCaseData(@"{
    ""ts_recv"": ""1768137120000000000"",
    ""hd"": {
        ""ts_event"": ""18446744073709551615"",
        ""rtype"": 196,
        ""publisher_id"": 1,
        ""instrument_id"": 42140878
    },
    ""side"": ""N"",
    ""price"": null,
    ""size"": 0,
    ""flags"": 128,
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
}").SetArgDisplayNames("BBO1Minute_PriceNull");
        }
    }

    [TestCaseSource(nameof(HistoricalLevelOneData))]
    public void DeserializeHistoricalLevelOneData(string json)
    {
        var res = json.DeserializeObject<LevelOneData>();

        Assert.IsNotNull(res);
        Assert.Greater(res.TsRecv, 0);
        Assert.Greater(res.Flags, 0);
        Assert.AreEqual(0, res.Depth);

        Assert.NotNull(res.Header);
        Assert.Greater(res.Header.TsEvent, 0);
        Assert.AreEqual(1, res.Header.PublisherId);
        Assert.Greater(res.Header.InstrumentId, 0);

        var dataTime = res.UtcDateTime.Value;
        Assert.AreNotEqual(default(DateTime), dataTime);

        Assert.That(
            res.Header.Rtype,
            Is.AnyOf(
                RecordType.MarketByPriceDepth1,
                RecordType.BBO1Second,
                RecordType.BBO1Minute
            ),
            $"Unexpected RecordType: {res.Header.Rtype}"
        );

        if (res.Action != default)
            Assert.IsTrue(char.IsLetter(res.Action), "Action must be a letter");
        Assert.IsTrue(char.IsLetter(res.Side), "Side must be a letter");

        if (res.Price is null)
        {
            Assert.AreEqual(0, res.Size);
        }
        else
        {
            Assert.Greater(res.Price, 0);
            Assert.Greater(res.Size, 0);
        }

        Assert.IsNotNull(res.Levels);
        Assert.AreEqual(1, res.Levels.Count);
        var level = res.Levels[0];
        AssertPositiveOrNull(level.BidPx);
        AssertPositiveOrNull(level.AskPx);
        Assert.GreaterOrEqual(level.BidSz, 0);
        Assert.GreaterOrEqual(level.AskSz, 0);
        Assert.GreaterOrEqual(level.BidCt, 0);
        Assert.GreaterOrEqual(level.AskCt, 0);
    }

    private void AssertPositiveOrNull(decimal? price)
    {
        if (price.HasValue)
        {
            Assert.Greater(price.Value, 0);
        }
        else
        {
            Assert.IsNull(price);
        }
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

        var res = json.DeserializeObject<Models.StatisticsData>();

        Assert.IsNotNull(res);


        Assert.AreEqual(1768156232522476283, res.Header.TsEvent);
        Assert.AreEqual(RecordType.Statistics, res.Header.Rtype);
        Assert.AreEqual(1, res.Header.PublisherId);
        Assert.AreEqual(42566722, res.Header.InstrumentId);
        Assert.AreEqual(470m, res.Quantity);
        Assert.AreEqual(StatisticType.OpenInterest, res.StatType);
    }

    private static IEnumerable<TestCaseData> SystemLiveMessages
    {
        get
        {
            yield return new TestCaseData(@"{""hd"":{""ts_event"":""1769176693139629181"",""rtype"":23,""publisher_id"":0,""instrument_id"":0},""msg"":""Heartbeat""}").SetArgDisplayNames("HeartbeatMessage");
            yield return new TestCaseData(@"{""hd"":{ ""ts_event"":""1769712709771313573"",""rtype"":23,""publisher_id"":0,""instrument_id"":0},""msg"":""Subscription request for mbp-1 data succeeded""}").SetArgDisplayNames("SubscriptionRequestMarketByPrice1DataSucceeded");
        }
    }

    [TestCaseSource(nameof(SystemLiveMessages))]
    public void DeserializeLiveSystemMessage(string json)
    {
        var res = json.DeserializeObject<SystemMessage>();

        Assert.IsNotNull(res);
        Assert.Greater(res.Header.TsEvent, 0);
        Assert.AreEqual(RecordType.System, res.Header.Rtype);
        Assert.AreEqual(0, res.Header.PublisherId);
        Assert.AreEqual(0, res.Header.InstrumentId);
        Assert.IsFalse(string.IsNullOrEmpty(res.Msg));
    }

    [TestCase("success=0|error=Unknown subscription param 'sssauth'", false)]
    [TestCase("success=0|error=User has reached their open connection limit", false)]
    [TestCase("success=0|error=Authentication failed.", false)]
    [TestCase("success=1|session_id=1769508116", true)]
    public void ParsePotentialAuthenticationMessageResponses(string authenticationResponse, bool success)
    {
        var auth = new AuthenticationMessageResponse(authenticationResponse);

        if (success)
        {
            Assert.IsTrue(auth.Success);
            Assert.AreNotEqual(0, auth.SessionId);
        }
        else
        {
            Assert.IsFalse(auth.Success);
            Assert.That(auth.Error, Is.Not.Null.And.Not.Empty);
        }
    }

    [TestCase("cram=HCxTgxMcqglVMTMeaDZ2ICmcnrW8j92e", "auth=a6c5c23e06854dc0310e11ce6d3081509e415a5a37a323bb94bc90f64c9214d4-12345|dataset=GLBX.MDP3|pretty_px=1|encoding=json|heartbeat_interval_s=5")]
    [TestCase("cram=HCxTgxMcqglVMTMeaDZ2ICmcnrW8j92e\n", "auth=a6c5c23e06854dc0310e11ce6d3081509e415a5a37a323bb94bc90f64c9214d4-12345|dataset=GLBX.MDP3|pretty_px=1|encoding=json|heartbeat_interval_s=5")]
    public void ParsePotentialCramChallenges(string challenge, string expectedString)
    {
        var auth = new AuthenticationMessageRequest(challenge, "my-api-key-12345", "GLBX.MDP3", TimeSpan.FromSeconds(5));

        var actualString = auth.ToString();

        Assert.AreEqual(expectedString, actualString);
    }

    [Test]
    public void DeserializeSymbolMappingMessage()
    {
        var json = @"{
    ""hd"": {
        ""ts_event"": ""1769546804979770503"",
        ""rtype"": 22,
        ""publisher_id"": 0,
        ""instrument_id"": 42140878
    },
    ""stype_in_symbol"": ""ESH6"",
    ""stype_out_symbol"": ""ESH6"",
    ""start_ts"": ""18446744073709551615"",
    ""end_ts"": ""18446744073709551615""
}";

        var marketData = json.DeserializeObject<MarketDataBase>();

        Assert.IsNotNull(marketData);
        Assert.AreEqual(1769546804979770503, marketData.Header.TsEvent);
        Assert.AreEqual(RecordType.SymbolMapping, marketData.Header.Rtype);
        Assert.AreEqual(0, marketData.Header.PublisherId);
        Assert.AreEqual(42140878, marketData.Header.InstrumentId);

        Assert.IsInstanceOf<SymbolMappingMessage>(marketData);

        var sm = marketData as SymbolMappingMessage;

        Assert.AreEqual("ESH6", sm.StypeInSymbol);
        Assert.AreEqual("ESH6", sm.StypeOutSymbol);
    }


    [Test]
    public void DeserializeMarketByPriceMessage()
    {
        var json = @"
{
    ""ts_recv"": ""1769546804990938439"",
    ""hd"": {
        ""ts_event"": ""1769546804990833083"",
        ""rtype"": 1,
        ""publisher_id"": 1,
        ""instrument_id"": 42005017
    },
    ""action"": ""A"",
    ""side"": ""A"",
    ""depth"": 0,
    ""price"": ""2676.400000000"",
    ""size"": 1,
    ""flags"": 128,
    ""ts_in_delta"": 17695,
    ""sequence"": 126257483,
    ""levels"": [
        {
            ""bid_px"": ""2676.300000000"",
            ""ask_px"": ""2676.400000000"",
            ""bid_sz"": 14,
            ""ask_sz"": 2,
            ""bid_ct"": 8,
            ""ask_ct"": 2
        }
    ]
}";
        var marketData = json.DeserializeObject<MarketDataBase>();

        Assert.IsNotNull(marketData);
        Assert.AreEqual(1769546804990833083, marketData.Header.TsEvent);
        Assert.AreEqual(RecordType.MarketByPriceDepth1, marketData.Header.Rtype);
        Assert.AreEqual(1, marketData.Header.PublisherId);
        Assert.AreEqual(42005017, marketData.Header.InstrumentId);

        Assert.IsInstanceOf<LevelOneData>(marketData);

        var mbp = marketData as LevelOneData;
        Assert.AreEqual('A', mbp.Action);
        Assert.AreEqual('A', mbp.Side);
        Assert.AreEqual(0, mbp.Depth);
        Assert.AreEqual(2676.4m, mbp.Price);
        Assert.AreEqual(1, mbp.Size);
        Assert.AreEqual(128, mbp.Flags);
        Assert.IsNotNull(mbp.Levels);
        Assert.AreEqual(1, mbp.Levels.Count);
        var level = mbp.Levels[0];
        Assert.AreEqual(2676.3m, level.BidPx);
        Assert.AreEqual(2676.4m, level.AskPx);
        Assert.AreEqual(14, level.BidSz);
        Assert.AreEqual(2, level.AskSz);
        Assert.AreEqual(8, level.BidCt);
        Assert.AreEqual(2, level.AskCt);
    }

    [Test]
    public void DeserializeUnknownJsonFormatShouldThrow()
    {
        var json = @"{ ""some_property"": ""some_value"" }";
        Assert.Throws<JsonSerializationException>(() => json.DeserializeObject<MarketDataBase>());

        var json2 = @"{ ""hd"": { ""rtype"": 9999 } }";
        Assert.Throws<NotSupportedException>(() => json2.DeserializeObject<MarketDataBase>());
    }

    private static IEnumerable<TestCaseData> ErrorResponses
    {
        get
        {
            yield return new TestCaseData(@"{
    ""detail"": {
        ""case"": ""data_end_after_available_end"",
        ""message"": ""The dataset GLBX.MDP3 has data available up to '2026-01-29 20:00:00+00:00'. The `end` in the query ('2026-01-29 20:23:08.167605900+00:00') is after the available range. Try requesting with an earlier `end`."",
        ""status_code"": 422,
        ""docs"": ""https://databento.com/docs/api-reference-historical/basics/datasets"",
        ""payload"": {
            ""dataset"": ""GLBX.MDP3"",
            ""start"": ""2026-01-28T18:00:00.000000000Z"",
            ""end"": ""2026-01-29T20:23:08.167605900Z"",
            ""available_start"": ""2010-06-06T00:00:00.000000000Z"",
            ""available_end"": ""2026-01-29T20:00:00.000000000Z""
        }
    }
}").SetArgDisplayNames("DataEndAfterAvailableEnd");

            yield return new TestCaseData(@"{
    ""detail"": {
        ""case"": ""data_start_before_available_start"",
        ""message"": ""`start` in query ('2010-01-11 00:00:00+00:00') was before the available start of dataset GLBX.MDP3 ('2010-06-06 00:00:00+00:00'). Try requesting with a later `start`."",
        ""status_code"": 422,
        ""docs"": ""https://databento.com/docs/api-reference-historical/basics/datasets"",
        ""payload"": {
            ""dataset"": ""GLBX.MDP3"",
            ""start"": ""2010-01-11T00:00:00.000000000Z"",
            ""end"": ""2026-01-30T00:00:00.000000000Z"",
            ""available_start"": ""2010-06-06T00:00:00.000000000Z"",
            ""available_end"": ""2026-01-29T00:00:00.000000000Z""
        }
    }
}").SetArgDisplayNames("DataStartBeforeAvailableStart");

            yield return new TestCaseData(@"{
    ""detail"": {
        ""case"": ""data_time_range_start_on_or_after_end"",
        ""message"": ""Invalid time range query, `start` 2026-01-30 00:10:00+00:00 cannot be on or after `end` 2026-01-30 00:05:00+00:00."",
        ""status_code"": 422,
        ""docs"": ""https://databento.com/docs/standards-and-conventions/common-fields-enums-types"",
        ""payload"": {
            ""start"": ""2026-01-30T00:10:00.000000000Z"",
            ""end"": ""2026-01-30T00:05:00.000000000Z""
        }
    }
}").SetArgDisplayNames("DataTimeRangeStartOnOrAfterEnd");

            yield return new TestCaseData(@"{
    ""detail"": {
        ""case"": ""data_start_after_available_end"",
        ""message"": ""`start` in query ('2026-01-30 13:16:22+00:00') was after the available end of dataset GLBX.MDP3 ('2026-01-30 13:00:00+00:00'). Try requesting with an earlier `start`."",
        ""status_code"": 422,
        ""docs"": ""https://databento.com/docs/api-reference-historical/basics/datasets"",
        ""payload"": {
            ""dataset"": ""GLBX.MDP3"",
            ""start"": ""2026-01-30T13:16:22.000000000Z"",
            ""end"": ""2026-01-30T13:16:32.149178800Z"",
            ""available_start"": ""2010-06-06T00:00:00.000000000Z"",
            ""available_end"": ""2026-01-30T13:00:00.000000000Z""
        }
    }
}").SetArgDisplayNames("DataStartAfterAvailableEnd");
        }
    }

    [TestCaseSource(nameof(ErrorResponses))]
    public void DeserializeErrorResponse(string json)
    {
        var error = json.DeserializeObject<ErrorResponse>();

        Assert.IsNotNull(error);
        Assert.IsNotNull(error.Detail);
        Assert.That(error.Detail.Case, Is.Not.Null.And.Not.Empty);
        var validCases = new[]
        {
            ErrorCases.DataEndAfterAvailableEnd,
            ErrorCases.DataStartBeforeAvailableStart,
            ErrorCases.DataStartAfterAvailableEnd,
            ErrorCases.DataTimeRangeStartOnOrAfterEnd
        };
        Assert.IsTrue(validCases.Any(x => x.Equals(error.Detail.Case, StringComparison.InvariantCultureIgnoreCase)));

        Assert.That(error.Detail.Message, Is.Not.Null.And.Not.Empty);
        Assert.Greater(error.Detail.StatusCode, 0);
        Assert.That(error.Detail.Docs, Is.Not.Null.And.Not.Empty);

        Assert.IsNotNull(error.Detail.Payload);
        Assert.AreEqual(422, error.Detail.StatusCode);
        if (!string.IsNullOrEmpty(error.Detail.Payload.Dataset))
        {
            Assert.AreEqual("GLBX.MDP3", error.Detail.Payload.Dataset);
        }

        Assert.AreNotEqual(default(DateTime), error.Detail.Payload.Start);
        Assert.AreNotEqual(default(DateTime), error.Detail.Payload.End);
        Assert.AreNotEqual(default(DateTime), error.Detail.Payload.AvailableStart);
        Assert.AreNotEqual(default(DateTime), error.Detail.Payload.AvailableEnd);
    }
}
