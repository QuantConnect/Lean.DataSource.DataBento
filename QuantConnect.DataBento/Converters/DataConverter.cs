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

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Logging;
using QuantConnect.Lean.DataSource.DataBento.Models;
using QuantConnect.Lean.DataSource.DataBento.Models.Live;
using QuantConnect.Lean.DataSource.DataBento.Models.Enums;

namespace QuantConnect.Lean.DataSource.DataBento.Converters;

public class DataConverter : JsonConverter<MarketDataBase>
{
    /// <summary>
    /// JSON property name used to identify the message header section.
    /// </summary>
    private const string headerIdentifier = "hd";

    /// <summary>
    /// JSON property name that specifies the market data record type.
    /// </summary>
    private const string recordTypeIdentifier = "rtype";

    /// <summary>
    /// Shared JSON serializer configured to use snake_case naming,
    /// used for deserializing market data payloads.
    /// </summary>
    private readonly static JsonSerializer _snakeSerializer = new()
    {
        ContractResolver = Serialization.SnakeCaseContractResolver.Instance
    };

    /// <summary>
    /// Gets a value indicating whether this <see cref="JsonConverter"/> can write JSON.
    /// </summary>
    /// <value><c>true</c> if this <see cref="JsonConverter"/> can write JSON; otherwise, <c>false</c>.</value>
    public override bool CanWrite => false;

    /// <summary>
    /// Gets a value indicating whether this <see cref="JsonConverter"/> can read JSON.
    /// </summary>
    /// <value><c>true</c> if this <see cref="JsonConverter"/> can read JSON; otherwise, <c>false</c>.</value>
    public override bool CanRead => true;

    /// <summary>
    /// Reads the JSON representation of the object.
    /// </summary>
    /// <param name="reader">The <see cref="JsonReader"/> to read from.</param>
    /// <param name="objectType">Type of the object.</param>
    /// <param name="existingValue">The existing property value of the JSON that is being converted.</param>
    /// <param name="serializer">The calling serializer.</param>
    /// <returns>The object value.</returns>
    public override MarketDataBase ReadJson(JsonReader reader, Type objectType, MarketDataBase existingValue, bool hasExistingValue, JsonSerializer serializer)
    {
        var jObject = JObject.Load(reader);

        var recordTypeToken = jObject[headerIdentifier]?[recordTypeIdentifier];
        if (recordTypeToken == null)
        {
            var msg = $"Cannot read '{recordTypeIdentifier}' from JSON";
            Log.Error($"{nameof(DataConverter)}.{nameof(ReadJson)}: {msg}. JSON: {jObject.ToString(Formatting.None)}.");
            throw new JsonSerializationException(msg);
        }

        var recordType = recordTypeToken.ToObject<RecordType>();

        var marketDataBase = default(MarketDataBase);
        switch (recordType)
        {
            case RecordType.OpenHighLowCloseVolume1Day:
            case RecordType.OpenHighLowCloseVolume1Hour:
            case RecordType.OpenHighLowCloseVolume1Minute:
            case RecordType.OpenHighLowCloseVolume1Second:
                marketDataBase = new OpenHighLowCloseVolumeData();
                break;
            case RecordType.MarketByPriceDepth1:
                marketDataBase = new LevelOneData();
                break;
            case RecordType.BBO1Second:
            case RecordType.BBO1Minute:
                marketDataBase = new BestBidOfferInterval();
                break;
            case RecordType.SymbolMapping:
                marketDataBase = new SymbolMappingMessage();
                break;
            case RecordType.Statistics:
                marketDataBase = new StatisticsData();
                break;
            case RecordType.System:
                marketDataBase = new SystemMessage();
                break;
            default:
                var msg = $"Unsupported RecordType '{recordType}'";
                Log.Error($"{nameof(DataConverter)}.{nameof(ReadJson)}: {msg}. JSON: {jObject.ToString(Formatting.None)}.");
                throw new NotSupportedException(msg);
        }

        _snakeSerializer.Populate(jObject.CreateReader(), marketDataBase);
        return marketDataBase;
    }

    /// <summary>
    /// Writes the JSON representation of the object.
    /// </summary>
    /// <param name="writer">The <see cref="JsonWriter"/> to write to.</param>
    /// <param name="value">The value.</param>
    /// <param name="serializer">The calling serializer.</param>
    public override void WriteJson(JsonWriter writer, MarketDataBase? value, JsonSerializer serializer)
    {
        throw new NotImplementedException();
    }
}