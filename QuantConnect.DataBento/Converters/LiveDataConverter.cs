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
using QuantConnect.Lean.DataSource.DataBento.Models;
using QuantConnect.Lean.DataSource.DataBento.Models.Enums;
using QuantConnect.Lean.DataSource.DataBento.Models.Live;

namespace QuantConnect.Lean.DataSource.DataBento.Converters;

public class LiveDataConverter : JsonConverter<MarketDataRecord>
{
    private const string headerIdentifier = "hd";

    private const string recordTypeIdentifier = "rtype";

    private static JsonSerializer _snakeSerializer = new JsonSerializer
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
    public override MarketDataRecord ReadJson(JsonReader reader, Type objectType, MarketDataRecord? existingValue, bool hasExistingValue, JsonSerializer serializer)
    {
        var jObject = JObject.Load(reader);

        var recordType = jObject[headerIdentifier]?[recordTypeIdentifier]?.ToObject<RecordType>();

        switch (recordType)
        {
            case RecordType.SymbolMapping:
                return jObject.ToObject<SymbolMappingMessage>(_snakeSerializer);
            case RecordType.MarketByPriceDepth1:
                return jObject.ToObject<LevelOneData>(_snakeSerializer);
            default:
                return null;
        }
    }

    /// <summary>
    /// Writes the JSON representation of the object.
    /// </summary>
    /// <param name="writer">The <see cref="JsonWriter"/> to write to.</param>
    /// <param name="value">The value.</param>
    /// <param name="serializer">The calling serializer.</param>
    public override void WriteJson(JsonWriter writer, MarketDataRecord? value, JsonSerializer serializer)
    {
        throw new NotImplementedException();
    }
}