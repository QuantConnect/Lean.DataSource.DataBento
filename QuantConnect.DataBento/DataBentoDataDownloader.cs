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
 *
*/

using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Securities;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.DataBento;

/// <summary>
/// Data downloader class for pulling data from DataBento
/// </summary>
public class DataBentoDataDownloader : IDataDownloader, IDisposable
{
    /// <summary>
    /// Provides access to historical market data via the DataBento service.
    /// </summary>
    private readonly DataBentoProvider _historyProvider;

    /// <summary>
    /// Provides exchange trading hours and market-specific time zone information.
    /// </summary>
    private readonly MarketHoursDatabase _marketHoursDatabase;

    /// <summary>
    /// Initializes a new instance of the <see cref="DataBentoDataDownloader"/>
    /// getting the DataBento API key from the configuration
    /// </summary>
    public DataBentoDataDownloader()
        : this(Config.Get("databento-api-key"))
    {

    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataBentoDataDownloader"/>
    /// </summary>
    /// <param name="apiKey">The DataBento API key.</param>
    public DataBentoDataDownloader(string apiKey)
    {
        _historyProvider = new DataBentoProvider(apiKey);
        _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
    }

    /// <summary>
    /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
    /// </summary>
    /// <param name="parameters">Parameters for the historical data request</param>
    /// <returns>Enumerable of base data for this symbol</returns>
    public IEnumerable<BaseData>? Get(DataDownloaderGetParameters parameters)
    {
        var symbol = parameters.Symbol;
        var resolution = parameters.Resolution;
        var startUtc = parameters.StartUtc;
        var endUtc = parameters.EndUtc;
        var tickType = parameters.TickType;

        var dataType = LeanData.GetDataType(resolution, tickType);
        var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
        var dataTimeZone = _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

        var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, symbol, resolution, exchangeHours, dataTimeZone, resolution,
            true, false, DataNormalizationMode.Raw, tickType);

        var historyData = _historyProvider.GetHistory(historyRequest);

        if (historyData == null)
        {
            return null;
        }

        return historyData;
    }


    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        _historyProvider.DisposeSafely();
    }
}