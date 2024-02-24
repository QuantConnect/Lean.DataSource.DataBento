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

using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Util;
using HistoryRequest = QuantConnect.Data.HistoryRequest;

namespace QuantConnect.DateBento
{
    public class DataBentoDataDownloader : IDataDownloader, IDisposable
    {
        private readonly DataBentoHistoryProvider _historyProvider = new();
        private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

        public IEnumerable<BaseData> Get(DataDownloaderGetParameters dataDownloaderGetParameters)
        {
            var symbol = dataDownloaderGetParameters.Symbol;
            var tickType = dataDownloaderGetParameters.TickType;
            var resolution = dataDownloaderGetParameters.Resolution;

            if (tickType == TickType.OpenInterest)
            {
                Log.Error(
                    $"{nameof(DataBentoDataDownloader)}.{nameof(Get)}: Historical data request with TickType 'OpenInterest' is not supported");
                yield break;
            }

            if (tickType == TickType.Quote && resolution is not Resolution.Tick or Resolution.Second)
            {
                Log.Error(
                    $"{nameof(DataBentoDataDownloader)}.{nameof(Get)}: Historical data request with TickType 'Quote' is not supported for resolutions other than Tick. Requested Resolution: {resolution}");
                yield break;
            }

            if (dataDownloaderGetParameters.EndUtc < dataDownloaderGetParameters.StartUtc)
            {
                Log.Error(
                    $"{nameof(DataBentoDataDownloader)}.{nameof(Get)}:InvalidDateRange. The history request start date must precede the end date, no history returned");
                yield break;
            }


            var historyRequests = new HistoryRequest(
                startTimeUtc: dataDownloaderGetParameters.StartUtc,
                endTimeUtc: dataDownloaderGetParameters.EndUtc,
                dataType: resolution == Resolution.Tick ? typeof(Tick) : typeof(TradeBar),
                symbol: symbol,
                resolution: resolution,
                exchangeHours: _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType),
                dataTimeZone: _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType),
                fillForwardResolution: dataDownloaderGetParameters.Resolution,
                includeExtendedMarketHours: true,
                isCustomData: false,
                dataNormalizationMode: DataNormalizationMode.Raw,
                tickType: tickType);

            foreach (var slice in _historyProvider.GetHistory(historyRequests))
            {
                yield return slice;
            }
        }

        public void Dispose()
        {
            _historyProvider.DisposeSafely();
        }
    }
}