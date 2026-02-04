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
using QuantConnect.Lean.DataSource.DataBento.Models;

namespace QuantConnect.Lean.DataSource.DataBento.Tests;

public static class Assertions
{
    public static void AssertLevelOneBookLevel(LevelOneBookLevel level)
    {
        Assert.IsNotNull(level);
        AssertPositiveOrNull(level.BidPx);
        AssertPositiveOrNull(level.AskPx);
        Assert.GreaterOrEqual(level.BidSz, 0);
        Assert.GreaterOrEqual(level.AskSz, 0);
        Assert.GreaterOrEqual(level.BidCt, 0);
        Assert.GreaterOrEqual(level.AskCt, 0);
    }

    public static void AssertPositiveOrNull(decimal? price)
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

    public static void AssertEnumIsDefined<TEnum>(TEnum value, string paramName) where TEnum : struct, Enum
    {
        if (!Enum.IsDefined(value))
        {
            Assert.Fail($"{paramName} must be one of {string.Join(", ", Enum.GetValues<TEnum>())}");
        }
    }
}
