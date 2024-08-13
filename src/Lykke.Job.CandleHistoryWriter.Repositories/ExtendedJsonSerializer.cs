// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Text.Json;

namespace Lykke.Job.CandleHistoryWriter.Repositories
{
    public static class ExtendedJsonSerializer
    {
        public static bool TrySerialize<TValue>(TValue value, out string json ,JsonSerializerOptions? options = null)
        {
            json = string.Empty;
            try
            {
                json = JsonSerializer.Serialize(value);
                return true;
            }
            catch (Exception)
            {
                // ignored
            }
            return false;
        }
    }
}
