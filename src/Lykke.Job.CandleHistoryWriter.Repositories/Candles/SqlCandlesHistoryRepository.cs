// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.RFactor;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.SettingsReader;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class SqlCandlesHistoryRepository : ICandlesHistoryRepository
    {
        private readonly IHealthService _healthService;
        private readonly ILog _log;
        private readonly IReloadingManager<string> _assetConnectionString;

        private readonly ConcurrentDictionary<string, SqlAssetPairCandlesHistoryRepository> _sqlAssetPairRepositories;

        public SqlCandlesHistoryRepository(IHealthService healthService, ILog log, 
            IReloadingManager<string> assetConnectionString)
        {
            _healthService = healthService;
            _log = log;
            _assetConnectionString = assetConnectionString;

            _sqlAssetPairRepositories = new ConcurrentDictionary<string, SqlAssetPairCandlesHistoryRepository>();
        }

        public async Task InsertOrMergeAsync(IEnumerable<ICandle> candles, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            var repo = GetRepo(assetPairId);

            await repo.InsertOrMergeAsync(candles);

        }

        public async Task<IEnumerable<ICandle>> GetCandlesAsync(string assetPairId, CandleTimeInterval interval,
            CandlePriceType priceType, DateTime from, DateTime to)
        {
            var repo = GetRepo(assetPairId);

            return await repo.GetCandlesAsync(priceType, interval, from, to);

        }
        
        public async Task<IEnumerable<ICandle>> GetCandlesAsync(string assetPairId, DateTime from, DateTime to)
        {
            var repo = GetRepo(assetPairId);

            return await repo.GetCandlesAsync(from, to);

        }

        public async Task<IEnumerable<ICandle>> GetLastCandlesAsync(string assetPairId, CandleTimeInterval interval,
            CandlePriceType priceType, DateTime to, int number)
        {
            var repo = GetRepo(assetPairId);

            return await repo.GetLastCandlesAsync(priceType, interval, to, number);

        }

        public async Task<ICandle> TryGetFirstCandleAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType)
        {
            var repo = GetRepo(assetPairId);

            return await repo.TryGetFirstCandleAsync(priceType, interval);

        }

        public async Task<int> DeleteCandlesAsync(IReadOnlyList<ICandle> candlesToDelete)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            var (assetPairId, interval, priceType) = PreEvaluateInputCandleSet(candlesToDelete);

            var repo = GetRepo(assetPairId);
            return
                // ReSharper disable once PossibleMultipleEnumeration
                await repo.DeleteCandlesAsync(candlesToDelete, priceType);
        }

        public async Task<int> ReplaceCandlesAsync(IReadOnlyList<ICandle> candlesToReplace)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            var (assetPairId, interval, priceType) = PreEvaluateInputCandleSet(candlesToReplace);

            var repo = GetRepo(assetPairId);
            return
                // ReSharper disable once PossibleMultipleEnumeration
                await repo.ReplaceCandlesAsync(candlesToReplace, priceType);

        }

        public async Task ApplyRFactor(string productId, decimal rFactor, DateTime rFactorDate, DateTime lastTradingDay)
        {
            var repo = GetRepo(productId);
            var commands = new List<UpdateCandlesCommand>
            {
                new UpdateShortLivedCandlesCommand(rFactor, rFactorDate),
                new UpdateOldMonthlyCandlesCommand(rFactor, rFactorDate),
                new UpdateOldWeeklyCandlesCommand(rFactor, rFactorDate),
            };

            if (!CandlesHistoryWriter.Core.Domain.Candles.DateTimeExtensions.SameWeek(rFactorDate, lastTradingDay, DayOfWeek.Monday))
            {
                commands.Add(new UpdateBrokenWeeklyCandlesCommand(rFactor, rFactorDate));
            }

            if (!CandlesHistoryWriter.Core.Domain.Candles.DateTimeExtensions.SameMonth(rFactorDate, lastTradingDay))
            {
                commands.Add(new UpdateBrokenMonthlyCandlesCommand(rFactor, rFactorDate));
            }

            await repo.ApplyRFactor(commands);
        }

        private SqlAssetPairCandlesHistoryRepository GetRepo(string assetPairId) =>
            _sqlAssetPairRepositories.GetOrAdd(assetPairId, id =>
                new SqlAssetPairCandlesHistoryRepository(id, _assetConnectionString.CurrentValue, _log));

        private (string assetPairId, CandleTimeInterval interval, CandlePriceType priceType) PreEvaluateInputCandleSet(
            IEnumerable<ICandle> candlesToCheck)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            var firstCandle = candlesToCheck?.FirstOrDefault();
            if (firstCandle == null)
                throw new ArgumentException("The input candle set is null or empty.");

            var assetPairId = firstCandle.AssetPairId;
            var interval = firstCandle.TimeInterval;
            var priceType = firstCandle.PriceType;

            // ReSharper disable once PossibleMultipleEnumeration
            if (candlesToCheck.Any(c =>
                c.AssetPairId != firstCandle.AssetPairId ||
                c.TimeInterval != firstCandle.TimeInterval ||
                c.PriceType != firstCandle.PriceType))
                throw new ArgumentException("The input set contains candles with different asset pair IDs, time intervals and/or price types.");

            return (assetPairId: assetPairId,
                interval: interval,
                priceType: priceType);
        }
    }
}
