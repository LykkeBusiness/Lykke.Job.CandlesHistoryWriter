// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using MoreLinq;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesCacheInitalizationService : ICandlesCacheInitalizationService
    {
        private readonly ILog _log;
        private readonly IAssetPairsManager _assetPairsManager;
        private readonly IClock _clock;
        private readonly ICandlesCacheService _candlesCacheService;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ICandlesAmountManager _candlesAmountManager;
        private readonly ICandlesShardValidator _candlesShardValidator;
        private readonly int _cacheCandlesAssetsBatchSize;

        private const int DefaultCacheCandlesAssetsBatchSize = 100;

        public CandlesCacheInitalizationService(
            ILog log,
            IAssetPairsManager assetPairsManager,
            IClock clock,
            ICandlesCacheService candlesCacheService,
            ICandlesHistoryRepository candlesHistoryRepository,
            ICandlesAmountManager candlesAmountManager,
            ICandlesShardValidator candlesShardValidator,
            int? configuredCacheCandlesAssetsBatchSize)
        {
            _log = log;
            _assetPairsManager = assetPairsManager;
            _clock = clock;
            _candlesCacheService = candlesCacheService;
            _candlesHistoryRepository = candlesHistoryRepository;
            _candlesAmountManager = candlesAmountManager;
            _candlesShardValidator = candlesShardValidator;
            _cacheCandlesAssetsBatchSize = GetActualCacheCandlesAssetsBatchSize(configuredCacheCandlesAssetsBatchSize);

            if (_cacheCandlesAssetsBatchSize <= 10)
            {
                _log.WriteWarning(nameof(CandlesCacheInitalizationService),
                    new { ConfiguredCacheCandlesAssetsBatchSize = _cacheCandlesAssetsBatchSize }.ToJson(),
                    "Configured cache candles assets batch size is too low. " +
                    "It may lead to performance issues. " +
                    "Please consider increasing it.");
            }

            if (_cacheCandlesAssetsBatchSize != configuredCacheCandlesAssetsBatchSize)
            {
                _log.WriteWarning(nameof(CandlesCacheInitalizationService),
                    new
                    {
                        ConfiguredCacheCandlesAssetsBatchSize = configuredCacheCandlesAssetsBatchSize,
                        ActualCacheCandlesAssetsBatchSize = _cacheCandlesAssetsBatchSize
                    }.ToJson(),
                    "Configured cache candles assets batch size is invalid. " +
                    "Using default value instead.");
            }
        }

        private static int GetActualCacheCandlesAssetsBatchSize(int? configuredCacheCandlesAssetsBatchSize)
        {
            return configuredCacheCandlesAssetsBatchSize.HasValue
                ? configuredCacheCandlesAssetsBatchSize.Value <= 0
                    ? DefaultCacheCandlesAssetsBatchSize
                    : configuredCacheCandlesAssetsBatchSize.Value
                : DefaultCacheCandlesAssetsBatchSize;
        }

        public async Task InitializeCacheAsync()
        {
            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, "Caching candles history...");

            var assetPairs = await _assetPairsManager.GetAllEnabledAsync();
            var now = _clock.UtcNow;

            foreach (var cacheAssetPairBatch in assetPairs.Batch(_cacheCandlesAssetsBatchSize))
            {
                await Task.WhenAll(cacheAssetPairBatch.Select(assetPair => CacheAssetPairCandlesAsync(assetPair, now)));
            }

            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, "All candles history is cached");
        }

        private async Task CacheAssetPairCandlesAsync(AssetPair assetPair, DateTime now)
        {
            if (!_candlesShardValidator.CanHandle(assetPair.Id))
            {
                await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null,
                    $"Skipping {assetPair.Id} caching, since it doesn't meet sharding condition");

                return;
            }

            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, $"Caching {assetPair.Id} candles history...");

            try
            {
                foreach (var priceType in Constants.StoredPriceTypes)
                {
                    foreach (var timeInterval in Constants.StoredIntervals)
                    {
                        var alignedToDate = now.TruncateTo(timeInterval).AddIntervalTicks(1, timeInterval);
                        var candlesAmountToStore = _candlesAmountManager.GetCandlesAmountToStore(timeInterval);
                        var candles = await _candlesHistoryRepository.GetLastCandlesAsync(assetPair.Id, timeInterval, priceType, alignedToDate, candlesAmountToStore);

                        await _candlesCacheService.InitializeAsync(assetPair.Id, priceType, timeInterval, candles.ToArray());
                    }
                }
            }
            catch (Exception e)
            {
                await _log.WriteErrorAsync(nameof(CandlesCacheInitalizationService), nameof(CacheAssetPairCandlesAsync),
                    $"Couldn't cache candles history for asset pair [{assetPair.Id}]", e);
            }
            finally
            {
                await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(CacheAssetPairCandlesAsync), null,
                    $"{assetPair.Id} candles history caching finished");
            }
        }
    }
}
