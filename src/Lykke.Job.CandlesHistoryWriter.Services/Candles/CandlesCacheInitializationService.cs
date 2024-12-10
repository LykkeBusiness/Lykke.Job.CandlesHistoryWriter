// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using MoreLinq;
using Polly;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesCacheInitializationService : ICandlesCacheInitializationService
    {
        private readonly ILog _log;
        private readonly IAssetPairsManager _assetPairsManager;
        private readonly IClock _clock;
        private readonly ICandlesCacheService _candlesCacheService;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ICandlesAmountManager _candlesAmountManager;
        private readonly ICandlesShardValidator _candlesShardValidator;
        private readonly int _cacheCandlesAssetsBatchSize;
        private readonly int _cacheCandlesAssetsRetryCount;

        /*
        last known initialization time of candles in BBVA with a batch of 100 equals to 190 seconds. we need to retry longer
        redis timeout is 5 seconds, so
        8 attempts means 2^0 + 2^1 +...+ 2^7 + 7*5=290 which is > 190
        */
        private const int DefaultCacheCandlesAssetsRetryCount = 8;
        private const int DefaultCacheCandlesAssetsBatchSize = 100;

        public CandlesCacheInitializationService(
            ILog log,
            IAssetPairsManager assetPairsManager,
            IClock clock,
            ICandlesCacheService candlesCacheService,
            ICandlesHistoryRepository candlesHistoryRepository,
            ICandlesAmountManager candlesAmountManager,
            ICandlesShardValidator candlesShardValidator,
            int? configuredCacheCandlesAssetsBatchSize,
            int? configuredCacheCandlesAssetsRetryCount)
        {
            _log = log;
            _assetPairsManager = assetPairsManager;
            _clock = clock;
            _candlesCacheService = candlesCacheService;
            _candlesHistoryRepository = candlesHistoryRepository;
            _candlesAmountManager = candlesAmountManager;
            _candlesShardValidator = candlesShardValidator;
            _cacheCandlesAssetsBatchSize = GetSafePositiveValue(configuredCacheCandlesAssetsBatchSize, DefaultCacheCandlesAssetsBatchSize);
            _cacheCandlesAssetsRetryCount = GetSafePositiveValue(configuredCacheCandlesAssetsRetryCount, DefaultCacheCandlesAssetsRetryCount);

            if (_cacheCandlesAssetsBatchSize <= 10)
            {
                _log.Warning(
                    "Configured cache candles assets batch size is too low. " +
                    "It may lead to performance issues. " +
                    "Please consider increasing it.",
                    context: new { ConfiguredCacheCandlesAssetsBatchSize = _cacheCandlesAssetsBatchSize }.ToJson(),
                    process: nameof(CandlesCacheInitializationService)
                );
            }

            if (_cacheCandlesAssetsBatchSize != configuredCacheCandlesAssetsBatchSize)
            {
                _log.Warning(
                    "Configured cache candles assets batch size is invalid. " +
                    "Using default value instead.",
                    context: new
                    {
                        ConfiguredCacheCandlesAssetsBatchSize = configuredCacheCandlesAssetsBatchSize,
                        ActualCacheCandlesAssetsBatchSize = _cacheCandlesAssetsBatchSize
                    }.ToJson(),
                    process: nameof(CandlesCacheInitializationService)
                );
            }
        }

        private static int GetSafePositiveValue(int? value, int defaultValue)
        {
            return value.HasValue
                ? value.Value <= 0
                    ? defaultValue
                    : value.Value
                : defaultValue;
        }

        public async Task InitializeCacheAsync()
        {
            _log.Info(nameof(CandlesCacheInitializationService), "Caching candles history...", nameof(InitializeCacheAsync));

            var assetPairs = await _assetPairsManager.GetAllEnabledAsync();
            var now = _clock.UtcNow;

            foreach (var cacheAssetPairBatch in assetPairs.Batch(_cacheCandlesAssetsBatchSize))
            {
                await Task.WhenAll(cacheAssetPairBatch.Select(assetPair => CacheAssetPairCandlesAsync(assetPair.Id, now)));
            }

            _log.Info(nameof(CandlesCacheInitializationService), "All candles history is cached", nameof(InitializeCacheAsync));
        }

        public async Task InitializeCacheAsync(string productId)
        {
            var now = _clock.UtcNow;

            await CacheAssetPairCandlesAsync(productId, now);
        }

        private async Task CacheAssetPairCandlesAsync(string productId, DateTime now)
        {
            if (!_candlesShardValidator.CanHandle(productId))
            {
                _log.Info(nameof(CandlesCacheInitializationService), $"Skipping {productId} caching, since it doesn't meet sharding condition", nameof(InitializeCacheAsync));

                return;
            }

            _log.Info(nameof(CandlesCacheInitializationService), $"Caching {productId} candles history...", nameof(InitializeCacheAsync));
            
            var policy = CreateRetryPolicy(productId);
            
            try
            {
                foreach (var priceType in Constants.StoredPriceTypes)
                {
                    foreach (var timeInterval in Constants.StoredIntervals)
                    {
                        var alignedToDate = now.TruncateTo(timeInterval).AddIntervalTicks(1, timeInterval);
                        var candlesAmountToStore = _candlesAmountManager.GetCandlesAmountToStore(timeInterval);
                        var candles = await _candlesHistoryRepository.GetLastCandlesAsync(productId, timeInterval, priceType, alignedToDate, candlesAmountToStore);
                        await policy.ExecuteAsync(async () => await _candlesCacheService.InitializeAsync(productId, priceType, timeInterval, candles.ToArray()));
                        
                        _log.Info(nameof(CandlesCacheInitializationService), $"{productId} candles history caching finished", nameof(InitializeCacheAsync));
                    }
                }
            }
            catch (Exception e)
            {
                throw new AggregateException($"Couldn't cache candles history for asset pair [{productId}] after {_cacheCandlesAssetsRetryCount} retries. Restart required." + Environment.NewLine +
                                             "Increase number of attempts in config if you see this second time. Every attempt increases wait time exponentially on a base of 2.", e);
            }
        }

        private AsyncPolicy CreateRetryPolicy(string productId)
        {
            return Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(_cacheCandlesAssetsRetryCount,
                    x => TimeSpan.FromSeconds(Math.Pow(2, x - 1)),
                    (exception, _) => _log.Info($"Caching {productId} candles history: retry."));
        }
    }
}
