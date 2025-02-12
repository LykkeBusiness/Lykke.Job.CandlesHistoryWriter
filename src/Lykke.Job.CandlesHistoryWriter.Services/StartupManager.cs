﻿// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Cqrs;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.RabbitMqBroker.Subscriber;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    [UsedImplicitly]
    public class StartupManager : IStartupManager
    {
        private readonly ILog _log;
        private readonly ICandlesCacheInitializationService _cacheInitalizationService;
        private readonly ISnapshotSerializer _snapshotSerializer;
        private readonly ICandlesPersistenceQueueSnapshotRepository _persistenceQueueSnapshotRepository;
        private readonly ICandlesPersistenceQueue _persistenceQueue;
        private readonly ICandlesPersistenceManager _persistenceManager;
        private readonly bool _migrationEnabled;
        private readonly ICqrsEngine _cqrsEngine;
        private readonly IEnumerable<INeedInitialization> _needInitializations;
        private readonly RedisCacheTruncator _redisCacheTruncator;
        private readonly RabbitMqListener<CandlesUpdatedEvent> _candlesUpdatedListener;

        public StartupManager(
            ILog log,
            ICandlesCacheInitializationService cacheInitalizationService,
            ISnapshotSerializer snapshotSerializer,
            ICandlesPersistenceQueueSnapshotRepository persistenceQueueSnapshotRepository,
            ICandlesPersistenceQueue persistenceQueue,
            ICandlesPersistenceManager persistenceManager,
            bool migrationEnabled,
            ICqrsEngine cqrsEngine,
            IEnumerable<INeedInitialization> needInitializations,
            RedisCacheTruncator redisCacheTruncator,
            RabbitMqListener<CandlesUpdatedEvent> candlesUpdatedListener)
        {
            if (log == null)
                throw new ArgumentNullException(nameof(log));
            _log = log.CreateComponentScope(nameof(StartupManager)) ??
                   throw new InvalidOperationException("Couldn't create a component scope for logging.");

            _cacheInitalizationService = cacheInitalizationService ??
                                         throw new ArgumentNullException(nameof(cacheInitalizationService));
            _snapshotSerializer = snapshotSerializer ?? throw new ArgumentNullException(nameof(snapshotSerializer));
            _persistenceQueueSnapshotRepository = persistenceQueueSnapshotRepository ??
                                                  throw new ArgumentNullException(
                                                      nameof(persistenceQueueSnapshotRepository));
            _persistenceQueue = persistenceQueue ?? throw new ArgumentNullException(nameof(persistenceQueue));
            _persistenceManager = persistenceManager ?? throw new ArgumentNullException(nameof(persistenceManager));
            _migrationEnabled = migrationEnabled;
            _cqrsEngine = cqrsEngine ?? throw new ArgumentNullException(nameof(cqrsEngine));
            _needInitializations = needInitializations;
            _redisCacheTruncator = redisCacheTruncator;
            _candlesUpdatedListener = candlesUpdatedListener;
        }

        public async Task StartAsync()
        {
            await _log.WriteInfoAsync(nameof(StartAsync), "", "Deserializing persistence queue async...");

            await _snapshotSerializer.DeserializeAsync(_persistenceQueue, _persistenceQueueSnapshotRepository);

            if (!_migrationEnabled)
            {
                await _log.WriteInfoAsync(nameof(StartAsync), "", "Initializing cache from the history async...");

                await _cacheInitalizationService.InitializeCacheAsync();
            }

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Starting cache truncator...");

            _redisCacheTruncator.Start();

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Starting persistence queue...");

            _persistenceQueue.Start();

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Starting persistence manager...");

            _persistenceManager.Start();

            if (!_migrationEnabled)
            {

                await _log.WriteInfoAsync(nameof(StartAsync), "", "Starting candles updated listener...");

                _candlesUpdatedListener.Start();
            }

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Starting cqrs engine ...");

            _cqrsEngine.StartAll();

            await _log.WriteInfoAsync(nameof(StartAsync), "", "Started up");

            foreach (var needInitialization in _needInitializations)
            {
                await _log.WriteInfoAsync(nameof(StartAsync), "", $"Initializing {needInitialization.GetType().Name} ...");
                await needInitialization.InitializeAsync();
            }
        }
    }
}
