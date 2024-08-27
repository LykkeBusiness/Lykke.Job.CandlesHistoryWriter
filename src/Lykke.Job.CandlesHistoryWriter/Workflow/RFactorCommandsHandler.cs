// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CorporateActions.Broker.Contracts.Workflow;
using JetBrains.Annotations;
using Lykke.Cqrs;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.Extensions.Logging;

namespace Lykke.Job.CandlesHistoryWriter.Workflow
{
    public class RFactorCommandsHandler
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ICandlesCacheInitializationService _candlesCacheInitializationService;
        private readonly ICandlesHistoryBackupService _candlesHistoryBackupService;
        private readonly ILogger<RFactorCommandsHandler> _logger;

        public RFactorCommandsHandler(ICandlesHistoryRepository candlesHistoryRepository,
            ICandlesCacheInitializationService candlesCacheInitializationService,
            ICandlesHistoryBackupService candlesHistoryBackupService,
            ILogger<RFactorCommandsHandler> logger)
        {
            _candlesHistoryRepository = candlesHistoryRepository;
            _candlesCacheInitializationService = candlesCacheInitializationService;
            _candlesHistoryBackupService = candlesHistoryBackupService;
            _logger = logger;
        }

        [UsedImplicitly]
        public async Task Handle(BackupHistoricalCandlesCommand command, IEventPublisher publisher)
        {
            _logger.LogInformation("{Command} received for product {ProductId}, task id {Id}",
                nameof(BackupHistoricalCandlesCommand),
                command.ProductId,
                command.TaskId);

            await _candlesHistoryBackupService.Backup(command.ProductId);

            publisher.PublishEvent(new BackupHistoricalCandlesFinishedEvent { TaskId = command.TaskId });
        }

        [UsedImplicitly]
        public async Task Handle(UpdateHistoricalCandlesCommand command, IEventPublisher publisher)
        {
            _logger.LogInformation("{Command} received for product {ProductId}, task id {Id}",
                nameof(UpdateHistoricalCandlesCommand),
                command.ProductId,
                command.TaskId);

            await _candlesHistoryRepository.ApplyRFactor(command.ProductId, command.RFactor, command.RFactorDate,
                command.LastTradingDay);

            publisher.PublishEvent(new HistoricalCandlesUpdatedEvent { TaskId = command.TaskId, });
        }

        [UsedImplicitly]
        public async Task Handle(UpdateCandlesCacheCommand command, IEventPublisher publisher)
        {
            _logger.LogInformation("{Command} received for product {ProductId}, task id {Id}",
                nameof(UpdateCandlesCacheCommand),
                command.ProductId,
                command.TaskId);

            await _candlesCacheInitializationService.InitializeCacheAsync(command.ProductId);

            publisher.PublishEvent(new CandlesCacheUpdatedEvent { TaskId = command.TaskId, });
        }
    }
}
