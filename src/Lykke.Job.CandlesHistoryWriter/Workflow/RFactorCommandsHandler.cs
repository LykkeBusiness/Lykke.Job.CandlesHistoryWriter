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
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.Extensions.Logging;

namespace Lykke.Job.CandlesHistoryWriter.Workflow
{
    public class RFactorCommandsHandler
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ICandlesManager _candlesManager;
        private readonly ILogger<RFactorCommandsHandler> _logger;

        public RFactorCommandsHandler(ICandlesHistoryRepository candlesHistoryRepository,
            ICandlesManager candlesManager,
            ILogger<RFactorCommandsHandler> logger)
        {
            _candlesHistoryRepository = candlesHistoryRepository;
            _candlesManager = candlesManager;
            _logger = logger;
        }

        [UsedImplicitly]
        public async Task Handle(UpdateHistoricalCandlesCommand command, IEventPublisher publisher)
        {
            _logger.LogInformation("{Command} received for product {ProductId}",
                nameof(UpdateHistoricalCandlesCommand),
                command.ProductId);

            var candles = (await _candlesHistoryRepository.GetCandlesAsync(command.ProductId,
                    command.RFactorDate.Date,
                    DateTime.UtcNow))
                .ToList();

            var updatedCandles = new List<ICandle>();

            var rFactor = decimal.ToDouble(command.RFactor);

            if (command.UpdateAllCandles)
            {
                updatedCandles = candles
                    .Select(x =>
                    {
                        var candle = Candle.Copy(x);
                        return candle.UpdateRFactor(rFactor);
                    })
                    .ToList();
            }
            else
            {
                if (command.UpdateMonthlyCandles)
                {
                    updatedCandles = candles
                        .Where(x => x.TimeInterval == CandleTimeInterval.Month)
                        .Select(x =>
                        {
                            var candle = Candle.Copy(x);
                            return candle.UpdateMonthlyOrWeeklyRFactor(rFactor);
                        })
                        .ToList();
                }

                if (command.UpdateWeeklyCandles)
                {
                    updatedCandles = candles
                        .Where(x => x.TimeInterval == CandleTimeInterval.Week)
                        .Select(x =>
                        {
                            var candle = Candle.Copy(x);
                            return candle.UpdateMonthlyOrWeeklyRFactor(rFactor);
                        })
                        .ToList();
                }
            }

            await _candlesManager.ProcessCandlesAsync(updatedCandles);

            publisher.PublishEvent(new HistoricalCandlesUpdatedEvent() { TaskId = command.TaskId, });
        }
    }
}
