// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Cleanup
{
    /// <summary>
    /// Logs the time of the cleanup.
    /// </summary>
    public sealed class CandlesCleanupTimer : ICandlesCleanup
    {
        private readonly ICandlesCleanup _decoratee;
        private readonly ILog _log;

        public CandlesCleanupTimer(ICandlesCleanup decoratee, ILog log)
        {
            _decoratee = decoratee ?? throw new ArgumentNullException(nameof(decoratee));
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public async Task Invoke()
        {
            await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke), "Starting candles cleanup.");
            
            var sw = new Stopwatch();
            sw.Start();

            await _decoratee.Invoke();

            await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke),
                $"Candles cleanup finished in {sw.Elapsed:G}.");
        }
    }
}
