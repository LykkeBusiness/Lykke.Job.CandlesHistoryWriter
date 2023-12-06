// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Cleanup
{
    /// <summary>
    /// Logs exceptions thrown by the candles cleanup.
    /// </summary>
    public sealed class CandlesCleanupFailureLogger : ICandlesCleanup
    {
        private readonly ICandlesCleanup _decoratee;
        private readonly ILog _log;

        public CandlesCleanupFailureLogger(ICandlesCleanup decoratee, ILog log)
        {
            _decoratee = decoratee ?? throw new ArgumentNullException(nameof(decoratee));
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public async Task Invoke()
        {
            try
            {
                await _decoratee.Invoke();
            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync(nameof(SqlCandlesCleanup), nameof(Invoke), null, ex);
                throw;
            }
        }
    }
}
