// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Cleanup
{
    /// <summary>
    /// Guards against multiple simultaneous runs of the cleanup procedure.
    /// </summary>
    public sealed class CandlesCleanupMultiRunGuard : ICandlesCleanup
    {
        private readonly ICandlesCleanup _decoratee;
        private readonly ILog _log;
        
        private static int _inProgress;

        public CandlesCleanupMultiRunGuard(ICandlesCleanup decoratee, ILog log)
        {
            _decoratee = decoratee ?? throw new System.ArgumentNullException(nameof(decoratee));
            _log = log ?? throw new System.ArgumentNullException(nameof(log));
        }

        public async Task Invoke()
        {
            if (1 == Interlocked.Exchange(ref _inProgress, 1))
            {
                await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke),
                    "Cleanup is already in progress, skipping.");
                return;
            }
                        
            try
            {
                await _decoratee.Invoke();
            }
            finally
            {
                Interlocked.Exchange(ref _inProgress, 0);
            }
        }
    }
}
