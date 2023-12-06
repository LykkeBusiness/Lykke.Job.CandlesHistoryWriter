// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesHistoryWriter.Core.Settings;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Cleanup
{
    /// <summary>
    /// Checks if cleanup is enabled in settings and skips the cleanup if it's not.
    /// </summary>
    public sealed class CandlesCleanupEnabledGuard : ICandlesCleanup
    {
        private readonly ICandlesCleanup _decoratee;
        private readonly CleanupSettings _cleanupSettings;
        private readonly ILog _log;
        public CandlesCleanupEnabledGuard(ICandlesCleanup decoratee, CleanupSettings cleanupSettings, ILog log)
        {
            _decoratee = decoratee ?? throw new System.ArgumentNullException(nameof(decoratee));
            _cleanupSettings = cleanupSettings ?? throw new System.ArgumentNullException(nameof(cleanupSettings));
            _log = log ?? throw new System.ArgumentNullException(nameof(log));
        }
        
        public async Task Invoke()
        {
            if (!_cleanupSettings.Enabled)
            {
                await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke),
                    "Cleanup is disabled in settings, skipping.");
                return;
            }
            
            await _decoratee.Invoke();
        }
    }
}
