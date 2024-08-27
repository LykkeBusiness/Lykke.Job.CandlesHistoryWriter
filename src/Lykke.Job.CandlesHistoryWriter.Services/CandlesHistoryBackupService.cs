// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading.Tasks;
using Lykke.Job.CandleHistoryWriter.Repositories.Candles;
using Lykke.SettingsReader;
using Microsoft.Extensions.Logging;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    public class CandlesHistoryBackupService : ICandlesHistoryBackupService
    {
        private readonly IReloadingManager<string> _assetConnectionString;
        private readonly ILoggerFactory _loggerFactory;

        public CandlesHistoryBackupService(
            IReloadingManager<string> assetConnectionString,
            ILoggerFactory loggerFactory)
        {
            _assetConnectionString = assetConnectionString;
            _loggerFactory = loggerFactory;
        }

        public async Task Backup(string productId)
        {
            var repo = GetBackupRepo(productId);

            var (sourceTableName, backupTableName) = await repo.CreateTable();
            await repo.CopyData(sourceTableName, backupTableName);
        }

        private BackupSqlAssetPairCandlesHistoryRepository GetBackupRepo(string productId)
        {
            return new BackupSqlAssetPairCandlesHistoryRepository(productId, _assetConnectionString.CurrentValue,
                _loggerFactory.CreateLogger<BackupSqlAssetPairCandlesHistoryRepository>());
        }
    }
}
