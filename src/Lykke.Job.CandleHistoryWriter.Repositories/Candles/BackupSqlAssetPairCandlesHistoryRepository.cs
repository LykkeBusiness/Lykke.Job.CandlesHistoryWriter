// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Threading.Tasks;
using Dapper;
using Lykke.Logs.MsSql.Extensions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    public class BackupSqlAssetPairCandlesHistoryRepository
    {
        private readonly string _assetName;
        private readonly string _connectionString;
        private readonly ILogger<BackupSqlAssetPairCandlesHistoryRepository> _logger;

        private const string SchemaName = "Candles";

        private const string CreateTableScript = @"
            CREATE TABLE {0}( 
             [Id] [bigint] NOT NULL PRIMARY KEY, 
             [AssetPairId] [nvarchar] (64) NOT NULL,  
             [PriceType] [int] NOT NULL, 
             [Open] [float] NOT NULL,  
             [Close] [float] NOT NULL,  
             [High] [float] NOT NULL,  
             [Low] [float] NOT NULL,  
             [TimeInterval] [int] NOT NULL,  
             [TradingVolume] [float] NOT NULL,  
             [TradingOppositeVolume] [float] NOT NULL,  
             [LastTradePrice] [float] NOT NULL,  
             [Timestamp] [datetime] NULL,  
             [LastUpdateTimestamp] [datetime] NULL);";

        private const string CopyDataScript = @"
            INSERT INTO {0}
            SELECT * FROM {1}";

        public BackupSqlAssetPairCandlesHistoryRepository(string assetName,
            string connectionString,
            ILogger<BackupSqlAssetPairCandlesHistoryRepository> logger)
        {
            _assetName = assetName;
            _connectionString = connectionString;
            _logger = logger;
        }

        public async Task CopyData(string sourceTableName, string backupTableName)
        {
            await using var conn = new SqlConnection(_connectionString);

            var script = string.Format(CopyDataScript, backupTableName, sourceTableName);
            try
            {
                await conn.ExecuteAsync(script);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Could not copy data to backup table {TableName}", backupTableName);
                throw;
            }
        }

        public async Task<(string sourceTableName, string backupTableName)> CreateTable()
        {
            var fixedAssetName = _assetName.Replace("-", "_");
            var suffix = "_backup";

            var sourceTableName = $"candleshistory_{fixedAssetName}";
            var backupTableName = $"{sourceTableName}{suffix}";

            var fullSourceTableName = $"[{SchemaName}].[{sourceTableName}]";
            var fullBackupTableName = $"[{SchemaName}].[{sourceTableName}{suffix}]";

            await using var conn = new SqlConnection(_connectionString);
            try
            {
                conn.CreateTableIfDoesntExists(CreateTableScript, backupTableName, SchemaName);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Could not create backup table {TableName}", fullBackupTableName);
                throw;
            }

            return (fullSourceTableName, fullBackupTableName);
        }
    }
}
