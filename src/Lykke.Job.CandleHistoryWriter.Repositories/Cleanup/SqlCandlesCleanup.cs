// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesHistoryWriter.Core.Settings;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Cleanup
{
    public class SqlCandlesCleanup : ICandlesCleanup
    {
        private readonly CleanupSettings _cleanupSettings;
        private readonly TimeSpan _effectiveTimeout;
        private readonly string _connectionString;
        
        private const int DefaultTimeoutSeconds = 1 * 60 * 60;
        private const string StoredProcFileName = "01_Candles.Cleanup.sql";
        private const string StoredProcName = "Candles.Cleanup";

        public SqlCandlesCleanup(CleanupSettings cleanupSettings, string connectionString)
        {
            _cleanupSettings = cleanupSettings ?? throw new ArgumentNullException(nameof(cleanupSettings));
            _connectionString = string.IsNullOrWhiteSpace(connectionString)
                ? throw new ArgumentNullException(nameof(connectionString))
                : connectionString;
            
            _effectiveTimeout = _cleanupSettings.Timeout ?? TimeSpan.FromSeconds(DefaultTimeoutSeconds);
        }

        public async Task Invoke()
        {
            await using var conn = new SqlConnection(_connectionString);
                
            await EnsureStoredProcedureExists(conn);

            await conn.ExecuteAsync($"EXEC {StoredProcName}", 
                commandTimeout: (int)_effectiveTimeout.TotalSeconds);
        }
        
        private Task EnsureStoredProcedureExists(SqlConnection conn)
        {
            var spBody = StoredProcFileName.GetFileContent();
            return conn.ExecuteAsync(string.Format(spBody, _cleanupSettings.GetFormatParams()));
        }
    }
}
