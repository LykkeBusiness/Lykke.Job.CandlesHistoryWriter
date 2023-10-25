// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Common.Log;
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
        private readonly ILog _log;
        private static int _inProgress;
        
        private const int DefaultTimeoutSeconds = 1 * 60 * 60;

        public SqlCandlesCleanup(CleanupSettings cleanupSettings, string connectionString, ILog log)
        {
            _cleanupSettings = cleanupSettings ?? throw new ArgumentNullException(nameof(cleanupSettings));
            _connectionString = string.IsNullOrWhiteSpace(connectionString)
                ? throw new ArgumentNullException(nameof(connectionString))
                : connectionString;
            _log = log ?? throw new ArgumentNullException(nameof(log));
            
            _effectiveTimeout = _cleanupSettings.Timeout ?? TimeSpan.FromSeconds(DefaultTimeoutSeconds);
        }

        public async Task Invoke()
        {
            if (!_cleanupSettings.Enabled)
            {
                await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke),
                    "Cleanup is disabled in settings, skipping.");
                return;
            }

            await using var conn = new SqlConnection(_connectionString);
            
            if (1 == Interlocked.Exchange(ref _inProgress, 1))
            {
                await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke),
                    "Cleanup is already in progress, skipping.");
                return;
            }
                
            try
            {
                var procedureBody = "01_Candles.Cleanup.sql".GetFileContent();
                await conn.ExecuteAsync(string.Format(procedureBody, _cleanupSettings.GetFormatParams()));

                await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke), "Starting candles cleanup.");
                var sw = new Stopwatch();
                sw.Start();

                await conn.ExecuteAsync("EXEC Candles.Cleanup", 
                    commandTimeout: (int)_effectiveTimeout.TotalSeconds);

                await _log.WriteInfoAsync(nameof(ICandlesCleanup), nameof(Invoke),
                    $"Candles cleanup finished in {sw.Elapsed:G}.");

            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync(nameof(SqlCandlesCleanup), nameof(Invoke), null, ex);
                throw;
            }
            finally
            {
                Interlocked.Exchange(ref _inProgress, 0);
            }
        }
    }
}
