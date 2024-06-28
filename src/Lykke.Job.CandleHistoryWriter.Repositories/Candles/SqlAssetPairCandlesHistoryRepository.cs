// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Dapper;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.RFactor;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Logs.MsSql.Extensions;
using Microsoft.Extensions.Internal;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    public class SqlAssetPairCandlesHistoryRepository
    {
        private const int ReadCommandTimeout = 36000;
        private const int WriteCommandTimeout = 600;

        private const string CreateTableScript = @"
CREATE TABLE {0}( 
 [Id] [bigint] NOT NULL IDENTITY(1,1) PRIMARY KEY, 
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
 [LastUpdateTimestamp] [datetime] NULL 
 ,INDEX IX_{UNIQUEINDEX_PLACEHOLDER} UNIQUE NONCLUSTERED (Timestamp, PriceType, TimeInterval)
 ,INDEX IX_{TABLEINDEX_TIMEINTERVAL_PLACEHOLDER} NONCLUSTERED (TimeInterval));";

        private static Type DataType => typeof(ICandle);

        private static readonly string GetColumns =
            "[" + string.Join("],[", DataType.GetProperties().Select(x => x.Name)) + "]";

        private static readonly string GetFields = string.Join(",", DataType.GetProperties().Select(x => "@" + x.Name));

        private readonly string _tableName;
        private readonly string _connectionString;
        private readonly ILog _log;
        private readonly ISystemClock _systemClock;

        public SqlAssetPairCandlesHistoryRepository(string assetName, string connectionString, ILog log)
        {
            _systemClock = new SystemClock();
            _log = log;
            _connectionString = connectionString;
            const string schemaName = "Candles";
            var fixedAssetName = assetName.Replace("-", "_");
            var justTableName = $"candleshistory_{fixedAssetName}";
            _tableName = $"[{schemaName}].[{justTableName}]";
            var createTableScript = CreateTableScript
                .Replace("{UNIQUEINDEX_PLACEHOLDER}", fixedAssetName)
                .Replace("{TABLEINDEX_TIMEINTERVAL_PLACEHOLDER}", $"{fixedAssetName}_TimeInterval");

            using (var conn = new SqlConnection(_connectionString))
            {
                try
                {
                    conn.CreateTableIfDoesntExists(createTableScript, justTableName, schemaName);
                }
                catch (Exception ex)
                {
                    log?.WriteErrorAsync(nameof(SqlAssetPairCandlesHistoryRepository),
                        "CreateTableIfDoesntExists",
                        new { createTableScript, justTableName, schemaName }.ToJson(),
                        ex);
                    throw;
                }
            }
        }

        public async Task InsertOrMergeAsync(IEnumerable<ICandle> candles)
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                if (conn.State == ConnectionState.Closed)
                    await conn.OpenAsync();

                var transaction = conn.BeginTransaction();
                try
                {
                    var timestamp = _systemClock.UtcNow.UtcDateTime;
                    var sql = $"IF EXISTS (SELECT * FROM {_tableName}" +
                              $" WHERE PriceType=@PriceType AND TimeStamp=@TimeStamp AND TimeInterval=@TimeInterval)" +
                              $" BEGIN UPDATE {_tableName}  SET [Open]=@Open, [Close]=@Close, [High]=@High, [Low]=@Low, [TradingVolume]=@TradingVolume, [TradingOppositeVolume]=@TradingOppositeVolume, [LastTradePrice]=@LastTradePrice, [LastUpdateTimestamp]='{timestamp}'" +
                              $" WHERE  PriceType=@PriceType AND TimeStamp=@TimeStamp AND TimeInterval=@TimeInterval END" +
                              " ELSE " +
                              $" BEGIN INSERT INTO {_tableName} ({GetColumns}) values ({GetFields}) END";

                    await conn.ExecuteAsync(sql, candles, transaction, commandTimeout: WriteCommandTimeout);

                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    var errorMessage = $"Failed to insert or update a candle list with following assetPairIds: {string.Join(",",candles.Select(candle => candle.AssetPairId))}";
                    _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository), nameof(InsertOrMergeAsync), errorMessage, ex);
                    transaction.Rollback();
                }
            }
        }

        public async Task<IEnumerable<ICandle>> GetCandlesAsync(CandlePriceType priceType, CandleTimeInterval interval,
            DateTime from, DateTime to)
        {
            var whereClause =
                "WHERE PriceType=@priceTypeVar AND TimeInterval=@intervalVar AND Timestamp >= @fromVar AND Timestamp <= @toVar";

            using (var conn = new SqlConnection(_connectionString))
            {
                try
                {
                    var objects = await conn.QueryAsync<SqlCandleHistoryItem>(
                        $"SELECT * FROM {_tableName} {whereClause}",
                        new { priceTypeVar = priceType, intervalVar = interval, fromVar = from, toVar = to }, null,
                        commandTimeout: ReadCommandTimeout);
                    return objects;
                }

                catch (Exception ex)
                {
                    _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository),
                        nameof(GetCandlesAsync),
                        new
                        {
                            message = "Failed to get an candle list",
                            priceType,
                            interval,
                            to,
                            _tableName
                        }.ToJson(), ex);
                    return Enumerable.Empty<ICandle>();
                }
            }
        }

        public async Task<IEnumerable<ICandle>> GetCandlesAsync(DateTime from, DateTime to)
        {
            var whereClause =
                "WHERE Timestamp >= @fromVar AND Timestamp <= @toVar";

            using (var conn = new SqlConnection(_connectionString))
            {
                try
                {
                    var objects = await conn.QueryAsync<SqlCandleHistoryItem>(
                        $"SELECT * FROM {_tableName} {whereClause}",
                        new { fromVar = from, toVar = to }, null, commandTimeout: ReadCommandTimeout);
                    return objects;
                }

                catch (Exception ex)
                {
                    _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository),
                        nameof(GetCandlesAsync),
                        new { message = "Failed to get an candle list", from, to, _tableName }.ToJson(), ex);
                    return Enumerable.Empty<ICandle>();
                }
            }
        }

        public async Task<IEnumerable<ICandle>> GetLastCandlesAsync(CandlePriceType priceType,
            CandleTimeInterval interval, DateTime to, int number)
        {
            var whereClause =
                "WHERE PriceType=@priceTypeVar AND TimeInterval=@intervalVar AND Timestamp <= @toVar";

            using (var conn = new SqlConnection(_connectionString))
            {
                try
                {
                    var objects = await conn.QueryAsync<SqlCandleHistoryItem>(
                        $"SELECT TOP {number} * FROM {_tableName} {whereClause} ORDER BY Timestamp DESC",
                        new { priceTypeVar = priceType, intervalVar = interval, toVar = to }, null,
                        commandTimeout: ReadCommandTimeout);
                    return objects.OrderBy(x => x.Timestamp);
                }

                catch (Exception ex)
                {
                    _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository), nameof(GetLastCandlesAsync),
                        new
                        {
                            message = "Failed to get an candle list",
                            priceType,
                            interval,
                            to,
                            number,
                            _tableName
                        }.ToJson(), ex);
                    return Enumerable.Empty<ICandle>();
                }
            }
        }

        public async Task<ICandle> TryGetFirstCandleAsync(CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                var candle = await conn.QueryFirstOrDefaultAsync<SqlCandleHistoryItem>(
                    $"SELECT TOP(1) * FROM {_tableName} WHERE PriceType=@priceTypeVar AND TimeInterval=@intervalVar ",
                    new { priceTypeVar = priceType, intervalVar = timeInterval });
                return candle;
            }
        }

        public async Task<int> DeleteCandlesAsync(IReadOnlyList<ICandle> candlesToDelete, CandlePriceType priceType)
        {
            throw new NotImplementedException();
            //int count = 0;

            //using (var conn = new SqlConnection(_connectionString))
            //{
            //    if (conn.State == ConnectionState.Closed)
            //        await conn.OpenAsync();
            //    var transaction = conn.BeginTransaction();
            //    try
            //    {
            //        count += await conn.ExecuteAsync(
            //            $"DELETE {TableName} WHERE TimeInterval=@TimeInterval AND" +
            //            $" Timestamp=@Timestamp AND PriceType=@PriceType", candlesToDelete, transaction);

            //        transaction.Commit();
            //    }
            //    catch (Exception ex)
            //    {
            //        _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository), nameof(GetCandlesAsync),
            //            $"Failed to get an candle list", ex);
            //        transaction.Rollback();
            //    }


            //}

            //return count;
        }

        public async Task<int> ReplaceCandlesAsync(IEnumerable<ICandle> candlesToReplace, CandlePriceType priceType)
        {
            throw new NotImplementedException();
            //int count = 0;

            //using (var conn = new SqlConnection(_connectionString))
            //{
            //    if (conn.State == ConnectionState.Closed)
            //        await conn.OpenAsync();
            //    var transaction = conn.BeginTransaction();
            //    try
            //    {
            //        var timestamp = _systemClock.UtcNow.UtcDateTime;
            //        count += await conn.ExecuteAsync(
            //                $"UPDATE {TableName} SET  [Close]=@Close, [High]=@High, [LastTradePrice]=@LastTradePrice," +
            //                $" [TradingVolume] = @TradingVolume, [Low] = @Low, [Open] = @Open, [LastUpdateTimestamp] = '{timestamp}'" +
            //                $" WHERE TimeInterval = @TimeInterval AND PriceType=@PriceType AND Timestamp = @Timestamp", candlesToReplace, transaction);

            //        transaction.Commit();
            //    }
            //    catch (Exception ex)
            //    {
            //        _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository), nameof(GetCandlesAsync),
            //            $"Failed to get an candle list", ex);
            //        transaction.Rollback();
            //    }

            //}

            //return count;
        }


        public async Task ApplyRFactor(List<UpdateCandlesCommand> commands)
        {
            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            await using var tran = conn.BeginTransaction();
            try
            {
                foreach (var command in commands)
                {
                    switch (command)
                    {
                        case UpdateShortLivedCandlesCommand c:
                            await UpdateShortLivedCandles(c, conn, tran);
                            break;
                        case UpdateBrokenMonthlyCandlesCommand c:
                            await UpdateBrokenMonthlyCandles(c, conn, tran);
                            break;
                        case UpdateBrokenWeeklyCandlesCommand c:
                            await UpdateBrokenWeeklyCandles(c, conn, tran);
                            break;
                        case UpdateOldMonthlyCandlesCommand c:
                            await UpdateOldMonthlyCandles(c, conn, tran);
                            break;
                        case UpdateOldWeeklyCandlesCommand c:
                            await UpdateOldWeeklyCandles(c, conn, tran);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($"RFactor command {nameof(command)} not supported");
                    }
                }

                tran.Commit();
            }
            catch (Exception ex)
            {
                _log?.WriteErrorAsync(nameof(SqlCandlesHistoryRepository), nameof(ApplyRFactor),
                    new { message = "Failed to update candles", _tableName }.ToJson(), ex);

                tran.Rollback();
                throw;
            }
        }

        private readonly string updateOldCandlesSql = @"update {0}
        set High *= @rFactor, Low *= @rFactor, [Open] *= @rFactor, [Close] *= @rFactor
        where [Timestamp] < @cutoffDate and TimeInterval = @timeInterval";

        private readonly string updateBrokenCandlesSql = @"update {0}
set 
    High = (case 
            when @rFactor < 1 then High * @rFactor
            when CONVERT(date, LastUpdateTimestamp) = CONVERT(date, @rFactorDate) then High * @rFactor
            else High
            end),
    Low = (case 
            when @rFactor > 1 then Low * @rFactor
            when CONVERT(date, LastUpdateTimestamp) = CONVERT(date, @rFactorDate) then Low * @rFactor
            else Low
            end), 
    [Open] *= @rFactor, 
    [Close] = (case 
            when CONVERT(date, LastUpdateTimestamp) = CONVERT(date, @rFactorDate) then [Close] * @rFactor
            else [Close]
            end) 
where 1=1
        and LastUpdateTimestamp > @rFactorDate
        and CONVERT(date, [Timestamp]) <= CONVERT(date, @rFactorDate)
        and TimeInterval = @timeInterval";
        
        private async Task UpdateBrokenWeeklyCandles(UpdateBrokenWeeklyCandlesCommand command, SqlConnection conn, SqlTransaction tran)
        {
            var sql = string.Format(updateBrokenCandlesSql, _tableName);
            await conn.ExecuteAsync(sql, new
            {
                rFactor = command.RFactor,
                rFactorDate = command.RFactorDate,
                timeInterval = (int)CandleTimeInterval.Week, 
            }, tran);
        }
        
        private async Task UpdateBrokenMonthlyCandles(UpdateBrokenMonthlyCandlesCommand command, SqlConnection conn, SqlTransaction tran)
        {
            var sql = string.Format(updateBrokenCandlesSql, _tableName);
            await conn.ExecuteAsync(sql, new
            {
                rFactor = command.RFactor,
                rFactorDate = command.RFactorDate,
                timeInterval = (int)CandleTimeInterval.Month, 
            }, tran);
        }

        private async Task UpdateOldWeeklyCandles(UpdateOldWeeklyCandlesCommand command, SqlConnection conn, SqlTransaction tran)
        {
            var sql = string.Format(updateOldCandlesSql, _tableName);

            await conn.ExecuteAsync(sql, new { 
                rFactor = command.RFactor, 
                cutoffDate = command.CutoffDate,
                timeInterval = (int)CandleTimeInterval.Week,
            }, tran);
        }

        private async Task UpdateOldMonthlyCandles(UpdateOldMonthlyCandlesCommand command, SqlConnection conn, SqlTransaction tran)
        {
            var sql = string.Format(updateOldCandlesSql, _tableName);

            await conn.ExecuteAsync(sql, new { 
                rFactor = command.RFactor, 
                cutoffDate = command.CutoffDate,
                timeInterval = (int)CandleTimeInterval.Month,
            }, tran);
        }

        private async Task UpdateShortLivedCandles(UpdateShortLivedCandlesCommand command, SqlConnection conn, SqlTransaction tran)
        {
            var sql =
                @$"update {_tableName} 
                set High *= @rFactor, Low *= @rFactor, [Open] *= @rFactor, [Close] *= @rFactor
                where CONVERT(date, [Timestamp]) <= CONVERT(date, @rFactorDate) and TimeInterval < {(int)CandleTimeInterval.Week}";

            await conn.ExecuteAsync(sql, new { rFactor = command.RFactor, rFactorDate = command.RFactorDate }, tran);
        }
    }
}
