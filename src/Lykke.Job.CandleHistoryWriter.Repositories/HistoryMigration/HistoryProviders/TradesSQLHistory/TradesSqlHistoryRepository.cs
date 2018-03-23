﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;

namespace Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public class TradesSqlHistoryRepository : ITradesSqlHistoryRepository, IDisposable
    {
        private readonly int _sqlQueryBatchSize;
        private readonly ILog _log;

        private readonly SqlConnection _sqlConnection;

        private int StartingRowOffset { get; set; }
        private string AssetPairId { get; }

        public TradesSqlHistoryRepository(
            string sqlConnString,
            int sqlQueryBatchSize,
            ILog log,
            int startingRowOffset,
            string assetPairId
            )
        {
            _sqlQueryBatchSize = sqlQueryBatchSize;
            _log = log;

            StartingRowOffset = startingRowOffset;
            AssetPairId = assetPairId;

            _sqlConnection = new SqlConnection(sqlConnString);
            _sqlConnection.Open();
        }
        
        public async Task<IEnumerable<TradeHistoryItem>> GetNextBatchAsync()
        {
            var result = new List<TradeHistoryItem>();

            try
            {
                if (_sqlConnection == null || _sqlConnection.State != ConnectionState.Open)
                    throw new InvalidOperationException("Can't fetch from DB while connection is not opened. You should call InitAsync first.");

                await _log.WriteInfoAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync),
                    $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}",
                    $"Trying to fetch next {_sqlQueryBatchSize} rows...");
                
                using (var sqlCommand = new SqlCommand(BuildCurrentQueryCommand(), _sqlConnection))
                {
                    using (var reader = await sqlCommand.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            result.Add(new TradeHistoryItem
                            {
                                Id = reader.GetInt64(0),
                                Volume = reader.GetDecimal(1),
                                Price = reader.GetDecimal(2),
                                DateTime = reader.GetDateTime(3),
                                OppositeVolume = reader.GetDecimal(4)
                            });
                        }
                    }
                }

                await _log.WriteInfoAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync),
                    $"Starting offset = {StartingRowOffset}, asset pair ID = {AssetPairId}",
                    $"Fetched {_sqlQueryBatchSize} rows successfully.");

                StartingRowOffset += result.Count;
            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync(nameof(TradesSqlHistoryRepository), nameof(GetNextBatchAsync), ex);
            }

            return result;
        }

        public void Dispose()
        {
            _sqlConnection?.Close();
        }

        private string BuildCurrentQueryCommand()
        {
            var commandBld = new StringBuilder();

            // TODO: implement a query with pagination.
            //commandBld.Append($@"SELECT TOP {_sqlQueryBatchSize} Volume, Price, ""DateTime"", OppositeVolume ");
            commandBld.Append($@"SELECT Id, Volume, Price, ""DateTime"", OppositeVolume ");
            commandBld.Append("FROM Trades ");
            commandBld.Append("WHERE ");
            commandBld.Append($@"OrderType = 'Limit' AND Asset + OppositeAsset = '{AssetPairId}' AND Direction IN ('Buy', 'Sell') ");
            commandBld.Append(@"ORDER BY ""DateTime"", Id ASC ");
            commandBld.Append($"OFFSET {StartingRowOffset} ROWS FETCH NEXT {_sqlQueryBatchSize} ROWS ONLY;");

            return commandBld.ToString();
        }
    }
}