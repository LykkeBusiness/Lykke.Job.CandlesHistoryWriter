// Copyright (c) 2024 Lykke Corp.
// See the LICENSE file in the project root for more information.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using DotNet.Testcontainers.Builders;
using Lykke.Job.CandleHistoryWriter.Repositories.Candles;
using Lykke.Job.CandleHistoryWriter.Repositories.Snapshots;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.Data.SqlClient;
using Moq;
using Shouldly;
using Testcontainers.MsSql;
using Xunit;

namespace Lykke.Job.CandlesHistoryWriter.Tests.Integration
{
    public class SqlAssetPairCandlesHistoryRepositoryTests : IAsyncLifetime
    {    
        private const string Database = "test_db";
        private const string AssetName = "USD";

        private SqlAssetPairCandlesHistoryRepository _repo;
        private readonly MsSqlContainer _msSqlContainer;
        private Mock<ILog> _log;

        public SqlAssetPairCandlesHistoryRepositoryTests()
        {
            _msSqlContainer = new MsSqlBuilder()
                .WithImage("rapidfort/microsoft-sql-server-2019-ib")
                .WithEnvironment("ACCEPT_EULA", "Y")
                .WithPortBinding(1433, true)
                .WithWaitStrategy(
                    Wait.ForUnixContainer()
                        .UntilCommandIsCompleted(
                            "/opt/mssql-tools/bin/sqlcmd",
                            "-C",
                            "-Q",
                            "SELECT 1;"
                        )
                )
                .Build();
        }
        
        public async Task InitializeAsync()
        {
            await _msSqlContainer.StartAsync();

            await using var con = new SqlConnection(_msSqlContainer.GetConnectionString());
            await con.OpenAsync();
            var command = con.CreateCommand();
            command.CommandText = "CREATE DATABASE " + Database;
            command.ExecuteNonQuery();
            await con.CloseAsync();

            _log = new Mock<ILog>();
            _repo = new SqlAssetPairCandlesHistoryRepository(AssetName, _msSqlContainer.GetConnectionString(), _log.Object);
        }

        public Task DisposeAsync()
        {
            return _msSqlContainer.DisposeAsync().AsTask();
        }
        
        [Fact]
        public async Task InsertOrMerge_Inserts_InEmptyBaseFine()
        {
            var now = DateTime.UtcNow;
            
            await _repo.InsertOrMergeAsync([
                new SnapshotCandleEntity
                {
                    AssetPairId = AssetName,
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Minute,
                    Timestamp = now,
                    Open = 0.5m,
                    Close = 0.7m,
                    High = 1m,
                    Low = 0.2m,
                    TradingVolume = 25m,
                    LastUpdateTimestamp = now,
                    LastTradePrice = 0.6m,
                    TradingOppositeVolume = 51m,
                }
            ]);

            var candles = await _repo.GetCandlesAsync(CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddSeconds(-1), now.AddSeconds(1));
                
            candles.Count().ShouldBe(1);
        }
        
        
        [Fact]
        public async Task InsertOrMerge_SameEntityTwiceInParallel_WorksFine()
        {
            var now = DateTime.UtcNow;

            var entity = new SnapshotCandleEntity
            {
                AssetPairId = AssetName,
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Minute,
                Timestamp = now,
                Open = 0.5m,
                Close = 0.7m,
                High = 1m,
                Low = 0.2m,
                TradingVolume = 25m,
                LastUpdateTimestamp = now,
                LastTradePrice = 0.6m,
                TradingOppositeVolume = 51m,
            };
            var tasks = new List<Func<Task>>() {() => _repo.InsertOrMergeAsync([entity]), () => _repo.InsertOrMergeAsync([entity])};
            await Task.WhenAll(tasks.AsParallel().Select(async task => await task()));

            var candles = await _repo.GetCandlesAsync(CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddSeconds(-1), now.AddSeconds(1));
                
            candles.Count().ShouldBe(1);
            // exception is suppressed, so analyzing error logs
            _log.Verify(x => x.WriteErrorAsync(nameof(SqlAssetPairCandlesHistoryRepository), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Exception>(), null), Times.Never);
        }
    }
}
