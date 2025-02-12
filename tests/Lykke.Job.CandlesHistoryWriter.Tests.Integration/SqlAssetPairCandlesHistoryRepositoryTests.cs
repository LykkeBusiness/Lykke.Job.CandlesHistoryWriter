﻿// Copyright (c) 2024 Lykke Corp.
// See the LICENSE file in the project root for more information.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Common;
using Common.Log;
using DotNet.Testcontainers.Builders;
using FluentAssertions;
using Lykke.Job.CandleHistoryWriter.Repositories.Candles;
using Lykke.Job.CandleHistoryWriter.Repositories.Snapshots;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.Data.SqlClient;
using Moq;
using Testcontainers.MsSql;
using Xunit;
using Xunit.Abstractions;

namespace Lykke.Job.CandlesHistoryWriter.Tests.Integration
{
    public class SqlAssetPairCandlesHistoryRepositoryTests : IAsyncLifetime
    {    
        private const string Database = "test_db";
        private const string AssetName = "USD";

        private SqlAssetPairCandlesHistoryRepository _repo;
        private readonly MsSqlContainer _msSqlContainer;
        private Mock<ILog> _log;
        private readonly ITestOutputHelper _testOutputHelper;

        public SqlAssetPairCandlesHistoryRepositoryTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            MsSqlBuilder builder = RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                ? new MsSqlBuilder()
                    .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
                : new MsSqlBuilder()
                    .WithImage("rapidfort/microsoft-sql-server-2019-ib")
                    .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("/opt/mssql-tools/bin/sqlcmd", "-C", "-Q", "SELECT 1;"));
            _msSqlContainer = builder
                .WithEnvironment("ACCEPT_EULA", "Y")
                .WithPortBinding(1433, true)
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
            var now = DateTime.UtcNow.RoundToSecond();

            ICandle snapshotCandleEntity = new SnapshotCandleEntity
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
            await _repo.InsertOrMergeAsync([
                snapshotCandleEntity
            ]);

            var candles = await _repo.GetCandlesAsync(CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddSeconds(-1), now.AddSeconds(1));

            var candle = candles.Single();
            candle.AssetPairId.Should().Be(AssetName);
            candle.PriceType.Should().Be(CandlePriceType.Ask);
            candle.TimeInterval.Should().Be(CandleTimeInterval.Minute);
            candle.Timestamp.Should().Be(now);
            candle.Open.Should().Be(0.5);
            candle.Close.Should().Be(0.7);
            candle.High.Should().Be(1);
            candle.Low.Should().Be(0.2);
            candle.TradingVolume.Should().Be(25);
            candle.LastUpdateTimestamp.Should().Be(now);
            candle.LastTradePrice.Should().Be(0.6);
            candle.TradingOppositeVolume.Should().Be(51);
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
                
            candles.Count().Should().Be(1);
            // exception is suppressed, so analyzing error logs
            _log.Verify(x => x.WriteErrorAsync(nameof(SqlAssetPairCandlesHistoryRepository), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Exception>(), null), Times.Never);
        }
             
        [Fact(Skip = "Performance test")]
        public async Task InsertOrMerge_PerformanceTest()
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

            var requests = new[] {10, 100, 1000, 5000, 6000, 10000};
            foreach (var r in requests)
            {
                try
                {
                    var tasks = new List<Func<Task>>();
                    for (var i = 0; i < r; i++)
                    {
                        tasks.Add(() => _repo.InsertOrMergeAsync([entity]));
                    }

                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    await Task.WhenAll(tasks.AsParallel().Select(async task => await task()));
                    watch.Stop();
                    var elapsedMs = watch.ElapsedMilliseconds;
                    _testOutputHelper.WriteLine($"Execution time of {r} is :{elapsedMs} ms");
                }
                catch (Exception e)
                {
                    _testOutputHelper.WriteLine($"Execution time of {r} is unknown: {e.Message}");
                }

                var candles = await _repo.GetCandlesAsync(CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddSeconds(-1), now.AddSeconds(1));
                
                candles.Count().Should().Be(1);
                // exception is suppressed, so analyzing error logs
                _log.Verify(x => x.WriteErrorAsync(nameof(SqlAssetPairCandlesHistoryRepository), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Exception>(), null), Times.Never);
            }
        }
    }
}
