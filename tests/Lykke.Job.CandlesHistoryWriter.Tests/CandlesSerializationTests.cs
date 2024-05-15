// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Text.Json;
using AutoFixture;
using Lykke.Job.CandleHistoryWriter.Repositories.Candles;
using Lykke.Job.CandleHistoryWriter.Repositories.Snapshots;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lykke.Job.CandlesHistoryWriter.Tests
{
    [TestClass]
    public class CandlesSerializationTests
    {
        [TestMethod]
        public void Candles_Should_Be_Serializable_To_Json()
        {
            var fix = new Fixture();
            var snapshotCandleEntity1 = fix.Create<SnapshotCandleEntity>();
            var snapshotCandleEntity2 = fix.Create<SnapshotCandleEntity>();
          
            var candle = Candle.Create(
                snapshotCandleEntity1.AssetPairId,
                snapshotCandleEntity1.PriceType,
                CandleTimeInterval.Hour,
                snapshotCandleEntity1.Timestamp,
                (double) snapshotCandleEntity1.Open,
                (double) snapshotCandleEntity1.Close,
                (double) snapshotCandleEntity1.High,
                (double) snapshotCandleEntity1.Low,
                (double) snapshotCandleEntity1.TradingVolume,
                (double) snapshotCandleEntity1.TradingOppositeVolume,
                (double) snapshotCandleEntity1.LastTradePrice,
                snapshotCandleEntity1.LastUpdateTimestamp
            );
        
            var sqlCandleHistoryItem = fix.Create<SqlCandleHistoryItem>();
            var testCandle = fix.Create<TestCandle>();
           
            var candles = new List<ICandle>()
            {
                candle,
                snapshotCandleEntity1,
                snapshotCandleEntity2,
                sqlCandleHistoryItem,
                testCandle,
            };

            var json = JsonSerializer.Serialize(candles);
            var jsonArrayLength = JsonDocument.Parse(json).RootElement.GetArrayLength();
            Assert.AreEqual(candles.Count, jsonArrayLength);
        }
    }
}
