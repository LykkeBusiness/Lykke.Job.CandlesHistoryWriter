﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class TradesCandleBatch
    {
        public string AssetId { get; }
        
        public CandleTimeInterval TimeInterval { get; }

        public static CandlePriceType PriceType =>
            CandlePriceType.Trades;

        public DateTime MinTimeStamp { get; private set; }
        public DateTime MaxTimeStamp { get; private set; }

        public IDictionary<DateTime, ICandle> Candles { get; }

        private readonly string _assetToken;
        // TODO: Remove unused field
        private readonly string _reverseAssetToken;

        public TradesCandleBatch(string assetId, string assetToken, string reverseAssetToken,
            CandleTimeInterval interval, IReadOnlyCollection<TradeHistoryItem> trades)
        {
            AssetId = assetId;
            _assetToken = assetToken;
            _reverseAssetToken = reverseAssetToken;
            TimeInterval = interval;

            MinTimeStamp = DateTime.MaxValue;
            MaxTimeStamp = DateTime.MinValue;

            Candles = MakeFromTrades(trades);
        }

        public TradesCandleBatch(string assetId, CandleTimeInterval interval, TradesCandleBatch basis)
        {
            AssetId = assetId;
            // Here we do not set up asset tokens for they are not needed.
            TimeInterval = interval;

            MinTimeStamp = DateTime.MaxValue;
            MaxTimeStamp = DateTime.MinValue;

            Candles = new Dictionary<long, ICandle>();

            CandlesCount = DeriveFromSmallerIntervalAsync(basis);
        }

        private IDictionary<DateTime, ICandle> MakeFromTrades(IReadOnlyCollection<TradeHistoryItem> trades)
        {
            var candles = new Dictionary<DateTime, ICandle>();

            foreach (var trade in trades)
            {
                // If the trade is straight or reverse.
                var isStraight = trade.Volume * trade.Price == trade.OppositeVolume; // Decimals are safe for comparation with ==.
                var volumeMultiplier = 1.0M / Math.Max(trades.Count(t => t.TradeId == trade.TradeId), 1.0M);

                var truncatedDate = trade.DateTime.TruncateTo(TimeInterval);

                var tradeCandle = Candle.Create(
                    AssetId,
                    PriceType,
                    TimeInterval,
                    truncatedDate,
                    (double) trade.Price,
                    (double) trade.Price,
                    (double) trade.Price,
                    (double) trade.Price,
                    Convert.ToDouble((isStraight ? trade.Volume : trade.OppositeVolume) * volumeMultiplier),
                    Convert.ToDouble((isStraight ? trade.OppositeVolume : trade.Volume) * volumeMultiplier),
                    0, // Last Trade Price is enforced to be = 0
                    trade.DateTime
                );

                if (!candles.TryGetValue(truncatedDate, out var existingCandle))
                {
                    candles.Add(truncatedDate, tradeCandle);

                    if (truncatedDate < MinTimeStamp)
                        MinTimeStamp = truncatedDate;
                    if (truncatedDate > MaxTimeStamp)
                        MaxTimeStamp = truncatedDate;
                }
                else
                {
                    candles[truncatedDate] = existingCandle.ExtendBy(tradeCandle);
                }
            }

            return candles;
        }

        private int DeriveFromSmallerIntervalAsync(TradesCandleBatch basis)
        {
            if ((int)basis.TimeInterval >= (int)TimeInterval)
                throw new InvalidOperationException($"Can't derive candles for time interval {TimeInterval.ToString()} from candles of {basis.TimeInterval.ToString()}.");

            if (basis.AssetId != AssetId)
                throw new InvalidOperationException($"Can't derive candles for asset pair ID {AssetId} from candles of {basis.AssetId}");

            var candles = new Dictionary<DateTime, ICandle>();

            foreach (var candle in basis.Candles)
            {
                var truncatedDate = candle.Value.Timestamp.TruncateTo(TimeInterval);

                if (!candles.TryGetValue(truncatedDate, out var existingCandle))
                {
                    candles.Add(truncatedDate, candle.Value.RebaseToInterval(TimeInterval));

                    if (truncatedDate < MinTimeStamp)
                        MinTimeStamp = truncatedDate;
                    if (truncatedDate > MaxTimeStamp)
                        MaxTimeStamp = truncatedDate;
                }
                else
                {
                    candles[truncatedDate] = existingCandle.ExtendBy(candle.Value.RebaseToInterval(TimeInterval));
                }
            }

            return candles;
        }
    }
}
