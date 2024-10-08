﻿// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public class Candle : ICandle
    {
        public string AssetPairId { get; }
        public CandlePriceType PriceType { get; }
        public CandleTimeInterval TimeInterval { get; }
        public DateTime Timestamp { get; }
        public double Open { get; }
        public double Close { get; }
        public double High { get; }
        public double Low { get; }
        public double TradingVolume { get; }
        public double TradingOppositeVolume { get; }
        public double LastTradePrice { get; }
        public DateTime LastUpdateTimestamp { get; }
        
        public ICandle UpdateRFactor(double rFactor)
        {
            if (rFactor <= 0)
            {
                throw new ArgumentException("R-factor should be greater than 0.");
            }

            return new Candle(
                AssetPairId,
                PriceType,
                TimeInterval,
                Timestamp,
                Open * rFactor,
                Close * rFactor,
                High * rFactor,
                Low * rFactor,
                TradingVolume,
                TradingOppositeVolume,
                LastTradePrice,
                LastUpdateTimestamp);
        }
        
        public ICandle UpdateMonthlyOrWeeklyRFactor(double rFactor)
        {
            if (rFactor <= 0)
            {
                throw new ArgumentException("R-factor should be greater than 0.");
            }

            if (TimeInterval != CandleTimeInterval.Month && TimeInterval != CandleTimeInterval.Week)
            {
                throw new ArgumentException("Can only use this method for updating Monthly or Weekly candles");
            }

            var low = Low;
            var high = High;

            if (rFactor > 1)
            {
                low *= rFactor;
            }
            else
            {
                high *= rFactor;
            }

            return new Candle(
                AssetPairId,
                PriceType,
                TimeInterval,
                Timestamp,
                Open * rFactor,
                Close,
                high,
                low,
                TradingVolume,
                TradingOppositeVolume,
                LastTradePrice,
                LastUpdateTimestamp);
        }

        private Candle(
            string assetPair,
            CandlePriceType priceType,
            CandleTimeInterval timeInterval,
            DateTime timestamp,
            double open,
            double close,
            double high,
            double low,
            double tradingVolume,
            double tradingOppositeVolume,
            double lastTradePrice,
            DateTime lastUpdateTimestamp)
        {
            AssetPairId = assetPair;
            PriceType = priceType;
            TimeInterval = timeInterval;
            Timestamp = timestamp;
            Open = open;
            Close = close;
            High = high;
            Low = low;
            TradingVolume = tradingVolume;
            TradingOppositeVolume = tradingOppositeVolume;
            LastTradePrice = lastTradePrice;
            LastUpdateTimestamp = lastUpdateTimestamp;
        }

        public static Candle Create(
            string assetPair,
            CandlePriceType priceType,
            CandleTimeInterval timeInterval,
            DateTime timestamp,
            double open,
            double close,
            double high,
            double low,
            double tradingVolume,
            double tradingOppositeVolume,
            double lastTradePrice,
            DateTime lastUpdateTimestamp)
        {
            return new Candle(
                assetPair,
                priceType,
                timeInterval,
                timestamp.TruncateTo(timeInterval),
                open,
                close,
                high,
                low,
                tradingVolume,
                tradingOppositeVolume,
                lastTradePrice,
                lastUpdateTimestamp);
        }

        public Candle Update(
            double close,
            double low,
            double high,
            double tradingVolume,
            double tradingOppositeVolume,
            double lastTradePrice,
            DateTime updateTimestamp)
        {
            if (updateTimestamp > LastUpdateTimestamp)
            {
                return new Candle(
                    assetPair: AssetPairId,
                    priceType: PriceType,
                    timeInterval: TimeInterval,
                    timestamp: Timestamp,
                    open: Open,
                    close: close,
                    high: high,
                    low: low,
                    tradingVolume: tradingVolume,
                    tradingOppositeVolume: tradingOppositeVolume,
                    lastTradePrice: lastTradePrice,
                    lastUpdateTimestamp: updateTimestamp);
            }

            return this;
        }

        public static Candle Copy(ICandle candle)
        {
            return new Candle
            (
                assetPair: candle.AssetPairId,
                priceType: candle.PriceType,
                timeInterval: candle.TimeInterval,
                timestamp: candle.Timestamp,
                open: candle.Open,
                close: candle.Close,
                high: candle.High,
                low: candle.Low,
                tradingVolume: candle.TradingVolume,
                tradingOppositeVolume: candle.TradingOppositeVolume,
                lastTradePrice: candle.LastTradePrice,
                lastUpdateTimestamp: candle.LastUpdateTimestamp
            );
        }
    }
}
