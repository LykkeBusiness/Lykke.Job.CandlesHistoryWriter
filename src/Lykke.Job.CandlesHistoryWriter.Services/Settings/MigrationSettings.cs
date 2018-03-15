﻿using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class MigrationSettings
    {
        public QuotesSettings Quotes { get; set; }
        public TradesSettings Trades { get; set; }
    }

    public class QuotesSettings
    {
        public int CandlesToDispatchLengthThrottlingThreshold { get; set; }
        public TimeSpan ThrottlingDelay { get; set; }
    }

    public class TradesSettings
    {
        public string SQLTradesDataSourceConnString { get; set; }
        public int SQLQueryBatchSize { get; set; }
    }
}
