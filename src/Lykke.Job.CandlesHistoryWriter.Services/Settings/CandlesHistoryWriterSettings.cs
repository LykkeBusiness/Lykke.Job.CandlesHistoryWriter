﻿// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Settings;
using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    [UsedImplicitly]
    public class CandlesHistoryWriterSettings
    {
        public AssetsCacheSettings AssetsCache { get; set; }

        public RabbitSettings Rabbit { get; set; }

        public QueueMonitorSettings QueueMonitor { get; set; }

        public PersistenceSettings Persistence { get; set; }

        public DbSettings Db { get; set; }

        [Optional, CanBeNull]
        public MigrationSettings Migration { get; set; }

        public ErrorManagementSettings ErrorManagement { get; set; }

        [Optional, CanBeNull]
        public ResourceMonitorSettings ResourceMonitor { get; set; }

        public int HistoryTicksCacheSize { get; set; }

        public TimeSpan CacheCleanupPeriod { get; set; }

        /// <summary>
        /// The size of the asset pairs batch when caching candles
        /// </summary>
        [Optional]
        public int? CacheCandlesAssetsBatchSize { get; set; }

        /// <summary>
        /// The number of retries if caching failed
        /// </summary>
        [Optional]
        public int? CacheCandlesAssetsRetryCount { get; set; }

        [Optional]
        public bool UseSerilog { get; set; }

        public CqrsSettings Cqrs { get; set; }

        [Optional]
        public CleanupSettings CleanupSettings { get; set; } = new CleanupSettings();
    }
}
