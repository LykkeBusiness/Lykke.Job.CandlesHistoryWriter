﻿// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class QueueMonitorSettings
    {
        public int BatchesToPersistQueueLengthWarning { get; set; }
        public int CandlesToDispatchQueueLengthWarning { get; set; }
        public TimeSpan ScanPeriod { get; set; }
    }
}
