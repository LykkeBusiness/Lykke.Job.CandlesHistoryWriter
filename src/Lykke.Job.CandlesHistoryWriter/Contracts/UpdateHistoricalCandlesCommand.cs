// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using MessagePack;

// ReSharper disable once CheckNamespace
namespace CorporateActions.Broker.Contracts.Workflow
{
    [MessagePackObject]
    public class UpdateHistoricalCandlesCommand
    {
        [Key(0)] public string TaskId { get; set; }
        [Key(1)] public string ProductId { get; set; }
        [Key(2)] public DateTime RFactorDate { get; set; }

        [Key(3)] public decimal RFactor { get; set; }
    }
    
    [MessagePackObject]
    public class HistoricalCandlesUpdatedEvent
    {
        [Key(0)] public string TaskId { get; set; }
    }
}
