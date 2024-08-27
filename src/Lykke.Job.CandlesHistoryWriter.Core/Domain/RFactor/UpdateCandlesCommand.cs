// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.RFactor
{
    public abstract class UpdateCandlesCommand
    {
        public decimal RFactor { get; }
        public DateTime RFactorDate { get; }

        protected UpdateCandlesCommand(decimal rFactor, DateTime rFactorDate)
        {
            RFactor = rFactor;
            RFactorDate = rFactorDate;
        }
    }
}
