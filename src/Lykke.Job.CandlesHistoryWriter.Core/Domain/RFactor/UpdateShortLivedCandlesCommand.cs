// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.RFactor
{
    public class UpdateShortLivedCandlesCommand : UpdateCandlesCommand
    {
        public decimal RFactor { get; }
        public DateTime RFactorDate { get; }

        public UpdateShortLivedCandlesCommand(decimal rFactor, DateTime rFactorDate)
            : base(rFactor, rFactorDate)
        {
        }
    }
}
