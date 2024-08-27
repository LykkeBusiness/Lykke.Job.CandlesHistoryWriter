using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.RFactor
{
    public class UpdateBrokenMonthlyCandlesCommand : UpdateCandlesCommand
    {
        public UpdateBrokenMonthlyCandlesCommand(decimal rFactor, DateTime rFactorDate)
            : base(rFactor, rFactorDate)
        {
        }
    }
}