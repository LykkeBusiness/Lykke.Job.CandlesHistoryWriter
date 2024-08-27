using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.RFactor
{
    public class UpdateBrokenWeeklyCandlesCommand : UpdateCandlesCommand
    {
        public UpdateBrokenWeeklyCandlesCommand(decimal rFactor, DateTime rFactorDate)
            : base(rFactor, rFactorDate)
        {
        }
    }
}