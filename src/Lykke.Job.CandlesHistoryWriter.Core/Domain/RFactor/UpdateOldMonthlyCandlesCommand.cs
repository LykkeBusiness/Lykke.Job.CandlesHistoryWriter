using System;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.RFactor
{
    public class UpdateOldMonthlyCandlesCommand : UpdateCandlesCommand
    {
        public DateTime CutoffDate { get; }
        public UpdateOldMonthlyCandlesCommand(decimal rFactor, DateTime rFactorDate)
            : base(rFactor, rFactorDate)
        {
            CutoffDate = rFactorDate.StartOfMonth();
        }
    }
}
