using System;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.RFactor
{
    public class UpdateOldWeeklyCandlesCommand : UpdateCandlesCommand
    {
        public DateTime CutoffDate { get; }
        public UpdateOldWeeklyCandlesCommand(decimal rFactor, DateTime rFactorDate)
            : base(rFactor, rFactorDate)
        {
            CutoffDate = rFactorDate.StartOfWeek(DayOfWeek.Monday);
        }
    }
}
