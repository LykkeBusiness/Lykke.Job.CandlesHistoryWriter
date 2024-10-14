using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IRabbitPoisonHandlingService<T> where T : class
    {
        Task<string> PutMessagesBack();
    }
}
