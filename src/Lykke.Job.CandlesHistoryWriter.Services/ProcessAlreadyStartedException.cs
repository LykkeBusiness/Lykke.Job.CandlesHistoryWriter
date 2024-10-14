using System;

namespace Lykke.Job.CandlesHistoryWriter.Services;

public sealed class ProcessAlreadyStartedException : Exception
{
    public ProcessAlreadyStartedException(string message) : base(message)
    {
    }
}
