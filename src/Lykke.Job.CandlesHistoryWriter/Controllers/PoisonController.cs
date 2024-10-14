using System.Threading.Tasks;
using Lykke.Common.Api.Contract.Responses;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Services;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.AspNetCore.Mvc;

namespace Lykke.Job.CandlesHistoryWriter.Controllers;

[Route("api/[controller]")]
public class PoisonController : Controller
{
    private readonly IRabbitPoisonHandlingService<CandlesUpdatedEvent> _rabbitPoisonHandingService;

    public PoisonController(IRabbitPoisonHandlingService<CandlesUpdatedEvent> rabbitPoisonHandingService)
    {
        _rabbitPoisonHandingService = rabbitPoisonHandingService;
    }

    [HttpPost("put-messages-back")]
    public async Task<IActionResult> PutMessagesBack()
    {
        try
        {
            return Ok(await _rabbitPoisonHandingService.PutMessagesBack());
        }
        catch (ProcessAlreadyStartedException ex)
        {
            return Conflict(ErrorResponse.Create(ex.Message));
        }
    }
}
