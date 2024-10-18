using System.Net;
using Lykke.Common.Api.Contract.Responses;
using Lykke.RabbitMqBroker;
using Microsoft.AspNetCore.Mvc;

namespace Lykke.Job.CandlesHistoryWriter.Controllers;

[Route("api/[controller]")]
public class PoisonController : Controller
{
    [HttpPost("put-messages-back")]
    public IActionResult PutMessagesBack([FromServices] IPoisonQueueHandler poisonQueueHandler)
    {
        try
        {
            var text = poisonQueueHandler.TryPutMessagesBack();
            if (string.IsNullOrEmpty(text))
            {
                return NotFound(ErrorResponse.Create("There are no messages to put back"));
            }
            return Ok(text);
        }
        catch (ProcessAlreadyStartedException ex)
        {
            return Conflict(ErrorResponse.Create(ex.Message));
        }
        catch (LockAcqTimeoutException ex)
        {
            return StatusCode((int)HttpStatusCode.InternalServerError, ErrorResponse.Create(ex.Message));
        }
    }
}
