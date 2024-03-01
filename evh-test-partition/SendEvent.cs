using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;

namespace evh_test_partition;

public class SendEvent
{
    [FunctionName("p1")]
    public async Task<IActionResult> Run1Async(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "p1/{n}")] HttpRequest req, 
        int n,
        ILogger log)
    {
        var eventHubClient = EventHubClient.CreateFromConnectionString(Environment.GetEnvironmentVariable("evh1"));
        for(var i = 0; i < n; i++)
        {
            await SendEventAsync(eventHubClient, $"p1 {i}");
        }
        await eventHubClient.CloseAsync();
        return new OkResult();
    }
    
    [FunctionName("p2")]
    public async Task<IActionResult> Run2Async(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "p2/{n}")] HttpRequest req, 
        int n,
        ILogger log)
    {
        var eventHubClient = EventHubClient.CreateFromConnectionString(Environment.GetEnvironmentVariable("evh2"));
        for(var i = 0; i < n; i++)
        {
            await SendEventAsync(eventHubClient, $"p2 {i}");
        }
        await eventHubClient.CloseAsync();
        return new OkResult();
    }
    
    [FunctionName("p4")]
    public async Task<IActionResult> Run4Async(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "p4/{n}")] HttpRequest req, 
        int n,
        ILogger log)
    {
        var eventHubClient = EventHubClient.CreateFromConnectionString(Environment.GetEnvironmentVariable("evh4"));
        for(var i = 0; i < n; i++)
        {
            await SendEventAsync(eventHubClient, $"p4 {i}");
        }
        await eventHubClient.CloseAsync();
        return new OkResult();
    }
    
    async Task SendEventAsync(EventHubClient eventHubClient, string message)
    {
        try
        {
            var eventData = new EventData(Encoding.UTF8.GetBytes(message));

            await eventHubClient.SendAsync(eventData);

            Console.WriteLine($"Sent event: {message}");
        }
        catch (Exception exception)
        {
            Console.WriteLine($"Error sending event: {exception.Message}");
        }
    }
}