using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace evh_test_partition;

public static class TwoPartitionTest
{
    [FunctionName("TwoPartitionTest")]
    public static async Task RunAsync([EventHubTrigger("p2", Connection = "evh")] EventData[] events,
        ILogger log)
    {
        var exceptions = new List<Exception>();

        foreach (EventData eventData in events)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                log.LogInformation($"data: {eventData.EventBody}");
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }
        }

        if (exceptions.Count > 1)
            throw new AggregateException(exceptions);

        if (exceptions.Count == 1)
            throw exceptions.Single();
    }
}