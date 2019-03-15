using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Documents;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace POCDataLakeEvents
{
    public static class PublishChangesDICOM
    {
        [FunctionName("PublishChangesDICOM")]
        public static void Run([CosmosDBTrigger(
            databaseName: "health",
            collectionName: "dicom",
            ConnectionStringSetting = "CosmosDBConnectionDICOM",
            CreateLeaseCollectionIfNotExists = true,
            MaxItemsPerInvocation = 1000,
            StartFromBeginning =true,
            LeaseCollectionPrefix = "dicompub",
            LeaseCollectionName = "leases")]IReadOnlyList<Document> input,
          [EventHub("dicomevents", Connection = "EventHubConnectionString")] ICollector<EventData> outputMessages,
          ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                log.LogInformation($"Publishing {input.Count} dicom metadata messages to dicomevents.");
                foreach (Document d in input)
                {
                    string json = d.ToString();
                    outputMessages.Add(new EventData(Encoding.UTF8.GetBytes(json)));
                }
            }
        }
    }
}
