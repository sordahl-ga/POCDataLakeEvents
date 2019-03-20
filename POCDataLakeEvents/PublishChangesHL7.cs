using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Documents;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace POCDataLakeEvents
{
    public static class PublishChangesHL7
    {
        [FunctionName("PublishChangesHL7")]
        public static void Run([CosmosDBTrigger(
            databaseName: "hl7json",
            collectionName: "messages",
            ConnectionStringSetting = "CosmosDBConnectionHL7",
            CreateLeaseCollectionIfNotExists = true,
            StartFromBeginning = false,
            MaxItemsPerInvocation =1000,
            LeaseCollectionPrefix = "hl7pub1",
            LeaseCollectionName = "leases")]IReadOnlyList<Document> input,
          [EventHub("hl7events", Connection = "EventHubConnectionString")] ICollector<EventData> outputMessages,
          ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                log.LogInformation($"Publishing {input.Count} hl7messages to hl7events.");
                foreach (Document d in input)
                {
                    string json = d.ToString();
                    outputMessages.Add(new EventData(Encoding.UTF8.GetBytes(json)));
                }
            }
        }
    }
}
