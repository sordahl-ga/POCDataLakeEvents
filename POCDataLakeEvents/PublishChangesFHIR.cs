using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Documents;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace POCDataLakeEvents
{
    public static class PublishChangesFHIR
    {
      
        [FunctionName("PublishChangesFHIR")]
        public static void Run([CosmosDBTrigger(
            databaseName: "health",
            collectionName: "fhir",
            ConnectionStringSetting = "CosmosDBConnectionFHIR",
            CreateLeaseCollectionIfNotExists = true,
            MaxItemsPerInvocation = 1000,
            StartFromBeginning = false,
            LeaseCollectionPrefix = "fhirpub",
            LeaseCollectionName = "leases")]IReadOnlyList<Document> input,
          [EventHub("fhirevents", Connection = "EventHubConnectionString")] ICollector<EventData> outputMessages,
          ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                log.LogInformation($"Publishing {input.Count} FHIR resources to fhirevents.");
                foreach (Document d in input)
                {
                    string json = d.ToString();
                    outputMessages.Add(new EventData(Encoding.UTF8.GetBytes(json)));
                }
            }
        }
    }
}
