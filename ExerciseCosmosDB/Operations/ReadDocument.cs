using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;

namespace MsLearnCosmosDB
{
    public class ReadDocument : Operation
    {
        public string GetOperationType()
        {
            return "Query";
        }

        public async Task Execute(Experiment experiment, DocumentClient client, DocumentCollection collection, Uri documentCollectionUri, int taskId, ConfigurationOptions options)
        {
            try
            {
                RequestOptions requestOptions = new RequestOptions();
                requestOptions.PartitionKey = new PartitionKey(options.PartitionKey);

                var response = await client.ReadDocumentAsync(options.DocumentLink, requestOptions);

                experiment.IncrementOperationCount();
                experiment.UpdateRequestUnits(taskId, response.RequestCharge);

                if (options.Record)
                {
                    Console.WriteLine("Order: {0}", response.Resource);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to read {0}. Exception was {1}", collection.SelfLink, e);
                if (e is DocumentClientException)
                {
                    DocumentClientException de = (DocumentClientException)e;
                    if (de.StatusCode == HttpStatusCode.Forbidden)
                    {
                        experiment.IncrementOperationCount();
                    }
                }
            }

        }
    }
}
