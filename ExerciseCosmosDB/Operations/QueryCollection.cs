using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;

namespace MsLearnCosmosDB
{
    public class QueryCollection : Operation
    {
        public string GetOperationType()
        {
            return "Query";
        }

        public async Task Execute(Experiment experiment, DocumentClient client, DocumentCollection collection, Uri documentCollectionUri, int taskId, ConfigurationOptions options)
        {
            try
            {
                IDocumentQuery<dynamic> query = client.CreateDocumentQuery(documentCollectionUri, options.QueryString,
                    new FeedOptions
                    {
                        PopulateQueryMetrics = true,
                        MaxItemCount = -1,
                        MaxDegreeOfParallelism = -1,
                        EnableCrossPartitionQuery = true,
                        EnableScanInQuery = true
                    }).AsDocumentQuery();

                while (query.HasMoreResults)
                {
                    FeedResponse<dynamic> response = await query.ExecuteNextAsync<dynamic>();

                    experiment.UpdateRequestUnits(taskId, response.RequestCharge);

                    if (options.Record)
                    {
                        var enumerator = response.GetEnumerator();
                        while (enumerator.MoveNext())
                        {
                            var current = enumerator.Current;
                            Console.WriteLine("Order {0}", current.ToString());
                        }
                    }
                }
                experiment.IncrementOperationCount();
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
