using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

namespace MsLearnCosmosDB
{

    class InsertDocument : Operation
    {
        public string GetOperationType()
        {
            return "Write";
        }

        public async Task Execute(Experiment experiment, DocumentClient client, DocumentCollection collection, Uri documentCollectionUri, int taskId, ConfigurationOptions options)
        {
            var cancellationTokenService = new CancellationTokenSource();
            try
            {
                cancellationTokenService.CancelAfter(2500);

                Order order = Order.NewOrder();

                ResourceResponse<Document> response = await client.CreateDocumentAsync(documentCollectionUri, order, new RequestOptions() { }, false, cancellationTokenService.Token);

                if (options.Record)
                {
                    Console.WriteLine("Order: {0}", JsonConvert.SerializeObject(order));
                }
                experiment.IncrementOperationCount();
                experiment.UpdateRequestUnits(taskId, response.RequestCharge);
            }
            catch (Exception e)
            {
                Trace.TraceError("Failed to write. Exception was {0}", e);
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