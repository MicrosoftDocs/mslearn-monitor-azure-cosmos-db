using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;

namespace MsLearnCosmosDB
{
    class ReadCollection: Operation
    {
        public string GetOperationType()
        {
            return "Read";
        }

        public async Task Execute(Experiment experiment, DocumentClient client, DocumentCollection collection, Uri documentCollectionUri, int taskId, ConfigurationOptions options) 
        {
            try
            {
                ResourceResponse<DocumentCollection> response = await client.ReadDocumentCollectionAsync(collection.SelfLink, new RequestOptions() {} );
                experiment.IncrementOperationCount();
                experiment.UpdateRequestUnits(taskId, response.RequestCharge);
            }
            catch (Exception e)
            {
                Trace.TraceError("Failed to read {0}. Exception was {1}", collection.SelfLink, e);
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