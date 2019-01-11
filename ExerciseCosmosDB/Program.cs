namespace MsLearnCosmosDB {
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using System.Threading;
    using System;
    using CommandLine;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;

    public class ConfigurationOptions {
        [Option ('d', "database", Required = false, HelpText = "The database to exercise")]
        public string Database { get; set; }

        [Option ('c', "collection", Required = false, HelpText = "The collection to exercise")]
        public string Collection { get; set; }

        [Option('t', "throughput", Required = false, HelpText = "Throughput of the collection (in RU/s)")]
        public int Throughput { get; set; }

        [Option ('o', "operation", Required = false, HelpText = "Operation to run for experiment. Name of Operation class to load.")]
        public string Operation { get; set; }

        [Option ('n', "number", Required = false, HelpText = "Number of operations to run")]
        public int NumberOfOperations { get; set; }

        [Option('p', "parallelism", Required = false, HelpText = "Number of threads to start")]
        public int Parallelism { get; set; }

        [Option ('q', "query", Required = false, HelpText = "The query string to run")]
        public string QueryString { get; set; }

        [Option ('k', "key", Required = false, HelpText = "The value of the partition key to access")]
        public string PartitionKey { get; set; }

        [Option ('l', "link", Required = false, HelpText = "The collection or document link to access")]
        public string DocumentLink { get; set; }

        [Option('v', "verbose", Required = false, HelpText = "Log configuration and results (with reduced performance)")]
        public bool Verbose { get; set; }

        [Option('r', "record", Required = false, HelpText = "Print out records as they are read and written")]
        public bool Record { get; set; }

        void Log(string message, params string[] args)
        {
            if (Verbose)
            {
                Console.WriteLine(message, args);
            }
        }
    }

    /// <summary>
    /// This sample repeatedly runs an operation on a CosmosDB collection to demonstrate high performance throughput.
    /// </summary>
    public sealed class Program {
        private static string InstanceId = Dns.GetHostEntry("LocalHost").HostName + Process.GetCurrentProcess().Id;

        private static ConfigurationOptions Options;

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp,
            RequestTimeout = new TimeSpan (1, 0, 0),
            MaxConnectionLimit = 1000,
            RetryOptions = new RetryOptions {
            MaxRetryAttemptsOnThrottledRequests = 10,
            MaxRetryWaitTimeInSeconds = 60
            }
        };

        private const int MinThreadPoolSize = 100;
        private DocumentClient client;

        /// <summary>
        /// Initialize a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private Program (DocumentClient client) {
            this.client = client;
        }

        /// <summary>
        /// Main method for the sample.
        /// </summary>
        /// <param name="args">command line arguments.</param>
        public static void Main (string[] args) {

            ThreadPool.SetMinThreads (MinThreadPoolSize, MinThreadPoolSize);

            ProcessArgs (args);
        }

        /// <summary>
        /// Processes arguments from config file and command line.
        /// Where both are set, command line takes precedence.
        /// </summary>
        /// <param name="args">String array of arguments passed in via the command line</param>
        private static void ProcessArgs (string[] args) {
            CommandLine.Parser.Default.ParseArguments<ConfigurationOptions> (args)
                .WithParsed<ConfigurationOptions> (opts => HandleConfigFileAndRun (opts))
                .WithNotParsed<ConfigurationOptions> ((errs) => HandleParseError (errs));
        }

        /// <summary>
        /// Handles settings from the config file and run.
        /// </summary>
        private static void HandleConfigFileAndRun (ConfigurationOptions opts) {
            string endpoint = Environment.GetEnvironmentVariable ("ENDPOINT");
            string authKey = Environment.GetEnvironmentVariable ("KEY");

            if ((endpoint == null) || (authKey == null)) {
                Console.WriteLine ("Error: ENDPOINT and KEY environment variables must be set");
                Environment.Exit (-1);
            }

            string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
            string DataCollectionName = ConfigurationManager.AppSettings["CollectionName"];
            int CollectionThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);
            string PartitionKey = ConfigurationManager.AppSettings["CollectionPartitionKey"];
            string Operation = ConfigurationManager.AppSettings["Operation"];
            int DegreeOfParallelism = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);
            string QueryString = ConfigurationManager.AppSettings["QueryString"];
            int NumberOfOperations = int.Parse (ConfigurationManager.AppSettings["NumberOfOperations"]);

            Options = opts;

            if (Options.Database == null) {
                Options.Database = DatabaseName;
            }

            if (Options.Collection == null) {
                Options.Collection = DataCollectionName;
            }

            if (Options.Throughput == 0)
            {
                Options.Throughput = CollectionThroughput;
            }

            if (Options.PartitionKey == null)
            {
                Options.PartitionKey = PartitionKey;
            }

            if (Options.Operation == null) {
                Options.Operation = Operation;
            }

            if (Options.Parallelism == 0)
            {
                Options.Parallelism = DegreeOfParallelism;
            }

            if (Options.NumberOfOperations == 0) {
                Options.NumberOfOperations = NumberOfOperations;
            }



            Run(endpoint, authKey);

        }

        /// <summary>
        /// Handle error in command line arguments
        /// </summary>
        /// <param name="errs">Errors in the command line arguments</param>
        private static void HandleParseError (IEnumerable<CommandLine.Error> errs) {
            Environment.Exit (-1);
        }

        /// <summary>
        /// Runs this program with its parsed command line arguments
        /// </summary>
        /// <param name="endpoint">Endpoint read from the environment</param>
        /// <param name="authKey">Auth key read from the environment</param>
        private static void Run (string endpoint, string authKey) {

            try {
                using (var client = new DocumentClient (
                    new Uri (endpoint),
                    authKey,
                    ConnectionPolicy)) {
                    var program = new Program (client);
                    program.RunAsync ().Wait ();
                }
            }

#if !DEBUG
            catch (Exception e) {
                // If the Exception is a DocumentClientException, the "StatusCode" value might help identity 
                // the source of the problem. 
                Console.WriteLine ("Experiment failed with exception:{0}", e);
            }
#endif
            finally {
                //Console.WriteLine ("Press any key to exit...");
                //Console.ReadLine ();
                Console.WriteLine("CosmosDB experiment complete");
            }

        }

        /// <summary>
        /// Run the main body of the program: create the database and collection, if they do not exist,
        /// then run the specified experiment,
        /// cleaning up afterwards if configured.
        /// </summary>
        /// <returns>The task</returns>
        private async Task RunAsync () {

            Database database = GetDatabaseIfExists(Options.Database);

            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]) && database != null)
            {
                Console.WriteLine("Deleting database {0}", Options.Database);
                await client.DeleteDatabaseAsync(database.SelfLink);
            }

            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]) || database == null)
            {
                Console.WriteLine("Creating database {0}", Options.Database);
                database = await client.CreateDatabaseAsync(new Database { Id = Options.Database });
            }

            DocumentCollection dataCollection = GetCollectionIfExists(Options.Database, Options.Collection);

            if (dataCollection == null)
            {
                Console.WriteLine("Creating collection {0} with {1} RU/s", Options.Collection, Options.Throughput);
                dataCollection = await this.CreatePartitionedCollectionAsync(Options.Database, Options.Collection, Options.Throughput, Options.PartitionKey);
            }

            long currentCollectionThroughput = 0;
            currentCollectionThroughput = Options.Throughput;

            OfferV2 offer = (OfferV2) client.CreateOfferQuery ().Where (o => o.ResourceLink == dataCollection.SelfLink).AsEnumerable ().FirstOrDefault ();
            currentCollectionThroughput = offer.Content.OfferThroughput;

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri (Options.Database, Options.Collection);

            if (Options.Verbose)
            {

                Console.WriteLine("Summary:");
                Console.WriteLine("--------------------------------------------------------------------- ");
                Console.WriteLine("Endpoint: {0}", client.ServiceEndpoint);
                Console.WriteLine("Collection : {0}.{1} at {2} RU/s with partition key {3}", Options.Database, Options.Collection, currentCollectionThroughput, Options.PartitionKey);
                Console.WriteLine("Operations : {0} {1}", Options.NumberOfOperations, Options.Operation);
                Console.WriteLine("Degree of parallelism*: {0}", Options.Parallelism);
                Console.WriteLine("--------------------------------------------------------------------- ");
                Console.WriteLine();
            }

            var experiment = new Experiment (client, dataCollection, collectionUri, Options);
            await experiment.RunAsync ();

            if (bool.Parse (ConfigurationManager.AppSettings["ShouldCleanupOnFinish"])) {
                Console.WriteLine ("Deleting Database {0}", Options.Database);
                await client.DeleteDatabaseAsync (UriFactory.CreateDatabaseUri (Options.Database));
            }
        }

        /// <summary>
        /// Get the collection if it exists, null if it doesn't. Assumes that the database does exist.
        /// </summary>
        /// <returns>The requested collection</returns>
        private DocumentCollection GetCollectionIfExists (string databaseName, string collectionName) {

            if (Options.Verbose)
            {
                Console.WriteLine("Checking to see if collection: {0} exists", collectionName);
            }

            return client.CreateDocumentCollectionQuery (UriFactory.CreateDatabaseUri (databaseName))
                .Where (c => c.Id == collectionName).AsEnumerable ().FirstOrDefault ();
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested database</returns>
        private Database GetDatabaseIfExists (string databaseName) {
            if (Options.Verbose)
            {
                Console.WriteLine("Checking to see if database: {0} exists", databaseName);
            }
            var result = client.CreateDatabaseQuery ().Where (d => d.Id == databaseName).AsEnumerable ();
            return result.FirstOrDefault ();
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        private async Task<DocumentCollection> CreatePartitionedCollectionAsync (string databaseName, string collectionName, int throughput, string partitionKey) {
            DocumentCollection collection = new DocumentCollection ();

            collection.Id = collectionName;
            collection.PartitionKey.Paths.Add (partitionKey);

            // Show user cost of running this test
            double estimatedCostPerMonth = 0.06 * throughput;
            double estimatedCostPerHour = estimatedCostPerMonth / (24 * 30);

            if (Options.Verbose)
            {
                Console.WriteLine("The collection will cost an estimated ${0} per hour (${1} per month)", Math.Round(estimatedCostPerHour, 2), Math.Round(estimatedCostPerMonth, 2));
            }

            return await client.CreateDocumentCollectionAsync (
                UriFactory.CreateDatabaseUri (databaseName),
                collection,
                new RequestOptions { OfferThroughput = throughput });
        }

    }
}