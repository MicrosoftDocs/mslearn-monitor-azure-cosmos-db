namespace MsLearnCosmosDB
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Reflection;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;

    public class Experiment
    {
        private readonly DocumentClient client;
        private readonly DocumentCollection collection;
        private readonly Uri DocumentCollectionUri;
        private readonly ConfigurationOptions Options;
        private int PendingTaskCount;
        private long OperationCount;
        private ConcurrentDictionary<int, double> requestUnitsConsumed = new ConcurrentDictionary<int, double>();

        /// <summary>
        /// Initializes a new instance of the <see cref="T:MsLearnCosmosDB.Experiment"/> class.
        /// </summary>
        /// <param name="client">Client.</param>
        /// <param name="collection">Collection.</param>
        /// <param name="documentCollectionUri">Document collection URI.</param>
        /// <param name="options">Configured options</param>
        public Experiment(DocumentClient client, DocumentCollection collection, Uri documentCollectionUri, ConfigurationOptions options)
        {
            this.client = client;
            this.collection = collection;
            this.DocumentCollectionUri = documentCollectionUri;
            this.Options = options;
            this.OperationCount = 0;
        }

        /// <summary>
        /// Run the experiment. Pre-allocates customers and items so that there are multiple
        /// orders made by the same customer, and the same item is ordered multiple times.
        /// This is currently a fixed ratio but could be extended to configurable ones.
        /// </summary>
        /// <returns>The experiment task</returns>
        public async Task RunAsync()
        {
            Assembly assembly = Assembly.GetExecutingAssembly();
            Type operationType = assembly.GetType("MsLearnCosmosDB." + Options.Operation);

            if (operationType == null)
            {
                Console.WriteLine("Could not find operation of type: {0}", Options.Operation);
                Environment.Exit(-1);
            }

            Operation operation = (Operation)Activator.CreateInstance(operationType);

            if (operation.GetOperationType() == "Write")
            {
                Console.WriteLine("Setting up experiment...");
                int numCustomers = Math.Max(1, (Options.NumberOfOperations * 4) / 10);
                CustomerDetails.Allocate(numCustomers);
                int numItems = Math.Max(1, (Options.NumberOfOperations * 2) / 10);
                OrderItem.Allocate(numItems);
            }

            int taskCount;

            if (Options.Parallelism == -1)
            {
                // set TaskCount = 3 for each 1k RUs, minimum 1, maximum 250
                taskCount = Math.Max(Options.Throughput / 333, 1);
                taskCount = Math.Min(taskCount, 250);
            }
            else
            {
                taskCount = Options.Parallelism;
            }

            Console.WriteLine("Starting experiment with {0} tasks @ {1}", taskCount, DateTime.Now);

            PendingTaskCount = taskCount;

            int numberOfOperationsPerTask = Options.NumberOfOperations / taskCount;
            int remainingOperations = Options.NumberOfOperations - (numberOfOperationsPerTask * taskCount);

            var tasks = new List<Task>();
            tasks.Add(LogOutputStats(operation.GetOperationType()));

            for (var i = 0; i < taskCount; i++)
            {
                int numberOfOperations = numberOfOperationsPerTask;
                if (i == taskCount - 1)
                {
                    numberOfOperations += remainingOperations;
                }
                tasks.Add(RunTask(operation, i, numberOfOperations));
            }

            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <returns>The task.</returns>
        /// <param name="operation">Operation.</param>
        /// <param name="taskId">Task identifier.</param>
        /// <param name="numberOfOperations">Number of operations.</param>
        private async Task RunTask(Operation operation, int taskId, int numberOfOperations)
        {        
            requestUnitsConsumed[taskId] = 0;

            for (var i = 0; i < numberOfOperations; i++)
            {
                await operation.Execute(this, client, collection, DocumentCollectionUri, taskId, Options);
            }

            DecrementPendingTaskCount();
        }

        /// <summary>
        /// Logs the output stats.
        /// </summary>
        /// <returns>The output stats.</returns>
        /// <param name="type">Type.</param>
        private async Task LogOutputStats(string type)
        {
            long lastCount = 0;
            double lastRequestUnits = 0;
            double lastSeconds = 0;
            double requestUnits = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            while (PendingTaskCount > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                requestUnits = 0;
                foreach (int taskId in requestUnitsConsumed.Keys)
                {
                    requestUnits += requestUnitsConsumed[taskId];
                }

                long currentCount = OperationCount;

                LogOutput(type, currentCount, OperationCount, seconds, requestUnits);

                lastCount = OperationCount;
                lastSeconds = seconds;
                lastRequestUnits = requestUnits;
            }

            double totalSeconds = watch.Elapsed.TotalSeconds;

            Console.WriteLine();
            Console.WriteLine("----------------------------------------------------------------- ");
            LogOutput(type, lastCount, OperationCount, watch.Elapsed.TotalSeconds, requestUnits);
            Console.WriteLine("Total (consumed {0} RUs in {1} seconds)", Math.Round(requestUnits, 1), Math.Round(watch.Elapsed.TotalSeconds));
            Console.WriteLine("------------------------------------------------------------------");
        }

        /// <summary>
        /// Logs the output.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="count">Count.</param>
        /// <param name="operations">Operations.</param>
        /// <param name="seconds">Seconds.</param>
        /// <param name="requestUnits">Request units.</param>
        private void LogOutput(string type, long count, long operations, double seconds, double requestUnits)
        {
            double ruPerSecond = (requestUnits / seconds);

            Console.WriteLine("Performed {0} {1} operations @ {2} operations/s, {3} RU/s)",
                count,
                type,
                Math.Round(operations / seconds),
                Math.Round(ruPerSecond, 1));
        }


        /// <summary>
        /// Increments the operation count.
        /// </summary>
        public void IncrementOperationCount()
        {
            Interlocked.Increment(ref OperationCount);
        }

        /// <summary>
        /// Decrements the pending task count.
        /// </summary>
        public void DecrementPendingTaskCount()
        {
            Interlocked.Decrement(ref PendingTaskCount);
        }

        /// <summary>
        /// Updates the request units.
        /// </summary>
        /// <param name="taskId">Task identifier.</param>
        /// <param name="requestUnits">Request units.</param>
        public void UpdateRequestUnits(int taskId, double requestUnits)
        {
            requestUnitsConsumed[taskId] += requestUnits;
        }

    }
}