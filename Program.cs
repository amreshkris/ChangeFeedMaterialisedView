using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace ChangeFeedMaterialisedView
{
    class Program
    {
        private static readonly string monitoredContainer = "cart-container";
        private static readonly string leasesContainer = "leases-cart-container";
        private static readonly string partitionKeyPath = "/id";

        private static readonly string buyerContainer = "buyer-container";

        private static readonly string databaseId = "changefeed-basic";

        private static string endpoint = "changefeed-basic";

        private static string authKey = "changefeed-basic";
        static async Task Main(string[] args)
        {
            try
            {
                IConfigurationRoot configuration = new ConfigurationBuilder()
                  .AddJsonFile("appSettings.json")
                  .Build();

                Program.endpoint = configuration["EndPointUrl"];
                if (string.IsNullOrEmpty(endpoint))
                {
                    throw new ArgumentNullException
                    ("Please specify a valid endpoint in the appSettings.json");
                }

                Program.authKey = configuration["AuthorizationKey"];
                if (string.IsNullOrEmpty(authKey) || string.Equals(authKey, "Super secret key"))
                {
                    throw new ArgumentException("Please specify a valid AuthorizationKey in the appSettings.json");
                }

                using (CosmosClient client = new CosmosClient(endpoint, authKey))
                {
                    await Program.RunStartFromBeginningFeed("changefeed-basic", client);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Basic change feed functionality.
        /// </summary>
        /// <remarks>
        /// When StartAsync is called, the Change Feed Processor starts an initialization process that can take several milliseconds, 
        /// in which it starts connections and initializes locks in the leases container.
        /// </remarks>
        public static async Task RunStartFromBeginningFeed(
            string databaseId,
            CosmosClient client)
        {
            // Initialize database & containers, delete if exsits and create db,containers
            await Program.InitializeAsync(databaseId, client);

            // <BasicInitialization>
            Container leaseContainer = client.GetContainer(databaseId, Program.leasesContainer);
            Container monitoredContainer = client.GetContainer(databaseId, Program.monitoredContainer);
            
            ChangeFeedProcessor changeFeedProcessor = monitoredContainer
                .GetChangeFeedProcessorBuilder<Cart>("changeFeedBasic9", Program.HandleChangesAsync)
                    .WithInstanceName("consoleHost")
                    .WithLeaseContainer(leaseContainer)
                    .WithStartTime(DateTime.MinValue.ToUniversalTime())
                    .Build();
            // </BasicInitialization>

            Console.WriteLine("Starting Change Feed Processor...");
            await changeFeedProcessor.StartAsync();
            Console.WriteLine("Change Feed Processor started.");

            // Wait random time for the delegate to output all messages after initialization is done
            // await Task.Delay(5000);
            Console.WriteLine("Press any key to exit..");
            Console.ReadKey();
            await changeFeedProcessor.StopAsync();
        }

        /// <summary>
        /// Initialize Cosmos Database,Monitored & Leased container
        /// </summary>
        /// <param name="databaseId"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        private static async Task InitializeAsync(
            string databaseId,
            CosmosClient client)
        {
            Database database;
            try
            {
                database = await client.GetDatabase(databaseId).ReadAsync();
                await database.CreateContainerIfNotExistsAsync(new ContainerProperties(Program.leasesContainer, Program.partitionKeyPath));
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
            }
        }

        /// <summary>
        /// The delegate receives batches of changes as they are generated in the change feed and can process them.
        /// </summary>
        // <Delegate>
        private static async Task HandleChangesAsync(ChangeFeedProcessorContext context, IReadOnlyCollection<Cart> cart, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Started handling changes for lease {context.LeaseToken}...");
            Console.WriteLine($"Change Feed request consumed {context.Headers.RequestCharge} RU.");
            // SessionToken if needed to enforce Session consistency on another client instance
            Console.WriteLine($"SessionToken ${context.Headers.Session}");

            // We may want to track any operation's Diagnostics that took longer than some threshold
            if (context.Diagnostics.GetClientElapsedTime() > TimeSpan.FromSeconds(1))
            {
                Console.WriteLine($"Change Feed request took longer than expected. Diagnostics:" + context.Diagnostics.ToString());
            }


            foreach (Cart item in cart)
            {
                if (item.OrderStatus == CartStatus.Abondoned)
                {
                    Console.WriteLine($"Detected operation for item with id {item.CartId}");                    
                    ContactBuyer(item.BuyerId,item.CartId);

                }
                await Task.Delay(1);
            }
        }

        private static async void ContactBuyer(string buyerId,string cartId)
        {
            CosmosClient client = new CosmosClient(Program.endpoint, Program.authKey);
            Container buyerContainer = client.GetContainer(Program.databaseId, Program.buyerContainer);
            ItemResponse<Buyer> abondonedBuyerDetails = await buyerContainer.ReadItemAsync<Buyer>(buyerId,new PartitionKey(buyerId));
            var abondonedBuyer = abondonedBuyerDetails.Resource;
            Console.WriteLine($"RU charged to fetch buyer {abondonedBuyerDetails.RequestCharge}");
            Console.WriteLine($"Customer {abondonedBuyer.Name} with contact number {abondonedBuyer.ContactNumber} abondoned the cart with cart id {cartId}");            
        }
    }
}