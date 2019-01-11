using System;
using System.Threading;
using Newtonsoft.Json;

namespace MsLearnCosmosDB
{
    public class Order
    {
        [JsonProperty(PropertyName = "id")]
        public string OrderId { get; set; }
        public string OrderTime;
        public string OrderStatus { get; set; }
        public OrderItem Item { get; set; }
        public long Quantity { get; set; }
        public PaymentType PaymentInstrumentType { get; set; }
        public string PurchaseOrderNumber { get; set; }
        public CustomerDetails Customer { get; set; }
        public DateTime ShippingDate { get; set; }
        // padding for record size
        public byte[] Data { get; set; }

        static Order[] Orders;

        static int NextOrder = -1;

        public static void Allocate(int numOrders)
        {
            Orders = new Order[numOrders];
            for (int i = 0; i < numOrders; i++)
            {
                Orders[i] = Order.NewOrder();
            }
        }

        public static Order NewOrder()
        {
            Bogus.Faker<Order> orderGenerator = new Bogus.Faker<Order>().Rules(
            (faker, order) =>
            {
                order.OrderId = faker.Random.Guid().ToString();
                var now = DateTime.Now;
                order.OrderTime = now.ToShortTimeString();
                order.OrderStatus = "NEW";
                order.Item = OrderItem.GetRandomItem();
                order.Quantity = faker.Random.Long(1, 100);
                order.PaymentInstrumentType = faker.Random.Enum<PaymentType>();
                order.PurchaseOrderNumber = faker.Random.Replace("###-#####-##");
                order.Customer = CustomerDetails.GetRandomCustomer();
                order.ShippingDate = now.Add(new TimeSpan(faker.Random.Int(0, 10), 0, 0, 0));
                order.Data = faker.Random.Bytes(10);
            });

            return orderGenerator.Generate();
        }

        public static Order Next()
        {
            int index = Interlocked.Increment(ref NextOrder);

            if (index >= Orders.Length)
            {
                throw new Exception("Order allocation exceeded");
            }

            return Orders[index];
        }
    }
}