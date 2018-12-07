using System;

namespace MsLearnCosmosDB
{
    public class CustomerDetails
    {
        public Guid id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string StreetAddress { get; set; }
        public string ZipCode { get; set; }
        public string State { get; set; }
        static CustomerDetails[] Customers;
        static Random RandomIndex = new Random();

        /// <summary>
        /// Allocate the specified numCustomers.
        /// </summary>
        /// <param name="numCustomers">Number customers.</param>
        public static void Allocate(int numCustomers)
        {
            Customers = new CustomerDetails[numCustomers];
            for (int i = 0; i < numCustomers; i++)
            {
                Customers[i] = NewCustomerDetails();
            }
        }
        public static CustomerDetails NewCustomerDetails()
        {
            Bogus.Faker<CustomerDetails> customerDetailsGenerator = new Bogus.Faker<CustomerDetails>().Rules(
            (faker, customerDetails) =>
            {
                customerDetails.id = faker.Random.Guid();
                customerDetails.FirstName = faker.Name.FirstName();
                customerDetails.LastName = faker.Name.LastName();
                customerDetails.Email = faker.Internet.Email(customerDetails.FirstName, customerDetails.LastName);
                customerDetails.StreetAddress = faker.Address.StreetAddress();
                customerDetails.State = faker.Address.StateAbbr();
                customerDetails.ZipCode = faker.Address.ZipCode();
            });

            return customerDetailsGenerator.Generate();
        }

        public static CustomerDetails GetRandomCustomer()
        {
            // Assumes that customers are being allocated in a single thread.
            return Customers[RandomIndex.Next(0, Customers.Length)];
        }

    }

}