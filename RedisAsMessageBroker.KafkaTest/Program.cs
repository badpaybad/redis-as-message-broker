// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Net;

Console.WriteLine("Hello, World!");
string host = "127.0.0.1:9092";

var config = new ConsumerConfig
{
    BootstrapServers = host,
    //BootstrapServers = "localhost:9092,host2:9092",
    GroupId = "omt",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    Acks = Acks.All,
    AllowAutoCreateTopics = true,
    EnableAutoCommit = true,
};

string topics = "dunp-test";
var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

_ = Task.Run(async () =>
{

    consumer.Subscribe(topics);
    while (true)
    {
        var consumeResult = consumer.Consume();
        Console.WriteLine("----Consumer:begin");
        var obj = JsonConvert.DeserializeObject<TestMsg>(consumeResult.Value);
        Console.WriteLine(obj.ToString());
        Console.WriteLine("----Consumer:end");
        consumer.Commit(consumeResult);

        //await Task.Delay(1);
    }

});

var configProducer = new ProducerConfig
{
    BootstrapServers = host,
    ClientId = Dns.GetHostName(),

};

var producer = new ProducerBuilder<Null, string>(config).Build();

_ = Task.Run(async () =>
{
    while (true)
    {
        Console.WriteLine("Producer");

        Parallel.ForEach(Enumerable.Range(0, 100), new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, async (i) =>
        {
            var r = await producer.ProduceAsync(topics, new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(new TestMsg
                {
                    CreatedAt = DateTime.Now,
                    Msg = " Nguyen Phan Du "
                })
            });
        });

        await Task.Delay(1);
    }

});

AppDomain.CurrentDomain.ProcessExit += async (sender, e) =>
{
    try { consumer.Close(); }
    catch
    {
        //
    }

    Console.WriteLine("Exiting Main()");
    await Task.Yield();
};

Console.CancelKeyPress += (sender, e) =>
{
    try { consumer.Close(); }
    catch
    {
        //
    }
    Console.WriteLine("Exiting Main()");

    Environment.Exit(0);
};

while (true)
{
    if (Console.KeyAvailable)
    {
        var key = Console.ReadKey(true).Key;

        //Console.WriteLine(key);
    }
    await Task.Delay(1000);
}


public class TestMsg
{
    static long id = 0;

    public long Id { get; set; } = id++;

    public string Msg { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.Now;
    public override string ToString()
    {
        return $"Received after: {(DateTime.Now - CreatedAt).TotalMilliseconds} id={id} : {CreatedAt}";
    }
}
