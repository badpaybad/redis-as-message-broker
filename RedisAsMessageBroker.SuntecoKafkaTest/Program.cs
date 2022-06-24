// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;
using System.Net;
using System.Threading.Tasks.Dataflow;

Console.WriteLine("Hello, World!");
string host = "127.0.0.1:9092";

var cfg = new Dictionary<string, string>()
{
    {"bootstrap.servers",host },
    {"sasl.username",uid },
    {"sasl.password",pwd },
    {"sasl.mechanisms","SCRAM-SHA-512" },
    {"security.protocol","SASL_SSL" },
};

var cliConf = new ClientConfig(cfg)
{
    BootstrapServers = host,
    ClientId = Dns.GetHostName() + ": " + DateTime.Now,
    SaslUsername = uid,
    SslKeyPassword = pwd,
    SecurityProtocol = host.IndexOf("127.0.0.1") >= 0 ? SecurityProtocol.Plaintext : SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.ScramSha512,
};

//recomnend if topic have 100msg/seconds , number partion and consumer will be = 10 , mean number of partion = 1/ 10 number of total pushed msg/sec
var numberOfPartion = 6;

string topicName = $"dunp-test-1producer-{numberOfPartion}consumer-{numberOfPartion}partion";

var adminKafka = new AdminClientBuilder(cfg.Select(i => new KeyValuePair<string, string>(i.Key, i.Value))).Build();

try
{
    var existed = adminKafka.GetMetadata(topicName, TimeSpan.FromSeconds(10));
    if (existed.Topics.Count > 0)
    {
        await adminKafka.DeleteTopicsAsync(new List<string> { topicName });
    }

    await adminKafka.CreateTopicsAsync(new List<TopicSpecification> {
        new TopicSpecification
        {
            Name= topicName,
            NumPartitions=numberOfPartion
        }
        });
}
catch
{
    //
}

var configConsumer = new ConsumerConfig(cliConf)
{
    BootstrapServers = host,
    GroupId = "omttestperformance",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    Acks = Acks.All,
    AllowAutoCreateTopics = true,
    EnableAutoCommit = true,
};

List<IConsumer<Ignore, string>> consumers = new List<IConsumer<Ignore, string>>();

for (var i = 0; i < numberOfPartion; i++)
{
    var consumer = new ConsumerBuilder<Ignore, string>(configConsumer).Build();
    consumers.Add(consumer);
}

ActionBlock<IConsumer<Ignore, string>> ab = new ActionBlock<IConsumer<Ignore, string>>(async (consumer) =>
{
    _ = Task.Run(async () =>
    {
        consumer.Subscribe(topicName);

        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume();

                var obj = JsonConvert.DeserializeObject<TestMsg>(consumeResult.Value);

                Console.WriteLine(consumer.Name + " " + obj.ToString());

                consumer.Commit(consumeResult);

            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(consumer.Name + ": " + ex.Message);
            }
            await Task.Delay(1);
        }
    });

});

foreach (var consumer in consumers)
{
    await ab.SendAsync(consumer);
}

var configProducer = new ProducerConfig(cliConf)
{

};

var producer = new ProducerBuilder<Null, string>(configProducer).Build();

_ = Task.Run(async () =>
{
    long counter = 0;
    while (true)
    {
        Console.WriteLine("Producer");

        ActionBlock<int> abp = new ActionBlock<int>(async (i) =>
        {

            counter++;
            var r = await producer.ProduceAsync(topicName, new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(new TestMsg
                {
                    CreatedAt = DateTime.Now,
                    Msg = " Nguyen Phan Du " + i
                })
            });
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 5 });

        for (var i = 0; i < 100; i++)
        {
            await abp.SendAsync(i);
        }

        abp.Complete();
        await abp.Completion;

        Console.WriteLine("Total producer sent: " + counter);

        await Task.Delay(1000);
    }

});

AppDomain.CurrentDomain.ProcessExit += async (sender, e) =>
{
    foreach (var consumer in consumers)
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
    foreach (var consumer in consumers)
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
