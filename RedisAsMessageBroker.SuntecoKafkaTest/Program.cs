// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
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
var configProducer = new ProducerConfig(cfg)
{
    BootstrapServers = host,
    ClientId = Dns.GetHostName(),
    SaslUsername = uid,
    SslKeyPassword = pwd,
    //SecurityProtocol = SecurityProtocol.Plaintext
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.ScramSha512,
};
string topics = "dunp-test-1producer-11consumer-2";

List<IConsumer<Ignore, string>> consumers = new List<IConsumer<Ignore, string>>();


for (var i = 0; i < 3; i++)
{
    var configConsumer = new ConsumerConfig(cfg)
    {
        BootstrapServers = host,
        //BootstrapServers = "localhost:9092,host2:9092",
        GroupId = "omt" + i,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        Acks = Acks.All,
        AllowAutoCreateTopics = true,
        EnableAutoCommit = true,
        SaslUsername = uid,
        SslKeyPassword = pwd,
        //SecurityProtocol = SecurityProtocol.Plaintext
        SecurityProtocol = SecurityProtocol.SaslSsl,
        SaslMechanism = SaslMechanism.ScramSha512,

    };

    var consumer = new ConsumerBuilder<Ignore, string>(configConsumer).Build();

    consumers.Add(consumer);

}

ActionBlock<IConsumer<Ignore, string>> ab = new ActionBlock<IConsumer<Ignore, string>>(async (consumer) =>
{
    _ = Task.Run(async () =>
    {
        consumer.Subscribe(topics);

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

foreach(var consumer in consumers)
{
   await ab.SendAsync(consumer);
}

var producer = new ProducerBuilder<Null, string>(configProducer).Build();

_ = Task.Run(async () =>
{
    while (true)
    {
        Console.WriteLine("Producer");

        ActionBlock<int> abp = new ActionBlock<int>(async (i) => {
            var r = await producer.ProduceAsync(topics, new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(new TestMsg
                {
                    CreatedAt = DateTime.Now,
                    Msg = " Nguyen Phan Du "+i
                })
            });
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism=5});

        for (var i = 0; i < 100; i++)
        {
            await abp.SendAsync(i);
        }

        abp.Complete();
        await abp.Completion;

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
