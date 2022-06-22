// See https://aka.ms/new-console-template for more information
using Newtonsoft.Json;
using RedisAsMessageBroker.Core;

RedisConnect redis = new RedisConnect("localhost", 6379);

_ = Task.Run(() =>
{
    var c1 = new RedisConsumer(redis, "dunp1", "dunp", async (msg) =>
     {
         await Task.Yield();
         var obj = JsonConvert.DeserializeObject<TestMsg>(msg);
         Console.WriteLine(obj.ToString());
         // you code business here
     });

    _ = c1.Start();

});

//var c2 = new RedisConsumer(redis, "dunp2", "dunp", async (msg) =>
// {
//     await Task.Yield();
//     Console.WriteLine("consummer2: " + msg);
//     // you code business here
// });

//_ = c2.Start();

long msgIdx = 0;

var producer = new RedisProducer(redis);

var stressTest = Enumerable.Range(0, 100);
Console.WriteLine("Start stress test");

_ = Task.Run(async () => {

    while (true)
    {
        Parallel.ForEach(stressTest, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
            async i =>
            {
                await producer.Publish("dunp",
                JsonConvert.SerializeObject(new TestMsg
                  {
                      CreatedAt = DateTime.Now,
                      Msg = " Nguyen Phan Du "
                  })
                );
            });

        msgIdx++;

        await Task.Delay(1);
    }

});

while (true)
{
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
