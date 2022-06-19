// See https://aka.ms/new-console-template for more information
using RedisAsMessageBroker.Core;

RedisConnect redis = new RedisConnect("localhost", 6379);

_ = Task.Run(() =>
{

    var c1 = new RedisConsumer(redis, "dunp1", "dunp", async (msg) =>
     {
         await Task.Yield();
         Console.WriteLine("consummer1: " + msg);
         // you code business here
     });

    _ = c1.Start();

});

var c2 = new RedisConsumer(redis, "dunp2", "dunp", async (msg) =>
 {
     await Task.Yield();
     Console.WriteLine("consummer2: " + msg);
     // you code business here
 });

_ = c2.Start();

long msgIdx = 0;

var producer = new RedisProducer(redis);

var stressTest = Enumerable.Range(0, 1000);
Console.WriteLine("Start stress test");

while (true)
{
    List<Task> tasks = new List<Task>();    

    //Parallel.ForEach(stressTest, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
    //    i =>
    //    {
    //        tasks.Add( producer.Publish("dunp", i + "__" + DateTime.Now.ToString()));
            
    //    });

    tasks.Add( producer.Publish("dunp", msgIdx + "_" + DateTime.Now.ToString()));

    msgIdx++;

    await Task.WhenAll(tasks);

    await Task.Delay(1);
}