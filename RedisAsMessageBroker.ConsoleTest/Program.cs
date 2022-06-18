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


while (true)
{
    _ = new RedisProducer(redis).Publish("dunp", DateTime.Now.ToString());

    await Task.Delay(1000);
}