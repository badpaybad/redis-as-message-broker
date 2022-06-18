// See https://aka.ms/new-console-template for more information
using RedisAsMessageBroker.Core;

Console.WriteLine("Hello, World!");

RedisConnect redis = new RedisConnect("localhost", 6379);

_ = Task.Run(() =>
{

    var c1 = new RedisConsumer(redis, "dunp1", "dunp", async (msg) =>
     {
         Console.WriteLine("consummer1: "+ msg);
    
     });

    c1.Start();

});

var c2= new RedisConsumer(redis, "dunp2", "dunp", async (msg) =>
{
    Console.WriteLine("consummer2: " + msg);
    
});

c2.Start();


while (true)
{
    new RedisProducer(redis).Publish("dunp", DateTime.Now.ToString());

    await Task.Delay(1);
}