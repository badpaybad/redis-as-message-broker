namespace RedisAsMessageBroker.Core
{
    public class RedisProducer
    {
        RedisConnect _redis;

        public RedisProducer(RedisConnect redis)
        {
            _redis = redis;
        }

        public async Task<long> Publish(string topic, string msg)
        {
            var listConsumer = await _redis.HashGetAll($"{topic}:consumers");

            if (listConsumer.Count <= 0) return 0;

            List<Task> tasks = new List<Task>();
            foreach (var c in listConsumer)
            {
                var queueName = $"{topic}:data:{c.Key}";
                tasks.Add(_redis.Enqueue(queueName, msg));
            }

            await Task.WhenAll(tasks);

            return await (await _redis.GetSubscriber()).PublishAsync(topic, msg);
        }
    }
}