using StackExchange.Redis;

namespace RedisAsMessageBroker.Core
{
    public class RedisConsumer
    {
        RedisConnect _redis;

        string _topic;

        string _name;

        string _queueData;

        Func<string, Task> _onMsg;

        string _topicContainer;

        ISubscriber _subscriber;
        ChannelMessageQueue _channelMsgQueue;

        bool _started = false;

        public RedisConsumer(RedisConnect redis, string consumerName, string topic, Func<string, Task> onMsg)
        {
            _redis = redis;
            _topic = topic;
            _name = consumerName;

            _queueData = $"{_topic}:data:{_name}";

            _topicContainer = $"{topic}:consumers";

            _onMsg = onMsg;

        }
        async Task Do()
        {
            List<string> temp = new List<string>();
            while (true)
            {
                try
                {
                    var msg = await _redis.Dequeue(_queueData);

                    if (string.IsNullOrEmpty(msg)) break;

                    temp.Add(msg);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    break;
                }

                await Task.Delay(1);
            }

            foreach (var t in temp)
            {
                _ = _onMsg(t);
            }
        }

        async Task<ISubscriber> GetSubscriber()
        {
            _subscriber = _subscriber ?? await _redis.GetSubscriber();

            return _subscriber;
        }
        public async Task<long> Publish(string msg)
        {
            var listConsumer = await _redis.HashGetAll(_topicContainer);

            if (listConsumer.Count <= 0) return 0;

            List<Task> tasks = new List<Task>();
            foreach (var c in listConsumer)
            {
                var queueName = $"{_topic}:data:{c.Key}";
                tasks.Add(_redis.Enqueue(queueName, msg));
            }

            await Task.WhenAll(tasks);

            return await (await GetSubscriber()).PublishAsync(_topic, msg);
        }
        public async Task Start()
        {
            await Task.Yield();

            if (_started) return;

            _started = true;

            _channelMsgQueue = await (await GetSubscriber()).SubscribeAsync(_topic);

            _channelMsgQueue.OnMessage((msg) =>
            {
                _ = Do();
            });

            await _redis.HashSet(_topicContainer, _name, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

            _ = Do();
        }

        public async Task Stop()
        {
            await _redis.HashDelete(_topicContainer, _name);

            _ = Do();
        }
    }
}