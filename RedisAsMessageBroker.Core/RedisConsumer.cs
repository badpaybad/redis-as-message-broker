using StackExchange.Redis;

namespace RedisAsMessageBroker.Core
{
    public class RedisConsumer
    {
        readonly RedisConnect _redis;

        readonly string _topic;

        readonly string _name;

        readonly string _queueData;

        readonly Func<string, Task> _onMsg;

        readonly string _topicContainer;

        ISubscriber _subscriber;
        ChannelMessageQueue _channelMsgQueue;

        bool _started = false;
        readonly int _numOfItemDequeue = (Environment.ProcessorCount / 4)+1;
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
            int counterNoMsg = 0;
            while (true)
            {
                try
                {
                    List<string> temp = new List<string>();
                    for (var i = 0; i < _numOfItemDequeue; i++)
                    {
                        var msg = await _redis.Dequeue(_queueData);

                        if (!string.IsNullOrEmpty(msg))
                            temp.Add(msg);
                    }
                    if (temp.Count > 0)
                    {
                        List<Task> tasks = new List<Task>();
                        foreach (var t in temp)
                        {
                            tasks.Add(_onMsg(t));
                        }
                        await Task.WhenAll(tasks);
                    }
                    else
                    {
                        var lng = await _redis.ListLength(_queueData);
                        if (lng <= 0)
                        {
                            counterNoMsg++;
                            if (counterNoMsg > 5)
                            {
                                counterNoMsg = 0;
                                break;
                            }
                            await Task.Delay(100);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    break;
                }

                await Task.Delay(1);
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

            await _redis.HashSet(_topicContainer, _name, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

            _ = Do();

            _channelMsgQueue = await (await GetSubscriber()).SubscribeAsync(_topic);

            _channelMsgQueue.OnMessage((msg) =>
            {
                //pub sub trigger dequeue
                _ = Do();
            });

          
        }

        public async Task Stop()
        {
            await _redis.HashDelete(_topicContainer, _name);
            await (await GetSubscriber()).UnsubscribeAsync(_topic);
            _ = Do();

            _started = false;
        }
    }
}