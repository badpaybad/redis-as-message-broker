using StackExchange.Redis;

namespace RedisAsMessageBroker.Core
{
    public class RedisConnect
    {

        readonly SocketManager _socketManager;
        IConnectionMultiplexer _connectionMultiplexer;
        readonly ConfigurationOptions _options ;

        public RedisConnect(string host, int port, string pwd = "", int dbIdx = 0, bool allowAdmin = true)
        {

            _socketManager = new SocketManager(host);

            _options = new ConfigurationOptions()
            {
                EndPoints =
                {
                    {host, port}
                },
                Password = pwd,
                AllowAdmin = allowAdmin,
                SyncTimeout = 10 * 1000,
                SocketManager = _socketManager,
                AbortOnConnectFail = false,
                ConnectTimeout = 11 * 1000,
                DefaultDatabase = dbIdx,
                //HighPrioritySocketThreads = true,
                ConnectRetry = 3,
            };
        }


        async Task<ConnectionMultiplexer> GetConnection()
        {
            await Task.Yield();

            var multiplexConnection = await ConnectionMultiplexer.ConnectAsync(_options);

            return multiplexConnection;
        }
        async Task<IConnectionMultiplexer> GetConnectionMultiplexer()
        {
            await Task.Yield();
            if (_connectionMultiplexer != null && _connectionMultiplexer.IsConnected)
                return _connectionMultiplexer;

            if (_connectionMultiplexer != null && !_connectionMultiplexer.IsConnected)
            {
                _connectionMultiplexer.Dispose();
            }

            _connectionMultiplexer = await GetConnection();
            if (!_connectionMultiplexer.IsConnected)
            {
                var exception = new Exception($"Can not connect to redis RedisServices.RedisConnectionMultiplexer {_options.ToString()}");
                Console.WriteLine(exception);
                throw exception;
            }
            return _connectionMultiplexer;
        }
        public async Task<IDatabase> GetDatabase(int dbIdx = -1)
        {
            await Task.Yield();
            var mc = await GetConnectionMultiplexer();
            var redisDatabase = mc.GetDatabase(dbIdx);
            return redisDatabase;
        }

        public async Task<ISubscriber> GetSubscriber()
        {
            await Task.Yield();
            var redisSubscriber = (await GetConnectionMultiplexer()).GetSubscriber();

            return redisSubscriber;
        }
        public async Task<IServer> GetServer()
        {
            await Task.Yield();
            var multi = await GetConnection();
            var ep = _options.EndPoints.FirstOrDefault();
            return multi.GetServer(ep);

        }

        public async Task<long> Enqueue(string queueName, string msg)
        {
            return await (await GetDatabase()).ListLeftPushAsync(queueName, msg);
        }

        public async Task<string> Dequeue(string queueName)
        {
            return await (await GetDatabase()).ListRightPopAsync(queueName);
        }

        public async Task<bool> HashSet(string key, string field, string value)
        {

            return await (await GetDatabase()).HashSetAsync(key, field, value);

        }

        public async Task<bool> HashDelete(string key, string fieldName)
        {
            return await (await GetDatabase()).HashDeleteAsync(key, fieldName);

        }
        public async Task<Dictionary<string, string>> HashGetAll(string key)
        {
            var data = await (await GetDatabase()).HashGetAllAsync(key);

            Dictionary<string, string> temp = new Dictionary<string, string>();
            foreach (var d in data)
            {
                temp.Add(d.Name, d.Value);
            }
            return temp;

        }
    }
}