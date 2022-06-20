<?php
define('__APP_ROOT_DIR__', dirname(__FILE__));

$workingDir = str_replace("\\", "/", __APP_ROOT_DIR__);

echo "RootDir: " . $workingDir . "\r\n";

require_once  $workingDir . '/vendor/autoload.php';
echo "autoload.php init done\r\n";

if (extension_loaded("pthreads")) {
    echo "Using pthreads\n";
} else  echo "Using polyfill\n";

use libs\RedisConnect;
use libs\RedisConsumer;
use libs\RedisProducer;

$redis = new RedisConnect("localhost", 6379, "", 0);

$consumer1 = new RedisConsumer($redis, "php1", "dunp", function ($msg) {
    echo "\r\n----inside consumer php1: " . $msg . "---\r\n";
    //// you code business here
});

class RedisWorkerConsumer extends Thread
{
    private $consumer;
    public function __construct(RedisConsumer $redisConsumer)
    {
        $this->consumer = $redisConsumer;
    }

    public function run()
    {
        $this->consumer->Start();
    }
}

class RedisWorkerProducer extends Thread
{
    private $consumer;
    public function __construct(RedisConsumer $redisConsumer)
    {
        $this->consumer = $redisConsumer;
    }

    public function run()
    {
        $counter = 0;
        while (true) {

            $this->consumer->Publish("from php " . $counter);

            $counter = $counter + 1;
            sleep(1);
        }
    }
}


$worker1 = new RedisWorkerConsumer($consumer1);

$worker1->start();

$worker2 = new RedisWorkerProducer($consumer1);

$worker2->start();

$worker1->join();
$worker2->join();
$consumer1->Stop();
