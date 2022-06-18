<?php
define('__APP_ROOT_DIR__', dirname(__FILE__));

$workingDir = str_replace("\\", "/", __APP_ROOT_DIR__);

echo "RootDir: " . $workingDir . "\r\n";

require_once  $workingDir . '/vendor/autoload.php';
echo "autoload.php init done\r\n";

use app\RedisConnect;
use app\RedisConsumer;
use app\RedisProducer;

$redis = new RedisConnect("localhost", 6379, "", 0);

$consumer1 = new RedisConsumer($redis, "php1", "dunp", function ($msg) {
    echo "\r\n----inside consumer php1: " . $msg . "---\r\n";
    //// you code business here
});

$consumer1->Start();

$consumer1->Stop();
