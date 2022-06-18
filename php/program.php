<?php
define('__ROOT__', dirname(__FILE__));

echo "RootDir: " . __ROOT__ . "\r\n";

require_once  __ROOT__ . '/vendor/autoload.php';
echo "autoload.php init done\r\n";

use app\RedisConnect;
use app\RedisConsumer;
use app\RedisProducer;

$redis= new RedisConnect("localhost",6379,"",0);

$consumer1= new RedisConsumer($redis,"php1","dunp",function($msg){
    echo "\r\n----inside consumer php1: ". $msg ."---\r\n";
});

$consumer1->Start();

$consumer1->Stop();