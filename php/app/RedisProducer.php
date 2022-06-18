<?php
namespace app;
use App\RedisConnect;

class RedisProducer{
    private RedisConnect $redis;
    function __construct(RedisConnect $redis)
    {
        $this->redis=$redis;
        //
    }

    function Publish(string $topic, string $msg){
        $listConsumer =  $this->redis->HashGetAll($topic.":consumers");

        if ( count($listConsumer)==0) return 0;

        foreach($listConsumer as $c)
        {
            $queueName=$topic.":data:".$c;
            $this->redis->Enqueue($queueName,$msg);
        }

        return $this->redis->Publish($topic,$msg);
    }
}