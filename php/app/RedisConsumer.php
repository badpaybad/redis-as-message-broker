<?php

namespace app;

use App\RedisConnect;
use DateTime;

class RedisConsumer
{
    private RedisConnect $redis;
    private $_onMsg;
    private string $_topic;
    private string $_name;
    private string $_topicContainer;
    private string $_queueData;
    private bool $_isStart = false;

    private RedisConnect $redisForDequeue;

    function __construct(RedisConnect $redis, string $consumerName, string $topic, callable $onMsg)
    {
        $this->_topic = $topic;
        $this->_name = $consumerName;
        $this->_topicContainer = $topic . ":consumers";
        $this->_queueData = $topic . ":data:" . $consumerName;
        $this->redis = $redis;

        $this->redisForDequeue = new RedisConnect($redis->host, $redis->port, $redis->password, $redis->defaultDb);

        $this->_onMsg = $onMsg;
    }

    private function Do()
    {
        $counterNoMsg = 0;
        while (true) {
            try {
                $temp = [];
                for ($i = 0; $i < 10; $i++) {
                    $msg = $this->redisForDequeue->Dequeue($this->_queueData);
                    if (!empty($msg)) {
                        $temp[] = $msg;
                    }
                }

                if (count($temp) > 0) {
                    foreach ($temp as $msg) {
                        call_user_func_array($this->_onMsg, [$msg]);
                    }
                } else {
                    $len = $this->redisForDequeue->ListLength($this->_queueData);

                    if ($len <= 0) {
                        $counterNoMsg = $counterNoMsg + 1;
                        usleep(10000);
                        if ($counterNoMsg > 5) {
                            $counterNoMsg = 0;
                            break;
                        }
                    } else {
                        $counterNoMsg = 0;
                    }
                }

                usleep(100);
            } catch (\Throwable $th) {
                //
                echo "Error Do: " . $th;
                break;
            }
        }
    }

    function Publish(string $msg)
    {
        $listConsumer =  $this->redisForDequeue->HashGetAll($this->_topic . ":consumers");

        if (count($listConsumer) == 0) return 0;

        foreach ($listConsumer as $c) {
            $queueName = $this->_topic . ":data:" . $c;
            $this->redisForDequeue->Enqueue($queueName, $msg);
        }

        return $this->redisForDequeue->Publish($this->_topic, $msg);
    }

    public  function Start()
    {
        if ($this->_isStart) return;

        $this->_isStart = true;
        $date = new DateTime();
        $this->redisForDequeue->HashSet($this->_topicContainer, $this->_name, $date->format('Y-m-d H:i:s'));

        $this->Do();
        $this->redis->Subscribe($this->_topic, function ($msg) {
            $this->Do();
        });
    }

    public function Stop()
    {
        $this->redis->Unsubscribe($this->_topicContainer);
        $this->Do();
    }

    function __destruct()
    {
        $this->Stop();
    }
}
