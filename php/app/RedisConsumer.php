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

    function __construct(RedisConnect $redis, string $consumerName, string $topic, callable $onMsg)
    {
        $this->_topic = $topic;
        $this->_name = $consumerName;
        $this->_topicContainer = $topic . ":consumers";
        $this->_queueData = $topic . ":data:" . $consumerName;
        $this->redis = $redis;

        $this->_onMsg = $onMsg;
    }

    private function Do()
    {
        $temp = [];
        while (true) {
            try {
                $msg = $this->redis->Dequeue($this->_queueData);
                echo "\r\n-----\r\n".$this->_queueData.":" . $msg . "\r\n----\r\n";
                if (empty($msg)) break;

                $temp[] = $msg;

                usleep(1000);
            } catch (\Throwable $th) {
                //
                echo "Error Do";
                break;
            }
        }

        foreach ($temp as $msg) {
            
            call_user_func_array($this->_onMsg, [$msg]);
        }
    }

    public  function Start()
    {
        if ($this->_isStart) return;
        $this->_isStart = true;
        $date = new DateTime();
        $this->redis->HashSet($this->_topicContainer, $this->_name, $date->format('Y-m-d H:i:s'));
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
