from ast import While
from time import sleep
import redis
import datetime

class RedisConnect:
    """ https://redis.readthedocs.io/en/latest/
    """
    def __init__(self, host:str,port:int, pwd:str, db:int) -> None:
        self._host=host
        self._port=port
        self._db=db
        self._pwd=pwd
        self._redis=redis.Redis(host,port,db,pwd)        
        
        pass
    
    def Enqueue(self,key:str,msg:str):
        return self._redis.lpush(key,msg)
    
    def Dequeue(self,key:str):
        msg = self._redis.rpop(key)
        
        if msg and msg!=None:
            return msg.decode("utf-8") 
        
        return ""
        
    
    def HashSet(self,key:str,field:str,msg:str)  :
        return self._redis.hset(key,field,msg)
    
    def HashGetAll(self,key:str):
        dic={}
        temp= self._redis.hgetall(key)
        
        for k in temp:
            v=temp[k]
            dic[k.decode("utf-8") ]=v.decode("utf-8") 
            
        return dic
    
    def HashDelete(self,key:str,field:str):
        return self._redis.hdel(key,field)
    
    def Publish(self,topic:str,msg:str):
        return self._redis.publish(topic,msg)
    
    def Subscribe(self,topic:str):
        sub =self._redis.pubsub()
        sub.subscribe(topic)       
        #msg = sub.get_message() 
        return sub
                
    def SubscribeGetMsg(self, sub: redis.client.PubSub):
        msg= sub.get_message()
        
        if msg and msg!=None:
            data= msg['data']
            if data and data !=1:
                dataStr= data.decode("utf-8") 
                if dataStr and dataStr!=None :
                    return dataStr
        return ""
    
    def Unsubscribe(self,sub):
        sub.unsubscribe()        
    
    def ListLen(self,key:str):
        return self._redis.llen(key)
  

from multiprocessing import Process, Queue
from threading import Thread
            
class RedisConsummer:
    def __init__(self, redis:RedisConnect, name:str,topic:str, onMsg:callable) :
        self._redis=redis
        self._name=name
        self._topic=topic
        self._topicContainer=topic+":consumers"
        self._queueName=topic+":data:"+name
        self._onMsg=onMsg
        self._isStart=False
        self._isStop=False
        self.subscriber= self._redis.Subscribe(topic)
        self._numOfItemDequeue=2
        self._thread=Thread(target=self.pubSubOnMessage, args=(self.subscriber,), daemon=True)
        pass    
        
    def pubSubOnMessage(self,subscriber):
            
        while self._isStop==False:
            msg = self._redis.SubscribeGetMsg(self.subscriber)
            if msg and msg!="":                
                #trigger for dequeue
                self.Do()            
                
            sleep(0.00001)
    
    def Publish(self,msg:str):
        listSub= self._redis.HashGetAll(self._topicContainer)
        if len(listSub)<=0:
            return 0
        for k in listSub:
            queueName= self._topic+":data:"+k
            self._redis.Enqueue(queueName,msg)    
            
        return self._redis.Publish(self._topic,msg)
            
    def Start(self):
        if self._isStart:
            return
        self._isStart=True
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._redis.HashSet(self._topicContainer,self._name,now)
        
        self.Do()
        self._thread.start()
    
    def Stop(self):
        self._isStop=True
        self._isStart=False
        self._redis.HashDelete(self._topicContainer,self._name)
        self._redis.Unsubscribe(self.subscriber)
        self._thread.join()
        self.Do()
    
    def Do(self):        
        counterNoMsg=0
        while True:
            temp=[]
            for i in range(self._numOfItemDequeue):
                msg=self._redis.Dequeue(self._queueName)
                if msg!="":
                    temp.append(msg)
            
            if len(temp)>0:
                for msg in temp:
                    self._onMsg(msg)                
            else:
                length= self._redis.ListLen(self._queueName)
                if(length<=0):
                    counterNoMsg=counterNoMsg+1
                    if counterNoMsg>=5:
                        counterNoMsg=0
                        break
                    sleep(0.001)
                else:
                    counterNoMsg=0
                            
            sleep(0.001)

# redisConn= RedisConnect("localhost",6379,"",0)

# redisConn.HashSet("xxx","abc","123")

# print(redisConn.HashGetAll ("xxx"))

# redisConn.Enqueue("dunptest","123")

# x=redisConn.Dequeue("dunptest")
# print(x)

# tempSub= redisConn.Subscribe("topictest")

# redisConn.Publish("topictest","123 abc")

# while True:
#     msg= redisConn.SubscribeGetMsg(tempSub)
#     print(msg)
    
#     sleep(1)