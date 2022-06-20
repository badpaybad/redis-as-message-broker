from time import sleep
import RedisConnect

def onMsg(msg):
    print("msg python 1: "+ msg)
    # you code bussiness here

connect= RedisConnect.RedisConnect("localhost",6379,"",0)
consumer= RedisConnect.RedisConsummer(connect,"python1","dunp", onMsg)
consumer.Start()

counter=0
while True:
    
    consumer.Publish(f"from python: {counter}")
    counter=counter+1
    sleep(1)