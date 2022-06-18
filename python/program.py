from time import sleep
import RedisConnect

def onMsg(msg):
    print("msg python 1: "+ msg)

connect= RedisConnect.RedisConnect("localhost",6379,"",0)
consumer= RedisConnect.RedisConsummer(connect,"python1","dunp", onMsg)
consumer.Start()

while True:
    sleep(1)