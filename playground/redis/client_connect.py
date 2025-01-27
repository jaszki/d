from time import sleep

from src.redis import simpledb


client = simpledb.Client()

print(client.set("key1", 2))


sleep(10)