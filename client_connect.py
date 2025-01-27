from time import sleep

from src.redis import server


client = server.Client()
sleep(10)
print(client.set("key1", 2))