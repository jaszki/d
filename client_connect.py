from time import sleep

from src.redis import server


client = server.Client()

print(client.set("key1", 2))


sleep(10)