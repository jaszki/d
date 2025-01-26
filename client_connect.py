from src.redis import server

client = server.Client()
client.set("key1", 2)