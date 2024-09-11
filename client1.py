from client import Client
from constants import DEFAULT_PORT


client1 = Client(
        ip = '127.0.0.1',
        port = DEFAULT_PORT + 1,
        id = 1
    )

client1()