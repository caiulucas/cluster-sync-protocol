from client import Client
from cluster_element import DEFAULT_PORT


client2 = Client(
        ip = '127.0.0.1',
        port = DEFAULT_PORT + 2,
        id = 2
    )

client2()