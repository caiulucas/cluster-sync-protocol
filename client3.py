from client import Client
from cluster_element import DEFAULT_PORT


client3 = Client(
        ip = '127.0.0.1',
        port = DEFAULT_PORT + 3,
        id = 3
    )

client3()