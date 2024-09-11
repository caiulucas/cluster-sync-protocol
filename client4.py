from client import Client
from cluster_element import DEFAULT_PORT


client4 = Client(
        ip = '127.0.0.1',
        port = DEFAULT_PORT + 4,
        id = 4
    )

client4()