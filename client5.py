from client import Client
from cluster_element import DEFAULT_PORT


client5 = Client(
        ip = '127.0.0.1',
        port = DEFAULT_PORT + 5,
        id = 5
    )

client5()