import socket
BUFFER_SIZE = 1024
DEFAULT_PORT = 10000

class ElementInfo:
    def __init__(self, id: int, ip: str):
        self.id = id
        self.ip = ip
        self.port = DEFAULT_PORT + 100*self.id
        self.timestamp = None
        self.confirmation = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

cluster1 = ElementInfo(1, "127.0.0.1")
cluster2 = ElementInfo(2, "127.0.0.1")
cluster3 = ElementInfo(3, "127.0.0.1")
cluster4 = ElementInfo(4, "127.0.0.1")
cluster5 = ElementInfo(5, "127.0.0.1")