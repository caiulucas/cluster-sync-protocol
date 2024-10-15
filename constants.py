import socket
BUFFER_SIZE = 1024
DEFAULT_PORT = 14000

class ElementInfo:
    def __init__(self, id: int, ip: str):
        self.id = id
        self.ip = ip
        self.connection = False
        self.port = DEFAULT_PORT + 100*self.id
        self.timestamp = None
        self.confirmation = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

cluster1 = ElementInfo(1, "127.0.0.1")
cluster2 = ElementInfo(2, "127.0.0.1")
cluster3 = ElementInfo(3, "127.0.0.1")
cluster4 = ElementInfo(4, "127.0.0.1")
cluster5 = ElementInfo(5, "127.0.0.1")

cluster_list = [cluster1, cluster2, cluster3, cluster4, cluster5]

class ClusterStoreInfo:
    def __init__(self, id:int, ip: str):
        self.id = id
        self.ip = ip
        self.timestamp = None
        self.port = DEFAULT_PORT + (10 - self.id) * 100
        self.primary = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection = False
        self.last_ping = None

store1 =  ClusterStoreInfo(1, "127.0.0.1")
store2 =  ClusterStoreInfo(2, "127.0.0.1")
store3 =  ClusterStoreInfo(3, "127.0.0.1")

store_list = [store1, store2, store3]