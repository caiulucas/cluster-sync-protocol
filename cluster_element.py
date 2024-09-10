import socket
import json
import threading


BUFFER_SIZE = 1024
DEFAULT_PORT = 14000
CLUSTER_NUMBER = 5

# 1: ->2(10102), ->3(10103), ->4(10104), ->5(10105)
# 2: <- 1(10102) -> 3(10203), ->4(10204), ->5(10205)
# 3: <- 1, <- 2, ->4, ->5
# 4: <- 1, <- 2, <- 3, -> 5
# 5: <-1, <-2, <-3, <-4, 

# 1: ->2
# 2: <- 1

# Configuração dos clusters
clusters = {
    "cluster1": {"id": 1, "ip": "127.0.0.1"},
    "cluster2": {"id": 2, "ip": "127.0.0.1"},
    "cluster3": {"id": 3, "ip": "127.0.0.1"},
    "cluster4": {"id": 4, "ip": "127.0.0.1", "port": DEFAULT_PORT + 3},
    "cluster5": {"id": 5, "ip": "127.0.0.1", "port": DEFAULT_PORT + 4}
}

class ClusterElement:
    def __init__(self, id: int, ip:str, client_ip: str, client_port: int):
        self.id = id
        self.ip = ip
        self.cluster_element_info_list = []
        self.client_info = ClientInfo(1, client_ip, client_port)
        self.timestamp = None
        self.can_commit = False

    def connect_to_cluster(self, id, clusters, connect=True):
        element_info = ElementInfo(
            id,
            clusters["cluster{}".format(id)].get('ip')
        )
        if connect:
            try:
                
                element_info.socket.connect((element_info.ip, self.get_port(element_info.id)))
                print(f"Conectado ao cluster {element_info.id}: porta {self.get_port(element_info.id)}")
            except socket.error as e:
                print(f"Erro ao conectar cluster {self.id} ao cluster {element_info.id}: {e}")
        else:
            # element_info.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            element_info.socket.bind((self.ip, self.get_port(element_info.id)))
            print(f"Escutando {self.get_port(element_info.id)}")
            element_info.socket.listen()
            while True:
                conn, addr = element_info.socket.accept()
                with conn:
                    print(f"ClusterElement {self.id} conectado com o cluster {addr}")
    
    def request_priority(self):
        return
    
    def update_others_timestamp(self):
        return
    
    def reponse_priority(self):
        

    def connect_clusters(self):

        threads = []

        for id in range(self.id + 1, CLUSTER_NUMBER + 1):
            t = threading.Thread(target=self.connect_to_cluster, args=(id, clusters))
            t.start()
            threads.append(t)
        for id in range(1, self.id):
            t = threading.Thread(target=self.connect_to_cluster, args=(id, clusters, False))
            t.start()
            threads.append(t)
            
            # Espera todas as threads finalizarem
        for t in threads:
            t.join()

    def listen_client(self):
        self.client_info.socket.bind((self.ip, self.client_info.port))
        self.client_info.socket.listen()

        while True:
            conn, addr = self.client_info.socket.accept()
            with conn:
                print(f"ClusterElement {self.id} conectado com o cliente {addr}")

                while True:
                    request = conn.recv(BUFFER_SIZE)
                    if not request:
                        break

                    content = json.loads(request.decode())
                    self.timestamp = content.get('timestamp')

                    while True:
                        if self.can_commit:
                            break

                    conn.sendall(json.dumps({"status": "commited"}).encode())
                    self.can_commit = False

    def get_port(self, id2) -> str:
        if(self.id < id2):
            port = DEFAULT_PORT + (self.id * 100) + id2
        else:
            port = DEFAULT_PORT + (id2 * 100) + self.id
        return port


        
class ClientInfo:
    def __init__(self, id:int, ip: str, port: int):
        self.id = id
        self.ip = ip
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        try:
            self.socket.connect((self.ip, self.port))
            print(f"Conectado ao servidor {self.ip}:{self.port}")
        except socket.error as e:
            print(f"Erro ao conectar: {e}")


class ElementInfo:
    def __init__(self, id: int, ip: str):
        self.id = id
        self.ip = ip
        self.timestamp = None
        self.confirmation = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # def connect(self):
    #     try:
    #         self.socket.connect((self.ip, self.port))
    #         print(f"Conectado ao servidor {self.ip}:{self.port}")
    #     except socket.error as e:
    #         print(f"Erro ao conectar: {e}")


if __name__ == "__main__":
    #hello world

    cluster5 = ClusterElement(
        clusters['cluster5'].get('id'),
        clusters['cluster5'].get('port'),
        clusters['cluster5'].get('ip'),
        '127.0.0.1',
        6005
    )

    cluster4 = ClusterElement(
        clusters['cluster4'].get('id'),
        clusters['cluster4'].get('port'),
        clusters['cluster4'].get('ip'),
        '127.0.0.1',
        6004
    )

    cluster3 = ClusterElement(
        clusters['cluster3'].get('id'),
        clusters['cluster3'].get('ip'),
        '127.0.0.1',
        6003
    )

    cluster2 = ClusterElement(
        clusters['cluster2'].get('id'),
        clusters['cluster2'].get('ip'),
        '127.0.0.1',
        6002
    )

    cluster1 = ClusterElement(
        clusters['cluster1'].get('id'),
        clusters['cluster1'].get('ip'),
        '127.0.0.1',
        6001
    )
    
    # Criação das threads para conectar clusters
    thread_cluster2 = threading.Thread(target=cluster2.connect_clusters)
    thread_cluster1 = threading.Thread(target=cluster1.connect_clusters)

    # Iniciando as threads
    thread_cluster2.start()
    thread_cluster1.start()

    # Aguardando o término das threads
    thread_cluster2.join()
    thread_cluster1.join()

    print("Conexões de cluster4 e cluster5 finalizadas.")
