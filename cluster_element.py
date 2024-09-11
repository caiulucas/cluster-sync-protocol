import socket
import json
import threading
import time

from constants import BUFFER_SIZE, DEFAULT_PORT, CLUSTER_NUMBER, ElementInfo
from constants import cluster1, cluster2, cluster3, cluster4, cluster5


class ClusterElement:
    def __init__(self, id:int, ip: int, client_ip: str, client_port: int):
        self.id = id
        self.ip = ip
        self.listen_port = DEFAULT_PORT + self.id * 100
        self.cluster_list = [cluster1, cluster2, cluster3, cluster4, cluster5]
        del self.cluster_list[self.id - 1]

        self.client_info = ClientInfo(1, client_ip, client_port)
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    def connect_to_all_clusters(self):
        for cluster in self.cluster_list:
            t = threading.Thread(target=self.connect_to_cluster, args=(cluster,))
            t.start()

    def connect_to_cluster(self, cluster:ElementInfo):
        while True:
            try:
                print(f"Tentando connectar cluster {self.id} ao cluster {cluster.id}")
                cluster.socket.connect((cluster.ip, cluster.port))
                print(f"CLUSTER {self.id} CONECTOU AO CLUSTER {cluster.id}")
                break
            except socket.error as e:
                print(f"Erro ao conectar cluster {self.id} ao cluster {cluster.id}: {e}")
                time.sleep(10)

    def listen_clusters(self):
        self.listen_socket.bind((self.ip, self.listen_port))
        self.listen_socket.listen()
        print(f"Escutando {self.listen_port}")

        while True:
                conn, addr = self.listen_socket.accept()
                with conn:
                    request = conn.recv(BUFFER_SIZE)
                    print(f"RECEBI ALGO AQUI")
    
    def run(self):

        threads = []

        t = threading.Thread(target=self.listen_clusters)
        t.start()
        threads.append(t)

        self.connect_to_all_clusters()

        # t = threading.Thread(target=self.listen_client)
        # t.start()
        # threads.append(t)
        
        # Espera todas as threads 
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
                    print(f"TimeStamp:{self.timestamp} recebido do cliente.")

                    self.client_request_handler()

                    # conn.sendall(json.dumps({"status": "commited"}).encode())
                

    def client_request_handler(self):
        print("request handler")
        self.send_all_timestamp()

    def send_all_timestamp(self):
        for cluster in self.cluster_list:
            self.send_timestamp(cluster)
    
    def send_timestamp(self, cluster):
        json = {
            "id": self.id,
            "timestamp": time.time()
        }
        content = json.dumps(json)
        cluster.socket.sendall(content.encode())
        print(f"Sending the json:\n{json}\n to cluster: {cluster.id}\n")




class ClientInfo:
    def __init__(self, id:int, ip: str, port: int):
        self.id = id
        self.ip = ip
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

