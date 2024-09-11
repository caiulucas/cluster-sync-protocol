import socket
import json
import threading

from constants import BUFFER_SIZE, DEFAULT_PORT, CLUSTER_NUMBER, clusters



# 1: ->2(10102), ->3(10103), ->4(10104), ->5(10105)
# 2: <- 1(10102) -> 3(10203), ->4(10204), ->5(10205)
# 3: <- 1, <- 2, ->4, ->5
# 4: <- 1, <- 2, <- 3, -> 5
# 5: <-1, <-2, <-3, <-4, 

# 1: ->2
# 2: <- 1



class ClusterElement:
    def __init__(self, id: int, ip:str, client_ip: str, client_port: int):
        self.id = id
        self.ip = ip
        self.cluster_element_info_list = []
        self.client_info = ClientInfo(1, client_ip, client_port)
        self.timestamp = None
        self.can_commit = False
        self.client_requests = []

    def connect_to_cluster(self, id, clusters, connect=True):
        element_info = ElementInfo(
            id,
            clusters["cluster{}".format(id)].get('ip')
        )
        if connect:
            try:
                element_info.socket.connect((element_info.ip, self.get_port(element_info.id)))
                print(f"Conectado ao cluster {element_info.id}: porta {self.get_port(element_info.id)}")
                self.cluster_element_info_list.append(element_info)
                # t = threading.Thread(target=self.await_reponse, args=(element_info,))
                # t.start()
                
                while True:
                    
                    response = element_info.socket.recv(BUFFER_SIZE)
                    content = json.loads(response.decode)
                    self.process_response_content(content, element_info)

                    if element_info.socket == None:
                        break;
                
                    
                  
            except socket.error as e:
                print(f"Erro ao conectar cluster {self.id} ao cluster {element_info.id}: {e}")
        else:
            element_info.socket.bind((self.ip, self.get_port(element_info.id)))
            self.cluster_element_info_list.append(element_info)
            print(f"Escutando {self.get_port(element_info.id)}")
            element_info.socket.listen()
            while True:
                conn, addr = element_info.socket.accept()
                with conn:
                    print(f"ClusterElement {self.id} conectado com o cluster {addr}")

                    response =  conn.recv(BUFFER_SIZE)
                    content = json.loads(response.decode())

                    self.process_response_content(content, element_info)

                    if conn == None:
                        break


    def send_timestamp(self, conn):
        conn.sendall(json.dumps({"id": self.id, "status": "update_timestamp", "timestamp": self.timestamp}).encode())
    
    def await_reponse(self, element_info):
        response =  element_info.socket.recv(BUFFER_SIZE).decode()
        content = json.loads(response)

        self.process_response_content(content, element_info)

        if(self.client_info.socket == None):
            return
            # Receive data
            data = self.client_info.socket.recv(1024)
            if data == b'':  # Connection is closed
                return
    
    def process_response_content(self, content, element_info):
        
        if(content.get('status') == 'update_timestamp'):
            element_info.timestamp = content.get('timestamp')
        
        

    # def request_priority(self):

    #     return
    
    # def update_others_timestamp(self):
    #     return
    
    # def reponse_priority(self):
    #     return

    def connect_clusters(self):

        cluster_thread = []
        for id in range(self.id + 1, CLUSTER_NUMBER + 1):
            t = threading.Thread(target=self.connect_to_cluster, args=(id, clusters))
            t.start()
            cluster_thread.append(t)
        for id in range(1, self.id):
            t = threading.Thread(target=self.connect_to_cluster, args=(id, clusters, False))
            t.start()
            cluster_thread.append(t)
            
            # Espera todas as threads 
            for t in cluster_thread:
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

                    self.resolve_request()

                    print("resolvido")
                    conn.sendall(json.dumps({"status": "commited"}).encode())
                    
                    self.can_commit = False

    def resolve_request(self):
        for element in self.cluster_element_info_list:
            if element.id > self.id:
                # conexão "ativa"
                self.send_timestamp(element.socket)
                print(f"TimeStamp: {self.timestamp} enviado ao cluster {element.id}")

            else:
                # conexão "passiva"
                conn, addr = element.socket.accept()
                with conn:
                    print("Conn")
                    self.send_timestamp(conn)
                    print(f"TimeStamp: {self.timestamp} enviado ao cluster {element.id}")

        while True:
            i = 0

    def get_port(self, id2) -> str:
        if(self.id < id2):
            port = DEFAULT_PORT + (self.id * 100) + id2
        else:
            port = DEFAULT_PORT + (id2 * 100) + self.id
        return port

    def run(self):
        threads = []

        thread = threading.Thread(target=self.listen_client, args=())
        thread.start()
        threads.append(thread)

        thread = threading.Thread(target=self.connect_clusters, args=())
        thread.start()
        threads.append(thread)

        # Espera as threads finalizarem
        for t in threads:
            t.join()


        
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
