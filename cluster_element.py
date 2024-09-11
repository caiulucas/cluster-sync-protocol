import socket
import json
import threading
import time

from constants import BUFFER_SIZE, DEFAULT_PORT, ElementInfo
from constants import cluster1, cluster2, cluster3, cluster4, cluster5


class ClusterElement:
    def __init__(self, id:int, ip: int, client_id:int, client_ip: str):
        self.id = id
        self.ip = ip
        self.listen_port = DEFAULT_PORT + self.id * 100
        self.cluster_list = [cluster1, cluster2, cluster3, cluster4, cluster5]
        del self.cluster_list[self.id - 1]

        self.client_info = ClientInfo(client_id, client_ip, DEFAULT_PORT + 111 * client_id)
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
                
                message = f"CLUSTER {self.id} CONECTOU AO CLUSTER {cluster.id}"
                border_length = len(message) + 4
                print(f"\n\n+{'-' * border_length}+\n| {message} |\n+{'-' * border_length}+")

                break
            except socket.error as e:
                print(f"Erro ao conectar cluster {self.id} ao cluster {cluster.id}: {e}")
                time.sleep(10)

    def listen_clusters(self):
        self.listen_socket.bind((self.ip, self.listen_port))
        self.listen_socket.listen(4)
        print(f"Escutando {self.listen_port}")

        while True:
            conn, addr = self.listen_socket.accept()
            t_conn = threading.Thread(target=self.cluster_message, args=(conn, addr))
            t_conn.start()

    def cluster_message(self, conn, addr):
        request = conn.recv(BUFFER_SIZE)
        
        print(request.decode())
        t_handler_message = threading.Thread(target=self.cluster_message_handler, args=(request,))
        t_handler_message.start()

        conn.sendall(json.dumps({"status": "commited"}).encode())

                    
    def cluster_message_handler(self, request):
        message = json.loads(request.decode())
        cluster_message_id = message.get('id')
        command = message.get('command')

        print(f"Comando '{command}' recebido do cluster{cluster_message_id}")
        if(command == 'update_timestamp'):
            for cluster in self.cluster_list:
                if cluster.id == cluster_message_id:
                    old_timestamp = cluster.timestamp
                    cluster.timestamp = message.get('timestamp')

                    print(f"Update no timestamp do cluster{cluster.id} de {old_timestamp} para {cluster.timestamp}")
                    break

        if command == 'request_priority':
            print(f"Cluster{cluster_message_id} requisitou prioridade")

            if self.timestamp == None:
                self.send_ok(cluster_message_id)
            elif cluster.timestamp < self.timestamp:
                self.send_ok(cluster_message_id)
            elif cluster.timestamp == self.timestamp:
                if  self.id > cluster.id:
                    self.send_ok(cluster_message_id)
                else:
                    self.wait_to_send_ok(cluster_message_id)
            else:
                self.wait_to_send_ok(cluster_message_id)

        if command == 'ok':
            for cluster in self.cluster_list:
                if cluster.id == cluster_message_id:
                    print(f"Recebido OK do cluster{cluster.id}")
                    cluster.confirmation = True
                    break;
        
        if command == 'delete_timestamp':
            for cluster in self.cluster_list:
                if cluster.id == cluster_message_id:
                    cluster.timestamp = None
                    break
            
    def send_ok(self, cluster_message_id):

        ok_json = {
            "id": self.id,
            "command": "ok"
        }

        content = json.dumps(ok_json)

        for cluster in self.cluster_list:
            if cluster.id == cluster_message_id:
                cluster.socket.sendall(content.encode())
                break
        return
    

    def wait_to_send_ok(self, cluster_message_id):
        while True:
            # ok_numbers = 0

            # for cluster in self.cluster_list:
            #     if cluster.confirmation == True:
            #         ok_numbers += 1

            # if(ok_numbers == len(self.cluster_list)):
            if(self.timestamp == None):
                self.send_ok(cluster_message_id)
    
    def delete_timestamp(self):
        delete_json = {
            "id": self.id,
            "command": "delete_timestamp",
        }
        content = json.dumps(delete_json)

        for cluster in self.cluster_list:
            cluster.socket.sendall(content)

    def run(self):

        threads = []

        t = threading.Thread(target=self.listen_clusters)
        t.start()
        threads.append(t)

        self.connect_to_all_clusters()

        t_client = threading.Thread(target=self.listen_client)
        t_client.start()
        threads.append(t_client)
        
        

        # Espera todas as threads 
        for t in threads:
            t.join()

    def listen_client(self):
        self.client_info.socket.bind((self.ip, self.client_info.port))
        self.client_info.socket.listen()
        print(f"Escutando cliente na porta {self.client_info.port}")
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

                    t_send_all = threading.Thread(target=self.client_request_handler)
                    t_send_all.start()
                    
                    t_send_all.join()



                    while True:
                        print("Esperando resposta.")
                        time.sleep(20)
                    # conn.sendall(json.dumps({"status": "commited"}).encode())
                

    def client_request_handler(self):
        print("request handler")
        print("Sending timestamp to all")
        self.send_all_timestamp()
        # print("Pedindo prioridade")
        # self.request_priority()

        while True:
            print(f"Esperando para acessar recurso R")
            time.sleep(20)
        #pode acessar?
        #acessa o recurso R

        self.delete_timestamp()
        self.timestamp = None

    def request_priority(self):
        
        priority_json = {
            "id": self.id,
            "command": "request_priority"
        }

        content = json.dumps(priority_json)
        for cluster in self.cluster_list:
            print(f"-------------\nEnviando priority json to cluster{cluster.id}\n-------------")
            try:
                cluster.socket.sendall(content.encode())
                print(f"Sending the timestamp json:\n to cluster: {cluster.id}\n")
            except socket.error as e:
                print(f"Erro ao enviar timestamp para o cluster {cluster.id}: {e}")

    def send_all_timestamp(self):
        threads = []
        for cluster in self.cluster_list:
            if cluster.socket:
                #     #            Enviar mensagem
                # send_thread = threading.Thread(target=self.send_timestamp, args=(cluster,))
                # send_thread.start()
                # threads.append(send_thread)
                self.send_timestamp(cluster)
            else:
                print(f"Cluster {cluster.id} ainda não está conectado.")

        for t in threads:
            t.join()

    
    def send_timestamp(self, cluster):
        timestamp_json = {
            "id": self.id,
            "command": "update_timestamp",
            "timestamp": self.timestamp
        }
        content = json.dumps(timestamp_json)
        cluster.socket.sendall(content.encode())
        print(f"Sending the timestamp json:\n to cluster: {cluster.id} on port {cluster.port}\n")




class ClientInfo:
    def __init__(self, id:int, ip: str, port: int):
        self.id = id
        self.ip = ip
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

