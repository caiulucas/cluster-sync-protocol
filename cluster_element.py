import socket
import json
import threading
import time
import random

from constants import BUFFER_SIZE, DEFAULT_PORT, ElementInfo
from constants import cluster1, cluster2, cluster3, cluster4, cluster5
from constants import store_list

class ClusterElement:
    def __init__(self, id:int, ip: int, client_id:int, client_ip: str):
        self.id = id
        self.ip = ip
        self.listen_port = DEFAULT_PORT + self.id * 100
        self.cluster_list = [cluster1, cluster2, cluster3, cluster4, cluster5]
        del self.cluster_list[self.id - 1]
        self.timestamp = None
        self.client_info = ClientInfo(client_id, client_ip, DEFAULT_PORT + 111 * client_id)
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.all_threads = []
        self.stop_event = threading.Event()

        self.waiting_store = True
        # self.store = None

    def check_stop_cluster(self):
        stop = input()
        if stop == "":
            self.stop_event.set()
            print("\n\n\n\n\nSTOP EVENT ENVIADO \n\n\n\n\n")

    def connect_to_all_clusters(self):
        for cluster in self.cluster_list:
            t = threading.Thread(target=self.connect_to_cluster, args=(cluster,))
            t.daemon = True
            t.start()

    def connect_to_cluster(self, cluster:ElementInfo):
        while not self.stop_event.is_set():
            try:
                # print(f"Tentando connectar cluster {self.id} ao cluster {cluster.id}")
                cluster.socket.connect((cluster.ip, cluster.port))
                
                message = f"CLUSTER {self.id} CONECTOU AO CLUSTER {cluster.id}"
                border_length = len(message) + 4
                print(f"\n\n+{'-' * border_length}+\n| {message} |\n+{'-' * border_length}+")

                break
            except socket.error as e:
                # print(f"Erro ao conectar cluster {self.id} ao cluster {cluster.id}: {e}")
                time.sleep(10)

    def listen_clusters(self):
        self.listen_socket.bind((self.ip, self.listen_port))
        self.listen_socket.listen(7)
        # print(f"Escutando {self.listen_port}")


        while not self.stop_event.is_set():
            conn, addr = self.listen_socket.accept()
            t_conn = threading.Thread(target=self.cluster_message, args=(conn, addr))
            t_conn.daemon = True
            t_conn.start()

    def cluster_message(self, conn, addr):
        while not self.stop_event.is_set():
            request = conn.recv(BUFFER_SIZE)

            if(request.decode() == ''):
                break
            # print(request.decode())
            t_handler_message = threading.Thread(target=self.cluster_message_handler, args=(request,))
            t_handler_message.daemon = True
            t_handler_message.start()

        # conn.sendall(json.dumps({"status": "commited"}).encode())

                    
    def cluster_message_handler(self, request):
        
        if(request.decode() == ''):
            return
        # print(f"message:|{request}|")
        message = json.loads(request.decode())

        cluster_message_id = message.get('id')
        command = message.get('command')

        if(command == 'update_timestamp'):
            for cluster in self.cluster_list:
                if cluster.id == cluster_message_id:
                    old_timestamp = cluster.timestamp
                    cluster.timestamp = message.get('timestamp')

                    # print(f"Update no timestamp do cluster{cluster.id} de {old_timestamp} para {cluster.timestamp}")
                    break

        elif (command == 'request_priority'):
            print(f"\033[35mCluster{cluster_message_id} requisitou prioridade\033[0m\n")
            
            cluster = None

            for c in self.cluster_list:
                if c.id == cluster_message_id:
                    cluster = c
                    break

            if self.timestamp == None:
                # print(f"Timestamp None")
                self.send_ok(cluster_message_id)
            elif cluster.timestamp < self.timestamp:
                # print(f"timestamp cluster{cluster_message_id} < cluster{self.id}")
                self.send_ok(cluster_message_id)
            elif cluster.timestamp == self.timestamp:
                # print(f"Timestamp IGUAL")
                if  self.id > cluster.id:
                    self.send_ok(cluster_message_id)
                else:
                    self.wait_to_send_ok(cluster_message_id)
            else:
                # print(f"else, timestamp cluster{cluster_message_id} > cluster{self.id}")
                self.wait_to_send_ok(cluster_message_id)

        elif (command == 'ok'):
            # print(f"Recebido OK do cluster{cluster_message_id}")
            for cluster in self.cluster_list:
                if cluster.id == cluster_message_id:
                    cluster.confirmation = True
                    break;
        
        elif (command == 'delete_timestamp'):
            # print(f"Recebido comando delete_timestamp do cluster{cluster_message_id}")
            for cluster in self.cluster_list:
                if cluster.id == cluster_message_id:
                    cluster.timestamp = None
                    break

        elif (command == 'store_confirmation'):
            print(f"\033[35mStore{cluster_message_id} Confirmou acesso\033[0m\n")
            self.waiting_store = False
            
    def send_ok(self, cluster_message_id):

        ok_json = {
            "id": self.id,
            "command": "ok"
        }

        content = json.dumps(ok_json)

        for cluster in self.cluster_list:
            if cluster.id == cluster_message_id:
                cluster.socket.sendall(content.encode())
                # print(f"Sending ok to cluster{cluster.id}\n")
                break
        return
    

    def wait_to_send_ok(self, cluster_message_id):
        # print(f"Esperando para mandar ok para o cluster {cluster_message_id}")
        while not self.stop_event.is_set():
            
            if(self.timestamp == None):
                self.send_ok(cluster_message_id)
                break
    
    def delete_all_timestamp(self):
        self.timestamp = None
        # print(f"Enviando delete timestamp para todos os clusters")
        threads = []

        for cluster in self.cluster_list:
            t = threading.Thread(target=self.delete_timestamp, args=(cluster,))
            t.daemon = True
            t.start()
            threads.append(t)

            for t in threads:
                t.join()

    def delete_timestamp(self, cluster):

        delete_json = {
            "id": self.id,
            "command": "delete_timestamp",
        }
        content = json.dumps(delete_json)
        cluster.socket.sendall(content.encode())


    def run(self):

        threads = []

        t = threading.Thread(target=self.listen_clusters)
        t.daemon = True
        t.start()
        threads.append(t)

        self.connect_to_all_clusters()

        t_client = threading.Thread(target=self.listen_client)
        t_client.daemon = True
        t_client.start()
        threads.append(t_client)
        

        t_stop_signal = threading.Thread(target=self.check_stop_cluster)
        t_stop_signal.daemon = True
        t_stop_signal.start()
        threads.append(t_stop_signal)

        # Espera todas as threads 
        for t in threads:
            t.join()

    def listen_client(self):
        self.client_info.socket.bind((self.ip, self.client_info.port))
        self.client_info.socket.listen()
        print(f"Escutando cliente na porta {self.client_info.port}")
        while not self.stop_event.is_set():
            conn, addr = self.client_info.socket.accept()
            with conn:
                print(f"ClusterElement {self.id} conectado com o cliente {addr}")

                while not self.stop_event.is_set():
                    request = conn.recv(BUFFER_SIZE)
                    if not request:
                        break

                    try:
                        content = json.loads(request.decode())
                        
                        self.timestamp = content.get('timestamp')
                        print(f"TimeStamp:{self.timestamp} recebido do cliente.")

                        t_send_all = threading.Thread(target=self.client_request_handler)
                        t_send_all.daemon = True
                        t_send_all.start()
                    
                        t_send_all.join()

                        conn.sendall(json.dumps({"status": "commited"}).encode())
                    except:
                        conn.close()
                        return
                

    def client_request_handler(self):

        try:
            print("Enviando timestamp")
            self.send_all_timestamp()
            print("Requisitando prioridade")
            self.request_priority()

            print("Esperando prioridade")
            self.waiting_priority()

            self.access_critical_zone()

            print("Deletando timestamp")
            self.delete_all_timestamp()
            
        except Exception as e:
            print(f"Ocorreu um erro: {e}")

    def request_priority(self):
        # print("Pedindo prioridade para os clusters.")
        for cluster in self.cluster_list:
            threads = []
            t = threading.Thread(target=self.send_priority, args=(cluster,))
            t.daemon = True
            t.start()
            threads.append(t)

            for t in threads:
                t.join()


    def send_priority(self, cluster):
        priority_json = {
            "id": self.id,
            "command": "request_priority"
        }
        content = json.dumps(priority_json)
        cluster.socket.sendall(content.encode())
        # print(f"Asking for priority to cluster{cluster.id}")


    def waiting_priority(self):
        print(f"Esperando confirmação para acessar o recurso R.")
        while not self.stop_event.is_set():
            ok_numbers = 0

            for cluster in self.cluster_list:
                if cluster.confirmation == True:
                    ok_numbers += 1

            if(ok_numbers == len(self.cluster_list)):
                print(f"Confirmação recebida para acessar o recurso R.")                
                break
        
    def delete_confimations(self):
        for cluster in self.cluster_list:
            cluster.confirmation = False
    
    def access_critical_zone(self):
        print(f"\033[31mConectando a store para acessar recurso R.\033[0m")
        self.delete_confimations()
        # Gera um número aleatório entre 0.2 e 1 segundo

        timeout = 10

        while not self.stop_event.is_set():
            count = 0
            try: 
                store = self.connect_to_store()
                store.socket.settimeout(timeout)

                if store:
                    self.send_request_to_store(store)

                    while(self.waiting_store):
                        time.sleep(0.5)
                        count += 1
                        print("No loop.")
                        if count >= timeout:
                            store.connection = False
                            store.socket.shutdown(socket.SHUT_RDWR)
                            store.socket.close()
                            store.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            raise Exception('Timeout. Número máximo de tentativas atingido')



                    store.connection = False
                    store.socket.shutdown(socket.SHUT_RDWR)
                    store.socket.close()
                    store.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    
                    print(f"\033[31mFinalizando acesso ao recurso R\033[0m")
                    

                    self.waiting_store = True

                    break

    
                else:
                    print(f"\033[31mErro ao acessar store {store.id}\033[0m")

            except Exception as e:
                print(f"\033[33mTimeout ao acessar store, tentando novamente...\033[0m")
                

    def send_all_timestamp(self):
        print("Sending timestamp to all clusters.")
        threads = []
        for cluster in self.cluster_list:
            if cluster.socket:
                #     #            Enviar mensagem
                # send_thread = threading.Thread(target=self.send_timestamp, args=(cluster,))
                # send_thread.start()
                # threads.append(send_thread)
                self.send_timestamp(cluster)
            else:
                # print(f"Cluster {cluster.id} ainda não está conectado.")
                pass

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
        # print(f"Sending the timestamp json:\n to cluster: {cluster.id} on port {cluster.port}\n")


    def connect_to_store(self):
        store_number = random.randint(0, 2)
        
        for i in range(3):
            store = store_list[(store_number + i) % len(store_list)]
            try:
                store.socket.connect((store.ip, store.port))
                # self.store = store
                store.connection = True
                store.last_ping = time.time()
                message = f"\033[31mCLUSTER {self.id} CONECTOU AO STORE {store.id}\033[0m"
                border_length = len(message) + 4
                print(f"\n\n+{'-' * border_length}+\n| {message} |\n+{'-' * border_length}+")
                return store

            except socket.error as e:
                print(f"\033[31mErro ao conectar cluster {self.id} ao store {store.id}: {e}\033[0m")
                time.sleep(1)


    def send_request_to_store(self, store):
        print(f"\033[31mEnviando requisição a store {store.id}.\033[0m")

        request_json = {
            "id": self.id,
            "command": "request_access_critical_zone",
            "timestamp": self.timestamp,
            "id_client": self.client_info.id
        }

        try:
            content = json.dumps(request_json)
            if store.connection:
                store.socket.sendall(content.encode())
            print(f"\033[31mRequisitando acesso a store\033[0m")
        except Exception as e:
            print("Erro ao requsitar acesso a zona crítica.\n")

class ClientInfo:
    def __init__(self, id:int, ip: str, port: int):
        self.id = id
        self.ip = ip
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


