import random
import socket
import threading
import json
import time
import os

from constants import BUFFER_SIZE, DEFAULT_PORT, ClusterStoreInfo
from constants import store1, store2, store3
from constants import cluster_list, store_list


class ClusterStore:
    def __init__(self, id:int, ip:str, primary = False):
        self.id = id
        self.ip = ip

        self.store_list = [store1, store2, store3]
        del self.store_list[self.id - 1]
        
        self.timestamp = time.time()
        self.primary = primary
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_port = DEFAULT_PORT + (10 - self.id) * 100
        self.stop_event = threading.Event()

        self.threads = []


    def start_cluster_socket(self):
        threads = []
        for cluster in cluster_list:
            t = threading.Thread(target=self.connect_to_cluster, args=(cluster,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
            
        print("\n\nTodos os Clusters Conectados\n\n")

    
    def connect_to_cluster(self, cluster):
        print(f"Conectando o cluster {cluster.id}")
        while not self.stop_event.is_set():
            try:
                cluster.socket.connect((cluster.ip, cluster.port))
                break
            except:
                time.sleep(3)

        print(f"Conectado o cluster {cluster.id}")
        

    def connect_stores(self):
        for store in self.store_list:
            t = threading.Thread(target=self.connect_store, args=(store,))
            t.daemon = True
            # print(f"Thread connect_store criado: {t}")
            self.threads.append(t)
            t.start()

    def connect_store(self, store: ClusterStoreInfo):
        time.sleep(3)
        try:
            while not self.stop_event.is_set():
                store.socket.connect((store.ip, store.port))
                store.connection = True
                store.last_ping = time.time()
                message = f"STORE {self.id} CONECTOU AO STORE {store.id}"
                border_length = len(message) + 4
                print(f"\n\n+{'-' * border_length}+\n| {message} |\n+{'-' * border_length}+")

                break


        except socket.error as e:
            print(f"Erro ao conectar store {self.id} ao store {store.id}: {e}")
            time.sleep(2)
    
           
    def listen_store(self):
        self.listen_socket.bind((self.ip, self.listen_port))
        self.listen_socket.listen(7)
        # print(f"Escutando {self.listen_port}")

        try:
            while not self.stop_event.is_set():
                conn, addr = self.listen_socket.accept()
                t_conn = threading.Thread(target=self.store_message, args=(conn, addr))
                # print(f"Thread t_conn criado: {t_conn}")
                t_conn.daemon = True
                self.threads.append(t_conn)
                t_conn.start()
        except:
            pass

    def store_message(self, conn, addr):
        try:
            while not self.stop_event.is_set():
                request = conn.recv(BUFFER_SIZE)

                if(request.decode() == ""):
                    conn.close()
                    break

                # print(request.decode())
                t_handler_message = threading.Thread(target=self.store_message_handler, args=(request,))
                t_handler_message.daemon = True
                # print(f"Thread t_handler_message criado: {t_handler_message}")
                # print("mensagem |" + request.decode()+"|")

                self.threads.append(t_handler_message)

                t_handler_message.start()
        except:
            pass

    def store_message_handler(self, request):
        try:
            message = json.loads(request.decode())
            id = message.get('id')
            command = message.get('command')

            if(command == 'ping'):
                for store in self.store_list:
                    if(store.id == id):
                        store.last_ping = time.time()

            elif (command == 'request_primary_info'):
                self.send_primary_info(id)

            elif(command == 'update_primary_info'):
                for store in self.store_list:
                    if(store.id == id):
                        store.primary = message.get('primary_info')
                        store.timestamp = message.get('timestamp')

            elif(command == 'request_access_critical_zone'):
                # print(f"Acesso requerido pelo cluster {id}")
                # self.send_cricital_zone_confimartion(id)
                t = threading.Thread(target=self.handler_request_critical_zone, args=(id, message.get("id_client"), message.get('timestamp')))
                # print(f"Thread t_handler_request_critical_zone criado: {t}")
                t.daemon = True
                t.start()
            
            elif(command == 'redirect'):
                
                print(f"\033[35mRecebido Redirecionamento da Store {id}.\033[0m\n")

                id_cluster = message.get('id_cluster')
                id_client = message.get('id_client')
                timestamp = message.get('timestamp')
                
                t = threading.Thread(target=self.handler_redirect, args=(id, id_cluster, id_client, timestamp))
                # print(f"Thread t_access_critical_zone criado: {t}")
                t.daesmon = True
                t.start()

            elif(command == 'confirm_critical_zone'):
                id_cluster = message.get('id_cluster')
                # print(f"Thread t_handler_send_cluster_confirmation: {t}")
                t = threading.Thread(target=self.handler_send_cluster_confirmation, args=(id_cluster,))
                t.daemon = True
                t.start()
            elif (command == 'redirect_access_confirmation'):
                id_cluster = message.get('id_cluster')
                id_client = message.get('id_client')
                timestamp = message.get('timestasmp')

                t = threading.Thread(target=self.handler_redirect_access_confirmation, args=(id_cluster, id_client, timestamp))
                t.daemon = True
                t.start()

            elif(command == 'write_file'):
                t = threading.Thread(target=self.handler_write_file, args=(message.get('log'),))
                t.daemon = True
                t.start()

        except Exception as e:
            pass
    
    def handler_write_file(self, log):
        self.write_file(log)

    def handler_redirect_access_confirmation(self, id_cluster, id_client, timestamp):
        self.handler_send_cluster_confirmation(id_cluster)

    def handler_redirect(self, id, id_cluster, id_client, timestamp):
        self.handler_access_critical_zone(id, id_cluster, id_client, timestamp)



    def handler_send_cluster_confirmation(self, id_cluster):
        cluster_confirmation_json = {
            "id": self.id,
            "command": "store_confirmation",
        }
        content = json.dumps(cluster_confirmation_json)

        try: 
            for cluster in cluster_list:
                if cluster.id == id_cluster:    
                    cluster.socket.sendall(content.encode())

        except Exception as e:
            print(f"Erro ao enviar confirm_cluster_access. Erro {e}")

    def handler_access_critical_zone(self, id_redirected_store, id_cluster, id_client, timestamp):
        
        self.access_critical_zone(id_redirected_store, id_cluster, id_client, timestamp)
        
        if(id_redirected_store == self.id):
            self.handler_send_cluster_confirmation(id_cluster)
        else:
            self.send_redirected_access_confirmation(id_redirected_store, id_cluster, id_client, timestamp)
    
    def send_redirected_access_confirmation(self, id_store, id_cluster, id_client, timestamp):
        
        print(f"\033[35mResolvido o redirect da store {id_store}. Devolvendo a confirmação.\033[0m")
        redirect_access_confirmation_json = {
            "id": self.id,
            "command": "redirect_access_confirmation",
            "id_client": id_client,
            "id_cluster": id_cluster,
            "timestamp": timestamp 
        }
        content = json.dumps(redirect_access_confirmation_json)

        for store in store_list:
            if store.id == id_store:
                store.socket.sendall(content.encode())
                break
    
    #  Mandando pra store primária resolver meu problema.
    def send_critical_zone_confirmation(self, id, id_cluster):
        critical_zone_confirmation_json = {
            "id": self.id,
            "command": "confirm_critical_zone",
            "id_cluster": id_cluster
        }

        for store in self.store_list:
            if store.id == id:
                try:
                    content = json.dumps(critical_zone_confirmation_json)
                    if store.connection:
                        store.socket.sendall(content.encode())
                except:
                    print("Erro ao enviar critical_zone_confimartion_json")
    
    def handler_request_critical_zone(self, id_cluster, id_client, timestamp):

        if self.primary:
            print(f"\033[35mRecebido um peido do cliente {id_client}.\033[0m")
            self.access_critical_zone(self.id, id_cluster, id_client, timestamp)
            self.handler_send_cluster_confirmation(id_cluster)

        else:
            print("Redirecionando para store principal")
            self.redirect_critical_zone_request(id_cluster, id_client, timestamp)

    def redirect_critical_zone_request(self, id_cluster, id_client, timestamp):
        for store in self.store_list:
            if store.primary:
                self.send_redirect_message(id_cluster, id_client, timestamp, store)

        
    
    def send_redirect_message(self, id_cluster, id_client, timestamp, store: ClusterStoreInfo):
        redirect_json = {
            "id": self.id,
            "command": "redirect",
            "id_cluster": id_cluster,
            "id_client": id_client,
            "timestamp": timestamp
        }
        
        try:
            content = json.dumps(redirect_json)
            store.socket.sendall(content.encode())
        except Exception as e:
            print("Erro ao redirecionar pedido.\n")

    def send_cricital_zone_confimartion(self, id_cluster):

        for cluster in cluster_list:
            if cluster.id == id_cluster:

                confirmation_json = {
                    "id": self.id,
                    "command": "store_confirmation",
                }
                try:
                    
                    cluster = None
                    for c in cluster_list:
                        if c.id == id_cluster:
                            cluster = c
                            break

                    content = json.dumps(confirmation_json)
                    cluster.socket.sendall(content.encode())
                    # cluster.socket.shutdown(socket.SHUT_RDWR)
                    # cluster.socket.close()
                    # cluster.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                except Exception as e:
                    print("Erro ao enviar ping.\n")

    # def connect_cluster(self, cluster_id):
    #     for cluster in cluster_list:
    #         if cluster.id == cluster_id:
    #             try:
    #                 cluster.socket.connect((cluster.ip, cluster.port))
    #                 # self.store = store
    #                 message = f"Store {self.id} CONECTOU AO CLUSTER {cluster.id}"
    #                 border_length = len(message) + 4
    #                 print(f"\n\n+{'-' * border_length}+\n| {message} |\n+{'-' * border_length}+")
    #                 return cluster
                
    #             except:
    #                 pass

    def run(self):

        t_start_cluster_socket = threading.Thread(target = self.start_cluster_socket)
        t_start_cluster_socket.start()
    

        t = threading.Thread(target=self.listen_store)
        self.threads.append(t)
        # print(f"Thread listen_store criado: {t}")
        t.daemon = True
        t.start()

        t_stop_signal = threading.Thread(target=self.check_stop_store)
        self.threads.append(t_stop_signal)
        t_stop_signal.daemon = True
        # print(f"Thread t_stop_signal criado: {t_stop_signal}")

        t_stop_signal.start()

        t_ping_all = threading.Thread(target=self.ping_all)
        self.threads.append(t_ping_all)
        t_ping_all.daemon = True
        t_ping_all.start()
        # print(f"Thread t_ping_all criado: {t_ping_all}")

        t_check_connections = threading.Thread(target=self.check_connections)
        self.threads.append(t_check_connections)
        t_check_connections.daemon = True
        t_check_connections.start()
        # print(f"Thread t_check_connections criado: {t_check_connections}")

        self.connect_stores()
        self.election()

        if(self.stop_event.is_set()):
            return
        
        while not self.stop_event.is_set():
            time.sleep(1)

    def ping(self, store:ClusterStoreInfo):
        ping_json = {
            "id": self.id,
            "command": "ping",
        }
        try:

            content = json.dumps(ping_json)
            store.socket.sendall(content.encode())
        except Exception as e:
            print("Erro ao enviar ping.\n")

    def request_primary_info(self, store):
        primary_json = {
            "id": self.id,
            "command": "request_primary_info",
            "timestamp": self.timestamp,
        }

        try:
            content = json.dumps(primary_json)
            if store.connection:
                store.socket.sendall(content.encode())
        
        except Exception as e:
            print("Erro ao requsitar primary_info.\n")

    def election(self):

        time.sleep(6)

        # print(f"Eleição começou")
        if (self.primary):
            # print("Eu sou o primário")
            return
        else:
            for store in self.store_list:
                if store.primary & store.connection:
                    # print("Achei uma store primaria e com conexão")
                    return
                
        for store in self.store_list:
            self.request_primary_info(store)

        time.sleep(3)

        for store in self.store_list:
            if store.primary:
                return;

        new_primary = None
        
        for store in self.store_list:

            if store.connection:

                if store.timestamp == None:
                    continue
                elif new_primary == None:
                    new_primary = store
                elif store.timestamp < new_primary.timestamp:
                    new_primary = store

        if new_primary == None:
            self.primary = True
            print(f"STORE {self.id} ELEITO COMO PRIMÁRIO")

        elif new_primary.timestamp < self.timestamp:
            new_primary.primary = True
            print(f"STORE {new_primary.id} ELEITO COMO PRIMÁRIO")
        else:
            self.primary = True
            print(f"STORE {self.id} ELEITO COMO PRIMÁRIO")

        # if self.id == 1:
            # self.shutdown(30)

    def send_primary_info(self, id):
        current_primary = None

        primary_json = {
            "id": self.id,
            "command": "update_primary_info",
            "timestamp": self.timestamp,
            "primary_info": self.primary
        }
        content = json.dumps(primary_json)

        for store in self.store_list:
            if (store.id == id):
                store.socket.sendall(content.encode())

    def shutdown(self, timer):
        t = threading.Thread(target=self.shutdown_handler, args=(timer,))
        # print(f"Thread shutdown_handler cirado: {t}")
        self.threads.append(t)
        t.start()


    def shutdown_handler(self,timer):
        print(f"Erro crítico, Desligando em {timer} segundos")
        time.sleep(timer)

        # Envia uma flag falando para todas as threads parando o loop
        self.stop_event.set()
        # Fecho todos os sockets em aberto

        self.listen_socket.close()

        for store in self.store_list:
            store.socket.close()

        for t in self.threads:
            # print(self.threads)
            # print("\n--------------------------\n")
            t.join()
        print(f"Desligando agora.")
        os._exit()
    
    def check_stop_store(self):
        stop = input()
        if stop == "":
            # Envia uma flag falando para todas as threads parando o loop
            self.stop_event.set()
            # Fecho todos os sockets em aberto

            self.listen_socket.close()

            for store in self.store_list:
                store.socket.close()

            for t in self.threads:
                # print(self.threads)
                # print("\n--------------------------\n")
                t.join()
            print(f"Desligando agora.")
            os._exit()

    def check_connections(self):
        time.sleep(5)

        timeout = 12
        while not self.stop_event.is_set():
            for store in self.store_list:
                if store.connection:
                    if time.time() - store.last_ping > timeout:
                        store.connection = False
                        print(f"Conexão perdida com a store {store.id}.")
                        if store.primary:
                            store.primary = False
                            self.election()
                    
            time.sleep(5)
    
   
    def ping_all(self):
        time.sleep(5)
        while not self.stop_event.is_set():
            for store in self.store_list:
                if store.connection:

                    t = threading.Thread(target=self.ping, args=(store,))
                    self.threads.append(t)
                    t.daemon = True
                    t.start()
            time.sleep(5)
    
    def access_critical_zone(self, id_store, id_cluster, id_client, timestamp):
        random_sleep = random.uniform(0.2, 1)
        
        log_string = f"Timestamp: {timestamp} | Cliente: {id_client} | Cluster: {id_cluster} | Store: {id_store}\n"
        
        print("\033[91m\nACESSANDO A CRITICAL ZONE\033[0m")
        self.write_file(log_string)
        print("\033[91mSAINDO DA CRITICAL ZONE\n\033[0m")

        self.send_write_file(log_string)


    def write_file(self, log_string):

        with open(f"logs/store{self.id}.log", 'a') as file:
            file.write(log_string)

        if(self.id == 1):
            self.shutdown(0)
    
    def send_write_file(self, log_string):
        write_file_json = {
            "id": self.id,
            "command": "write_file",
            "log": log_string
        }

        try:
            content = json.dumps(write_file_json)

            for store in store_list:
                if store.connection:
                    store.socket.sendall(content.encode())

        except:
            print("Erro ao enviar comando de write_file.\n")

                
