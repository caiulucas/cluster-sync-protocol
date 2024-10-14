import random
import socket
import threading
import json
import time
import os

from constants import BUFFER_SIZE, DEFAULT_PORT, ClusterStoreInfo
from constants import store1, store2, store3


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
        self.listen_socket.listen(2)
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

                # print(request.decode())
                t_handler_message = threading.Thread(target=self.store_message_handler, args=(request,))
                t_handler_message.daemon = True
                # print("mensagem |" + request.decode()+"|")
                if(request.decode() == ""):
                    # print("mensagem vazia")
                    break
                # print(f"Thread t_handler_message criado: {t_handler_message}")

                self.threads.append(t_handler_message)

                t_handler_message.start()
        except:
            pass

    def store_message_handler(self, request):
        try:

            message = json.loads(request.decode())
            store_message_id = message.get('id')
            command = message.get('command')

            if(command == 'ping'):
                for store in self.store_list:
                    if(store.id == store_message_id):
                        store.last_ping = time.time()

            if (command == 'request_primary_info'):
                self.send_primary_info(store_message_id)

            if(command == 'update_primary_info'):
                for store in self.store_list:
                    if(store.id == store_message_id):
                        store.primary = message.get('primary_info')
                        store.timestamp = message.get('timestamp')
        except Exception as e:
            pass

    def run(self):

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
        os._exit(1)
    
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