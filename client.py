import random
import socket
import json
import time

BUFFER_SIZE = 1024

class Client:
    def __init__(self, ip: str, port: int, id: int):
        self.id = id
        self.ip = ip
        self.port = port
        self.value = random.randint(10, 50)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.request_number = random.randint(10, 50)

        self.connect()

    def connect(self):
        try:
            self.socket.connect((self.ip, self.port))
            print(f"Conectado ao servidor {self.ip}:{self.port}")
        except socket.error as e:
            print(f"Erro ao conectar: {e}")

    def send_request(self, connection):
        
        content = {
            "id": self.id,
            "timestamp": time.time()
        }
        content = json.dumps(content)
        self.socket.sendall(content.encode())
        print(f"Sending the json:\n{content}\n to\nip: {self.ip}\nport:{self.port}")

    def await_reponse(self, connection):
        response =  connection.recv(BUFFER_SIZE).decode()
        content = json.loads(response)
        print(f"Received the json:\n{content}")

    def sleep(self):
        time.sleep(random.uniform(1, 5))
    
    def close(self):
        self.socket.close()
        print("Conexão encerrada.")

    def __call__(self):
          
        with self.socket as connection:
            for _ in range(self.request_number):
                self.send_request(connection)
                self.await_reponse(connection)
                self.sleep(connection)
            connection.close()

    def __repr__(self):
        return f'Client(value={self.value}, ip={self.ip}, port={self.port})'
    
if __name__ == "__main__":

    client1 = Client(
        ip = '127.0.0.1',
        port = 6000,
        id = 1
    )
    client1()
