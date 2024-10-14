import random
import socket
import json
import time
from constants import BUFFER_SIZE, DEFAULT_PORT


class Client:
    def __init__(self, ip: str, port, id: int):
        self.id = id
        self.ip = ip
        self.port = DEFAULT_PORT + 111 * self.id
        self.value = random.randint(10, 50)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.request_number = random.randint(10, 50)

        self.connect()

    def connect(self):
        try:
            print(f"Tentando conectar na porta {self.port} do cluster\n")
            self.socket.connect((self.ip, self.port))
            print(f"Conectado ao servidor {self.ip}:{self.port}")
            time.sleep(10)
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
        while True:
            try:
                response =  connection.recv(BUFFER_SIZE).decode()
                if not response:  # Caso a resposta esteja vazia
                    raise ValueError("Resposta vazia recebida")
                content = json.loads(response)
                
                print(f"Received the json:\n{content}")

                if(content.get('status') == 'commited'):
                    break

            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON da resposta do cluster {self.id}: {e}")
                break
            except socket.error as e:
                print(f"Erro ao receber resposta do cluster {self.id}: {e}")
                break
            except ValueError as e:
                print(f"Value Error: {e}")
                break



    def sleep(self):
        print("Dormindo")
        time.sleep(random.uniform(1, 5))
        print("Acordei")

    def close(self):
        self.socket.close()
        print("Conexão encerrada.")

    def __call__(self):
          
        with self.socket as connection:
            for i in range(self.request_number):
                print(f"--------------\nPedido {i + 1}.")
                self.send_request(connection)
                self.await_reponse(connection)
                self.sleep()
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
