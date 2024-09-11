from client import Client
from cluster_element import ClusterElement
import threading

def createCluster(id: int):
    cluster = ClusterElement(1, "127.0.0.1",id, "127.0.0.1")
    cluster.run()
    print(f"Rodei o cluster")

if __name__ == "__main__":

    threads = []

    for i in range(1,6):
        print("for ")
        t = threading.Thread(target=createCluster, args=(i,))
        t.start()

    for t in threads:
        t.join()



