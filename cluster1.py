from cluster_element import ClusterElement
from constants import DEFAULT_PORT

if __name__ == "__main__":

    cluster = ClusterElement(1, "127.0.0.1",1, "127.0.0.1")
    print(f"Port: {cluster.listen_port}")
    cluster.run()
