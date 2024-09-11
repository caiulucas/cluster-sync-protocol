from cluster_element import ClusterElement
from constants import DEFAULT_PORT

if __name__ == "__main__":

    cluster = ClusterElement(3, "127.0.0.1", 3, "127.0.0.1")
    cluster.run()
