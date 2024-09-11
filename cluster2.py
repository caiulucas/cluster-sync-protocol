from cluster_element import ClusterElement
from constants import DEFAULT_PORT

if __name__ == "__main__":

    cluster = ClusterElement(2, "127.0.0.1", "127.0.0.1", DEFAULT_PORT)
    cluster.run()
