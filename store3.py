from cluster_store import ClusterStore
from constants import DEFAULT_PORT

if __name__ == "__main__":

    store = ClusterStore(3, "127.0.0.1")
    store.run()