from cluster_store import ClusterStore
from constants import DEFAULT_PORT

if __name__ == "__main__":

    store = ClusterStore(1, "127.0.0.1")
    # store.shutdown(40)
    store.run()
