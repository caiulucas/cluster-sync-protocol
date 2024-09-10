from cluster_element import ClusterElement, clusters



if __name__ == "__main__":
    #hello world


    cluster2 = ClusterElement(
        clusters['cluster2'].get('id'),
        clusters['cluster2'].get('ip'),
        '127.0.0.1',
        6002
    )

    cluster2.connect_clusters()