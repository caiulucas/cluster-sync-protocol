from cluster_element import ClusterElement, clusters


if __name__ == "__main__":
    #hello world


    cluster3 = ClusterElement(
        clusters['cluster3'].get('id'),
        clusters['cluster3'].get('ip'),
        '127.0.0.1',
        6002
    )

    cluster3.connect_clusters()