from cluster_element import ClusterElement, clusters


if __name__ == "__main__":
    #hello world


    cluster4 = ClusterElement(
        clusters['cluster4'].get('id'),
        clusters['cluster4'].get('ip'),
        '127.0.0.1',
        6002
    )

    cluster4.connect_clusters()