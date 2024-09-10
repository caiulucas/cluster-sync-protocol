from cluster_element import ClusterElement, clusters


if __name__ == "__main__":
    #hello world


    cluster5 = ClusterElement(
        clusters['cluster5'].get('id'),
        clusters['cluster5'].get('ip'),
        '127.0.0.1',
        6002
    )

    cluster5.connect_clusters()