from cluster_element import ClusterElement, clusters



if __name__ == "__main__":
    #hello world


    cluster1 = ClusterElement(
        clusters['cluster1'].get('id'),
        clusters['cluster1'].get('ip'),
        '127.0.0.1',
        6001
    )
    
    cluster1.connect_clusters()