from multiprocessing import Process
from node import RaftNode

def start_node(node_id, peers):
    node = RaftNode(node_id, peers)
    node.run()

if __name__ == '__main__':
    num_nodes = 3
    processes = []

    for i in range(num_nodes):
        peers = [j for j in range(num_nodes) if j != i]
        p = Process(target=start_node, args=(i, peers))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
