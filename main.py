from multiprocessing import Process, Queue
from node import RaftNode
import time

def start_node(node_id, peers, message_queues):
    node = RaftNode(node_id, peers, message_queues)
    node.run()

if __name__ == '__main__':
    num_nodes = 3
    processes = []

    # Use regular dictionary (not a Manager dict) for Queues
    message_queues = {i: Queue() for i in range(num_nodes)}

    for i in range(num_nodes):
        peers = [j for j in range(num_nodes) if j != i]
        p = Process(target=start_node, args=(i, peers, message_queues))
        p.start()
        processes.append(p)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down cluster...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
