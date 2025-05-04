from multiprocessing import Process, Queue
from node import RaftNode
import time

def start_node(node_id, peers, message_queues):
    node = RaftNode(node_id, peers, message_queues)
    node.run()

if __name__ == '__main__':
    num_nodes = 3
    processes = {}

    # shared queues between nodes
    message_queues = {i: Queue() for i in range(num_nodes)}

    # start initial processes
    for i in range(num_nodes):
        peers = [j for j in range(num_nodes) if j != i]
        p = Process(target=start_node, args=(i, peers, message_queues))
        p.start()
        processes[i] = p

    try:
        while True:
            for i, p in list(processes.items()):
                if not p.is_alive():
                    print(f"[RECOVERY] Node {i} crashed. Restarting...")
                    peers = [j for j in range(num_nodes) if j != i]
                    new_p = Process(target=start_node, args=(i, peers, message_queues))
                    new_p.start()
                    processes[i] = new_p
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down cluster...")
        for p in processes.values():
            p.terminate()
        for p in processes.values():
            p.join()
