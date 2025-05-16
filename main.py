from multiprocessing import Process, Queue
from multiprocessing.managers import BaseManager
from node import RaftNode
import time

# Step 1: Define the queue first
client_queue = Queue()

# Step 2: Register it for sharing
class ClientManager(BaseManager): pass
ClientManager.register('get_client_queue', callable=lambda: client_queue)

def start_node(node_id, peers, message_queues, client_queue):
    node = RaftNode(node_id, peers, message_queues, client_queue)
    node.run()

if __name__ == '__main__':
    num_nodes = 3
    processes = {}

    # Step 3: Start the manager after registering
    manager = ClientManager(address=('', 50000), authkey=b'raft')
    manager.start()

    message_queues = {i: Queue() for i in range(num_nodes)}

    for i in range(num_nodes):
        peers = [j for j in range(num_nodes) if j != i]
        p = Process(target=start_node, args=(i, peers, message_queues, client_queue))
        p.start()
        processes[i] = p

    try:
        while True:
            for i, p in list(processes.items()):
                if not p.is_alive():
                    print(f"[RECOVERY] Node {i} crashed. Restarting...")
                    peers = [j for j in range(num_nodes) if j != i]
                    new_p = Process(target=start_node, args=(i, peers, message_queues, client_queue))
                    new_p.start()
                    processes[i] = new_p
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down cluster...")
        for p in processes.values():
            p.terminate()
        for p in processes.values():
            p.join()
