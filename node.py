import time
import random

class RaftNode:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.state = 'follower'
        self.current_term = 0

    def run(self):
        print(f"Node {self.node_id} started as {self.state}. Peers: {self.peers}")
        time.sleep(random.uniform(1, 3))
        self.state = 'candidate'
        print(f"Node {self.node_id} became {self.state} and started election.")
