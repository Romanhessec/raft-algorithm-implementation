import time
import random
from multiprocessing import Queue

class RaftNode:
	def __init__(self, node_id, peers, message_queues):
		self.node_id = node_id
		self.peers = peers
		self.message_queues = message_queues

		self.state = 'follower'
		self.start_time = time.time()
		self.current_term = 0
		self.voted_for = None
		self.votes_received = 0

		self.election_timeout = random.uniform(1.5, 3.0)  # seconds

	def send_message(self, target_id, message):
		self.message_queues[target_id].put(message)

	def broadcast_message(self, message):
		for peer_id in self.peers:
			self.send_message(peer_id, message)

	def handle_message(self, message):
		if message['type'] == 'RequestVote':
			if (message['term'] > self.current_term and self.voted_for is None):
				self.voted_for = message['candidate_id']
				self.current_term = message['term']
				response = {
					'type': 'Vote',
					'term': self.current_term,
					'vote_granted': True,
					'to': message['candidate_id'],
					'from': self.node_id
				}
				self.send_message(message['candidate_id'], response)
		elif message['type'] == 'Vote':
			if self.state == 'candidate' and message['vote_granted']:
				self.votes_received += 1
				if self.votes_received > len(self.peers) // 2:
					self.state = 'leader'
					print(f"Node {self.node_id} became leader for term {self.current_term}")
		elif message['type'] == 'AppendEntries':
			if message['term'] >= self.current_term:
				self.current_term = message['term']
				self.state = 'follower'
				self.voted_for = None
				self.start_time = time.time()  # reset election timeout

	def run(self):
		print(f"Node {self.node_id} started as {self.state}. Peers: {self.peers}")

		heartbeat_interval = 0.5 # leader sends heartbeat signal every 0.5s

		self.start_time = last_heartbeat = time.time()
		while True:
			now = time.time()

			# check for election timeout
			if self.state == 'follower' and (now - self.start_time) >= self.election_timeout:
				self.state = 'candidate'
				self.current_term += 1
				self.voted_for = self.node_id
				self.votes_received = 1 # voted for self
				print(f"Node {self.node_id} became {self.state} in term {self.current_term}")
				self.broadcast_message({
					'type': 'RequestVote',
					'term': self.current_term,
					'candidate_id': self.node_id
				})
			elif self.state == 'leader' and (now - last_heartbeat) >= heartbeat_interval:
				self.broadcast_message({
					'type': 'AppendEntries',
					'term': self.current_term,
					'leader_id': self.node_id,
					'entries': [] # empty heartbeat signal
				})
				last_heartbeat = now

			# process incoming messages
			while not self.message_queues[self.node_id].empty():
				message = self.message_queues[self.node_id].get()
				self.handle_message(message)
			
			time.sleep(0.1) # tbd if we should keep this - busy waiting