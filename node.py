import time
import random
from multiprocessing import Queue

class RaftNode:
	def __init__(self, node_id, peers, message_queues):
		self.node_id = node_id
		self.peers = peers
		self.message_queues = message_queues

		self.state = 'follower'
		self.current_term = 0
		self.voted_for = None
		self.votes_received = 0

		self.log = []  # stores commands with terms
		self.commit_index = -1  # index of highest log entry known to be committed
		self.last_applied = -1  # index of highest entry applied to state machine
		
		self.next_index = {}  # for each follower: next index to send
		self.match_index = {}  # highest index known to be replicated on each follower

		self.election_timeout = random.uniform(1.5, 3.0)  # seconds
		self.start_time = time.time()

	def send_message(self, target_id, message):
		self.message_queues[target_id].put(message)

	def broadcast_message(self, message):
		for peer_id in self.peers:
			self.send_message(peer_id, message)

	def send_append_entries(self, peer_id):
		prev_index = self.next_index.get(peer_id, 0) - 1
		prev_term = self.log[prev_index]['term'] if prev_index >= 0 and len(self.log) > prev_index else -1
		entries = self.log[self.next_index.get(peer_id, 0):]

		message = {
			'type': 'AppendEntries',
			'term': self.current_term,
			'leader_id': self.node_id,
			'prev_log_index': prev_index,
			'prev_log_term': prev_term,
			'entries': entries,
			'leader_commit': self.commit_index
		}
		self.send_message(peer_id, message)

	def handle_message(self, message):
		if message['type'] == 'RequestVote':
			if (message['term'] >= self.current_term and
			 (self.voted_for is None or self.voted_for == message['candidate_id'])):
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
					print(f"[ELECTION] Node {self.node_id} became leader for term {self.current_term}")

					# Initialize leader state
					for peer in self.peers:
						self.next_index[peer] = len(self.log)
						self.match_index[peer] = -1

		elif message['type'] == 'AppendEntries':
			if message['term'] < self.current_term:
				return

			# even if it was a candidate, it steps down because it received a valid heartbeat
			self.state = 'follower'
			self.current_term = message['term']
			self.voted_for = None
			self.start_time = time.time()

			prev_index = message['prev_log_index']
			prev_term = message['prev_log_term']
			entries = message['entries']

			# Debug info — comment out unless needed
			# print(f"[DEBUG] Node {self.node_id} received AppendEntries from Leader {message['leader_id']}")
			# print(f"[DEBUG] Log before: {self.log}")
			# print(f"[DEBUG] Appending entries: {message['entries']}")
			# print(f"[DEBUG] New commit index: {self.commit_index}")

			if prev_index >= 0 and (len(self.log) <= prev_index or self.log[prev_index]['term'] != prev_term):
				response = {
					'type': 'AppendEntriesResponse',
					'success': False,
					'term': self.current_term,
					'from': self.node_id,
					'to': message['leader_id'],
					'match_index': -1
				}
			else:
				index = prev_index + 1
				for entry in entries:
					if index < len(self.log):
						if self.log[index]['term'] != entry['term']:
							# conflict found — delete everything from here
							self.log = self.log[:index] # truncate conflicting entries
							self.log.append(entry)
						# else: entry already matches — do nothing
					else:
						self.log.append(entry)
					index += 1

				if message['leader_commit'] > self.commit_index:
					self.commit_index = min(message['leader_commit'], len(self.log) - 1)

				response = {
					'type': 'AppendEntriesResponse',
					'success': True,
					'term': self.current_term,
					'from': self.node_id,
					'to': message['leader_id'],
					'match_index': index - 1
				}

			self.send_message(message['leader_id'], response)

		elif message['type'] == 'AppendEntriesResponse':
			if self.state != 'leader':
				return

			print(f"[REPLICATION] Leader {self.node_id} got response from Node {message['from']}: success={message['success']}, match_index={message['match_index']}")

			peer = message['from']
			if message['success']:
				self.match_index[peer] = message['match_index']
				self.next_index[peer] = message['match_index'] + 1
			else:
				self.next_index[peer] = max(0, self.next_index.get(peer, 1) - 1)

			# check if any entries are now committed
			for i in range(len(self.log) - 1, self.commit_index, -1):
				replicated = sum(1 for idx in self.match_index.values() if idx >= i)
				# count the leader itself (replicated + 1)
				if replicated + 1 > len(self.peers) // 2 and self.log[i]['term'] == self.current_term:
					self.commit_index = i
					print(f"[REPLICATION] Leader {self.node_id} committed log index {self.commit_index}")
					break

	def run(self):
		print(f"[BOOT] Node {self.node_id} started as {self.state}. Peers: {self.peers}")
		heartbeat_interval = 0.5
		self.start_time = last_heartbeat = time.time()

		while True:
			now = time.time()

			# Simulate random crash (10% chance every second)
			if random.random() < 0.01:
				print(f"[FAIL] Node {self.node_id} is crashing...")
				break  # exit the run loop = simulated crash

			if self.state == 'follower' and (now - self.start_time) >= self.election_timeout:
				self.state = 'candidate'
				self.current_term += 1
				self.voted_for = self.node_id
				self.votes_received = 1
				self.start_time = now
				print(f"[ELECTION] Node {self.node_id} became candidate in term {self.current_term}")
				self.broadcast_message({
					'type': 'RequestVote',
					'term': self.current_term,
					'candidate_id': self.node_id
				})

			elif self.state == 'leader' and (now - last_heartbeat) >= heartbeat_interval:
				command = f"cmd@{now:.2f}"
				self.log.append({'term': self.current_term, 'command': command})
				print(f"[LEADER] Node {self.node_id} appended command: {command}")

				for peer in self.peers:
					self.send_append_entries(peer)

				last_heartbeat = now

			while self.last_applied < self.commit_index:
				self.last_applied += 1
				entry = self.log[self.last_applied]
				print(f"[STATE] Node {self.node_id} applied log[{self.last_applied}]: {entry['command']}")

			try:
				message = self.message_queues[self.node_id].get(timeout=0.1)
				self.handle_message(message)
			except:
				pass

			# Note: time.sleep() is not necessary here anymore because of blocking get().
