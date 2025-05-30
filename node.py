import time
import random
import logging
import os
from multiprocessing import Queue
import csv
import json

class RaftNode:
	def __init__(self, node_id, peers, message_queues, client_queue, blocked_peers):
		self.node_id = node_id
		self.peers = peers
		self.message_queues = message_queues
		self.client_queue = client_queue
		self.blocked_peers = blocked_peers # network partitioning

		self.recovery_start = time.time()
		self.catchup_logged = False # flag to avoid logging to early

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

		log_dir = os.path.join(os.getcwd(), 'logs')
		os.makedirs(log_dir, exist_ok=True)

		self.logger = logging.getLogger(f"Node{self.node_id}")
		self.logger.setLevel(logging.INFO)
		handler = logging.FileHandler(os.path.join(log_dir, f"node{self.node_id}_state.log"), mode='w')
		formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
		handler.setFormatter(formatter)
		self.logger.addHandler(handler)

		# benchmarking metrics
		self.metrics_file = os.path.join(log_dir, f"node{self.node_id}_metrics.csv")
		if not os.path.exists(self.metrics_file):
			with open(self.metrics_file, mode='w', newline='') as f:
				writer = csv.writer(f)
				writer.writerow(["metric", "event", "timestamp", "details"])  # write header only once

	def log_metric(self, metric_type, event, details=""):
		timestamp = time.time()
		with open(self.metrics_file, mode='a', newline='') as f:
			writer = csv.writer(f)
			writer.writerow([metric_type, event, timestamp, details])
			f.flush()
			os.fsync(f.fileno())  # ensure write to disk
		self.logger.info(f"[METRIC] {metric_type} {event} {timestamp} {details}")


	def send_message(self, target_id, message):
		if (self.node_id, target_id) in self.blocked_peers:
			self.logger.info(f"[PARTITION] Blocking message from {self.node_id} to {target_id}")
			return
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
			candidate_term = message['term']
			candidate_id = message['candidate_id']
			candidate_last_log_index = message.get('last_log_index', -1)
			candidate_last_log_term = message.get('last_log_term', -1)

			self_last_log_index = len(self.log) - 1
			self_last_log_term = self.log[self_last_log_index]['term'] if self_last_log_index >= 0 else -1

			self.logger.info(f"[VOTE REQUEST] From Node {candidate_id}, Term {candidate_term}, "
							f"LastLogIndex {candidate_last_log_index}, LastLogTerm {candidate_last_log_term}")
			self.logger.info(f"[SELF STATE] Term {self.current_term}, LastLogIndex {self_last_log_index}, "
							f"LastLogTerm {self_last_log_term}, VotedFor {self.voted_for}")

			vote_granted = False

			if candidate_term > self.current_term:
				self.voted_for = candidate_id
				self.current_term = candidate_term
				vote_granted = True
				self.logger.info("[VOTE DECISION] Higher term, granting vote.")
			elif candidate_term == self.current_term:
				log_up_to_date = (
					candidate_last_log_term > self_last_log_term or
					(candidate_last_log_term == self_last_log_term and candidate_last_log_index >= self_last_log_index)
				)
				if (self.voted_for is None or self.voted_for == candidate_id) and log_up_to_date:
					self.voted_for = candidate_id
					vote_granted = True
					self.logger.info("[VOTE DECISION] Equal term, log up-to-date, granting vote.")
				else:
					self.logger.info("[VOTE DECISION] Log not up-to-date or already voted, denying vote.")
			else:
				exit(1)
				self.logger.info("[VOTE DECISION] Term too old, denying vote.")

			response = {
				'type': 'Vote',
				'term': self.current_term,
				'vote_granted': vote_granted,
				'to': candidate_id,
				'from': self.node_id
			}
			self.send_message(candidate_id, response)

		elif message['type'] == 'Vote':
			if self.state == 'candidate' and message['vote_granted']:
				self.votes_received += 1
				if self.votes_received > len(self.peers) // 2:
					self.state = 'leader'
					print(f"[ELECTION] Node {self.node_id} became leader for term {self.current_term}")
					self.log_metric("election", "won", f"term={self.current_term}")

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

				# log replication delay for benchmarking
				for idx in range(self.last_applied + 1, self.commit_index + 1):
					entry = self.log[idx]
					if 'timestamp' in entry:
						delay = time.time() - entry['timestamp']
						self.log_metric("replication", "follower_commit", f"idx={idx} delay={delay:.4f}")

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

		elif message['type'] == 'client_command':
			if self.state == 'leader':
				command = message['command']
				self.log.append({'term': self.current_term, 'command': command})
				self.logger.info(f"[CLIENT] Received command: {command}")
				print(f"[CLIENT] Leader {self.node_id} received client command: {command}")
				for peer in self.peers:
					self.send_append_entries(peer)
			else:
				# forward to known leader (basic — assumes most recent AppendEntries sender is leader)
				self.logger.info(f"[CLIENT] Not leader, ignoring client command.")

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
				self.log_metric("election", "start", f"term={self.current_term}")
				self.broadcast_message({
					'type': 'RequestVote',
					'term': self.current_term,
					'candidate_id': self.node_id,
					'last_log_index': len(self.log) - 1,
					'last_log_term': self.log[-1]['term'] if self.log else -1
				})

			elif self.state == 'leader' and (now - last_heartbeat) >= heartbeat_interval:
				command = f"cmd@{now:.2f}"
				timestamp = time.time()
				self.log.append({'term': self.current_term, 'command': command, 'timestamp': timestamp})
				self.log_metric("replication", "leader_append", f"{command} {timestamp}")
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

			try:
				client_message = self.client_queue.get_nowait()
				self.handle_message(client_message)
			except:
				pass

			# recovery time logging
			if self.state == 'follower' and not self.catchup_logged:
				if self.commit_index >= len(self.log) - 1:
					delay = time.time() - self.recovery_start
					self.log_metric("recovery", "caught_up", f"delay={delay:.4f}")
					self.catchup_logged = True

			if int(time.time()) % 5 == 0:  # once every 5 seconds
				self.logger.info(f"State: {self.state}, Term: {self.current_term}, Log Length: {len(self.log)}, Commit Index: {self.commit_index}")

			if int(time.time()) % 10 == 0:
				with open(f'logs/node{self.node_id}_log.json', 'w') as f:
					json.dump(self.log, f)
			
