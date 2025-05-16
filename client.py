from multiprocessing import Queue
import time
import sys
import os
from multiprocessing.managers import BaseManager

class ClientManager(BaseManager): pass
ClientManager.register('get_client_queue')

def send_command(command):
    manager = ClientManager(address=('localhost', 50000), authkey=b'raft')
    manager.connect()
    queue = manager.get_client_queue()
    message = {
        'type': 'client_command',
        'command': command,
        'from': 'client'
    }
    queue.put(message)
    print(f"[CLIENT] Sent: {command}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python client.py 'SET x=5'")
        sys.exit(1)

    send_command(sys.argv[1])
