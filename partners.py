import sys

import requests
from threading import Thread
from queue import Queue


class Partners:
    def __init__(self):
        self.partners = []
        self.tasks = Queue()
        self.add_block_cb = None

    def set_cb(self, cb):
        self.add_block_cb = cb
        Thread(target=self.run, daemon=True).start()

    def run(self):
        while True:
            name, data = self.tasks.get()
            if name == 'broadcast':
                for partner in self.partners:
                    requests.post(f'{partner}block/', json=data).json()
            elif name == 'request':
                missing, partner = data
                if partner in self.partners:
                    self.add_block_cb(requests.get(f'{partner}block/', params={'hash_code': missing}).json())
            self.tasks.task_done()

    def add_partner(self, address):
        self.partners.append(address)

    def broadcast(self, data):
        self.tasks.put(('broadcast', data))

    def request(self, missing, partner):
        self.tasks.put(('request', (missing, partner)))

    def wait(self):
        print('tasks', self.tasks.empty())
        sys.stdout.flush()
        self.tasks.join()
