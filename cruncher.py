import sys

from threading import Thread
from queue import Queue
from datetime import datetime
from random import randrange
import hashlib
import time


class Cruncher:
    def __init__(self, partners, blocks):
        partners.set_cb(self.add_block)
        self.partners = partners
        self.blocks = blocks
        self.me = None
        self.transactions = {}
        self.queue = Queue()
        self.please_stop = False

    def set_me(self, address):
        self.me = address
        self.blocks.init_db(address[-5:-1])
        Thread(target=self.run, daemon=True).start()

    def run(self):
        while True:
            order, data = self.queue.get()
            print(self.me, order, data)
            if self.please_stop:
                self.queue.task_done()
                continue
            if order == 'transaction':
                hash_code = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
                self.transactions[hash_code] = data
            elif order == 'block':
                self.process_block(data)
            block = self.prepare_block()
            if block['transactions']:
                self.try_to_crunch(block)
            self.queue.task_done()

    def process_block(self, block):
        for tx_key in block['transactions']:
            if tx_key in self.transactions:
                del self.transactions[tx_key]
        missing = self.blocks.add_block(block)
        if missing:
            self.partners.request(missing, block['owner'])

    def prepare_block(self):
        key, value = self.blocks.choose_tip()
        transaction_pool = self.transactions.copy()
        self.blocks.do_archive(transaction_pool, key)
        difficulty = self.calculate_difficulty(key)
        transactions_hash_code = hashlib.sha256(str(transaction_pool).encode('utf-8')).hexdigest()
        return {'index': value['index'] + 1 if key else 0,
                'owner': self.me,
                'difficulty': difficulty,
                'header': {'hash_code': transactions_hash_code,
                           'timestamp': datetime.now().strftime('%Y%m%d%H%M%S%f'),
                           'previous': key,
                           'nonce': 0},
                'transactions': transaction_pool}

    def try_to_crunch(self, block):
        found = False
        while True:
            if not self.queue.empty():
                break
            block['header']['nonce'] = randrange(1 << (block['difficulty'] + 32))
            block['hash_code'] = hashlib.sha256(str(block['header']).encode('utf-8')).hexdigest()
            if (block['difficulty']) <= 256 - int(block['hash_code'], 16).bit_length():
                found = True
                break
            time.sleep(0.001)
        if found:
            self.add_block(block)
            self.partners.broadcast(block)

    def calculate_difficulty(self, tip):
        my_last = self.blocks.find_my_previous(tip, owner=self.me)
        return len(self.partners.partners) - my_last + 16 if my_last else 16

    def add_transaction(self, transaction):
        self.queue.put(('transaction', transaction))

    def add_block(self, block):
        self.queue.put(('block', block))

    def wait(self):
        self.please_stop = True
        print('cruncher', self.me, self.queue.empty())
        sys.stdout.flush()
        self.queue.join()
