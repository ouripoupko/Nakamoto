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
        self.last_block = 0
        self.last_mine = 0

    def set_me(self, address):
        self.me = address
        self.blocks.init_db(address[-5:-1])
        Thread(target=self.run, daemon=True).start()

    def run(self):
        while True:
            if (not self.queue.empty()) or (self.last_block-self.last_mine <= 70):
                order, data = self.queue.get()
                if order == 'transaction':
                    transaction = {'timestamp': datetime.now().strftime('%Y%m%d%H%M%S%f'),
                                   'owner': self.me,
                                   'transaction': data}
                    transaction['hash_code'] = hashlib.sha256(str(transaction).encode('utf-8')).hexdigest()
                    self.transactions[transaction['hash_code']] = transaction
                elif order == 'block':
                    self.process_block(data)
            if not self.queue.empty():
                continue
            block = self.prepare_block()
            if self.transactions or (self.last_block-self.last_mine > 70):
                self.try_to_crunch(block)
            self.queue.task_done()

    def process_block(self, block):
        for tx in block['transactions']:
            if tx['hash_code'] in self.transactions:
                del self.transactions[tx['hash_code']]
        self.last_block = block['index']
        if block['owner'] == self.me:
            self.last_mine = block['index']
        missing = self.blocks.add_block(block)
        if missing:
            self.partners.request(missing, block['owner'])

    def prepare_block(self):
        key, value = self.blocks.choose_tip()
        transaction_pool, to_delete = self.blocks.do_archive(self.transactions, key)
        for tx_key in to_delete:
            del self.transactions[tx_key]
        self.transactions.update({k: v for k, v in transaction_pool.items() if v['owner'] == self.me})
        payload = sorted(transaction_pool.values(), key=lambda tx: tx['timestamp'])
        payload = payload if len(payload) <= 5 else payload[0:5]
        difficulty = self.calculate_difficulty(key)
        transactions_hash_code = hashlib.sha256(str(payload).encode('utf-8')).hexdigest()
        return {'index': value['index'] + 1 if key else 0,
                'owner': self.me,
                'difficulty': difficulty,
                'header': {'hash_code': transactions_hash_code,
                           'timestamp': datetime.now().strftime('%Y%m%d%H%M%S%f'),
                           'previous': key,
                           'nonce': 0},
                'transactions': payload}

    def try_to_crunch(self, block):
        found = False
        while True:
            block['header']['nonce'] = randrange(1 << (block['difficulty'] + 32))
            block['hash_code'] = hashlib.sha256(str(block['header']).encode('utf-8')).hexdigest()
            if (block['difficulty']) <= 256 - int(block['hash_code'], 16).bit_length():
                found = True
                break
            if not self.queue.empty():
                break
            time.sleep(0.001)
        if found:
            self.process_block(block)
            print(self.me, block['index'], block['difficulty'])
            self.partners.broadcast(block)

    def calculate_difficulty(self, tip):
        n = len(self.partners.partners)
        my_last = self.blocks.find_my_previous(tip, self.me, n)
        difficulty = n - my_last - 30 if my_last >= 0 else 0
        return difficulty if difficulty > 12 else 12

    def add_transaction(self, transaction):
        self.queue.put(('transaction', transaction))

    def add_block(self, block):
        self.queue.put(('block', block))

    def wait(self):
        print('cruncher', self.me, self.queue.empty())
        sys.stdout.flush()
        self.queue.join()

    def count(self):
        # self.blocks.print()
        return len(self.transactions)
