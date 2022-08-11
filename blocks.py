from partners import Partners
from random import choice, randrange
from threading import Thread, Lock
from queue import Queue
import hashlib
from mongodb_storage import DBBridge
import time
from datetime import datetime


def complement(queue, blocks):
    while True:
        (missing, owner) = queue.get()
        block = blocks.partners.request(missing, owner)
        blocks.add(block)
        queue.task_done()


def generate(queue, blocks):
    while True:
        block = queue.get()
        if isinstance(block, str) and block == 'signal to stop':
            queue.task_done()
            continue
        found = False
        while True:
            if not queue.empty():
                break
            block['header']['nonce'] = randrange(1 << (block['difficulty']+32))
            block['hash_code'] = hashlib.sha256(str(block['header']).encode('utf-8')).hexdigest()
            if (block['difficulty']) <= 256 - int(block['hash_code'], 16).bit_length():
                # print(block['hash_code'], block['difficulty'], int(block['hash_code'], 16).bit_length())
                found = True
                break
            time.sleep(0.001)
        if found:
            blocks.add(block)
            blocks.partners.broadcast(block)
        queue.task_done()


class Blocks:
    def __init__(self):
        self.db = None
        self.transactions = {}
        self.blocks = None
        self.archive = None
        self.pending = {}
        self.partners = Partners()
        self.min_index = 0
        self.max_index = 0
        self.owners = []
        self.me = ''
        self.lock = Lock()
        self.complement_queue = Queue()
        self.generate_queue = Queue()
        Thread(target=generate, args=(self.generate_queue, self), daemon=True).start()
        Thread(target=complement, args=(self.complement_queue, self), daemon=True).start()

    def set_me(self, address):
        self.me = address
        self.db = DBBridge(None, 27017, f'Nakamoto_{address[-5:-1]}')
        self.blocks = self.db.get_root_collection('blocks')
        self.archive = self.db.get_root_collection('archive')

    def add_partner(self, address):
        self.partners.add_partner(address)

    def get(self, hash_code):
        block = None
        with self.lock:
            if hash_code in self.blocks:
                block = self.blocks[hash_code].get_dict()
            elif hash_code in self.archive:
                self.archive[hash_code].get_dict()
        return block

    def get_longest(self):
        # longest = {}
        # index = 0
        # for key in self.blocks:
        #     if self.blocks[key]['index'] > index:
        #         longest = {}
        #         index = self.blocks[key]['index']
        #     if self.blocks[key]['index'] == index:
        #         longest[key] = self.blocks[key].get_dict()
        longest = self.blocks.get('index', '==', self.max_index)
        return longest

    def choose_tip(self):
        key = None
        value = None
        longest = self.get_longest()
        if longest:
            key = choice(list(longest.keys()))
            value = longest[key]
        return key, value

    def get_chain(self, tip):
        chain = {}
        blocks = self.blocks.get('index', '>', self.min_index-1)
        self.owners += [''] * (self.max_index + 1 - len(self.owners))
        while tip in blocks:
            block = blocks[tip]
            self.owners[block['index']] = block['owner']
            chain[tip] = block
            del blocks[tip]
            tip = block['header']['previous']
        return chain, blocks

    def do_archive(self, transactions, tip):
        if tip in self.blocks:
            stem, branches = self.get_chain(tip)
            self.min_index = stem[tip]['index']
            stem_tx = {}
            for key in stem:
                block = stem[key]
                for tx_key in block['transactions']:
                    stem_tx[tx_key] = block['transactions'][tx_key]
            for key in branches:
                block = branches[key]
                for tx_key in block['transactions']:
                    if tx_key not in stem_tx:
                        transactions[tx_key] = block['transactions'][tx_key]
                self.archive[key] = block
                del self.blocks[key]

    def calculate_difficulty(self):
        # if not self.owners:
        #     return 0
        # counters = {}
        # for owner in self.owners:
        #     counters[owner] = counters.get(owner, 0) + 1
        # index = len(counters) - ((len(self.partners.partners) + 3) >> 1)
        # values = sorted(list(counters.values()))
        # me_count = counters.get(self.me, 0)
        # difficulty = 8 + me_count - (values[index] if index >= 0 else 0)
        my_indexes = [i for i in reversed(range(len(self.owners))) if self.owners[i] == self.me]
        my_last_index = my_indexes[0] if my_indexes else None
        difficulty = len(self.partners.partners) - self.max_index + my_last_index if my_last_index else 0
        print('difficulty:', difficulty)
        return difficulty if difficulty > 0 else 0

    def create(self, transaction):
        self.generate_queue.put('signal to stop')
        self.generate_queue.join()
        with self.lock:
            hash_code = hashlib.sha256(str(transaction).encode('utf-8')).hexdigest()
            self.transactions[hash_code] = transaction
            key, value = self.choose_tip()
            transaction_pool = self.transactions.copy()
            self.do_archive(transaction_pool, key)
            difficulty = self.calculate_difficulty()
            transactions_hash_code = hashlib.sha256(str(transaction_pool).encode('utf-8')).hexdigest()
            block = {'index': value['index'] + 1 if key else 0,
                     'owner': self.me,
                     'difficulty': difficulty,
                     'header': {'hash_code': transactions_hash_code,
                                'timestamp': datetime.now().strftime('%Y%m%d%H%M%S%f'),
                                'previous': key,
                                'nonce': 0},
                     'transactions': transaction_pool}
        self.generate_queue.put(block)
        return 0

    def retrieve_from_archive(self, key):
        while key in self.archive:
            block = self.archive[key].get_dict()
            self.blocks[key] = block
            if block['index'] <= self.min_index:
                self.min_index = block['index'] - 1
            del self.archive[key]
            key = self.blocks[key]['header']['previous']

    def loop_pending(self):
        to_delete = []
        for key in self.pending:
            self.retrieve_from_archive(key)
            if self.pending[key]['header']['previous'] in self.blocks:
                self.blocks[key] = self.pending[key]
                index = self.pending[key]['index']
                if index > self.max_index:
                    self.max_index = index
                if index <= self.min_index:
                    self.min_index = index - 1
                to_delete.append(key)
        for key in to_delete:
            del self.pending[key]
        return len(to_delete) > 0

    def add(self, block):
        with self.lock:
            for tx_key in block['transactions']:
                if tx_key in self.transactions:
                    del self.transactions[tx_key]
            previous = block['header']['previous']
            self.retrieve_from_archive(previous)
            if not previous or previous in self.blocks:
                self.blocks[block['hash_code']] = block
                if block['index'] > self.max_index:
                    self.max_index = block['index']
                if block['index'] <= self.min_index:
                    self.min_index = block['index'] - 1
                while self.pending and self.loop_pending():
                    continue
            else:
                self.pending[block['hash_code']] = block
                self.complement_queue.put((previous, block['owner']))

    def wait(self):
        self.generate_queue.join()
