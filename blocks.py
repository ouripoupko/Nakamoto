from partners import Partners
from random import choice
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


def broadcast(queue, partners):
    while True:
        block = queue.get()
        partners.broadcast(block)
        queue.task_done()


class Blocks:
    def __init__(self):
        self.db = None
        self.blocks = None
        self.archive = None
        self.partners = Partners()
        self.pending = {}
        self.index = 0
        self.me = ''
        self.lock = Lock()
        self.complement_queue = Queue()
        self.broadcast_queue = Queue()
        Thread(target=broadcast, args=(self.broadcast_queue, self.partners), daemon=True).start()
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
        longest = self.blocks.get('index', '==', self.index)
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
        blocks = self.blocks.get()
        while tip:
            block = blocks[tip]
            chain[tip] = block
            tip = block['header']['previous']
        return chain

    def do_archive(self, transactions, tip):
        start = time.time()
        got = start
        branched = start
        if tip in self.blocks:
            branch = self.get_chain(tip)
            branch_tx = {}
            got = time.time()
            for key in branch:
                block = branch[key]
                for tx_key in block['transactions']:
                    branch_tx[tx_key] = block['transactions'][tx_key]
            branched = time.time()
            for key in self.blocks:
                if key not in branch:
                    block = self.blocks[key].get_dict()
                    for tx_key in block['transactions']:
                        if tx_key not in branch_tx:
                            transactions[tx_key] = block['transactions'][tx_key]
                    self.archive[key] = block
                    del self.blocks[key]
        end = time.time()
        return [end-start, got-start, branched-got, end-branched]

    def create(self, transactions):
        with self.lock:
            key, value = self.choose_tip()
            reply = self.do_archive(transactions, key)
            transactions_hash_code = hashlib.sha256(str(transactions).encode('utf-8')).hexdigest()
            block = {'index': value['index'] + 1 if key else 0,
                     'owner': self.me,
                     'timestamp': datetime.now().strftime('%Y%m%d%H%M%S%f'),
                     'header': {'hash_code': transactions_hash_code,
                                'previous': key,
                                'nonce': 0},
                     'transactions': transactions}
            block['hash_code'] = hashlib.sha256(str(block['header']).encode('utf-8')).hexdigest()
        self.add(block)
        self.broadcast_queue.put(block)
        return reply

    def retrieve_from_archive(self, key):
        while key in self.archive:
            self.blocks[key] = self.archive[key].get_dict()
            del self.archive[key]
            key = self.blocks[key]['header']['previous']

    def loop_pending(self):
        to_delete = []
        for key in self.pending:
            self.retrieve_from_archive(key)
            if self.pending[key]['header']['previous'] in self.blocks:
                self.blocks[key] = self.pending[key]
                if self.pending[key]['index'] > self.index:
                    self.index = self.pending[key]['index']
                to_delete.append(key)
        for key in to_delete:
            del self.pending[key]
        return len(to_delete) > 0

    def add(self, block):
        with self.lock:
            previous = block['header']['previous']
            self.retrieve_from_archive(previous)
            if not previous or previous in self.blocks:
                self.blocks[block['hash_code']] = block
                if block['index'] > self.index:
                    self.index = block['index']
                while self.pending and self.loop_pending():
                    continue
            else:
                self.pending[block['hash_code']] = block
                self.complement_queue.put((previous, block['owner']))

    def get_chain_print(self):
        key, value = self.choose_tip()
        owners = []
        # txs = []
        while value:
            owners.insert(0, value['owner'])
            # txs.insert(0, value['transactions'])
            key = value['header']['previous']
            value = self.blocks[key] if key else None
        return {'owners': owners}  # , 'txs': txs}
