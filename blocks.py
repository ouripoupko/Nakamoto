from partners import Partners
from random import choice
from threading import Thread, Lock
from queue import Queue
import hashlib


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
        self.partners = Partners()
        self.blocks = {}
        self.archive = {}
        self.pending = {}
        self.me = ''
        self.lock = Lock()
        self.complement_queue = Queue()
        self.broadcast_queue = Queue()
        Thread(target=broadcast, args=(self.broadcast_queue, self.partners), daemon=True).start()
        Thread(target=complement, args=(self.complement_queue, self), daemon=True).start()

    def set_me(self, address):
        self.me = address

    def add_partner(self, address):
        self.partners.add_partner(address)

    def get(self, hash_code):
        return self.blocks[hash_code] if hash_code in self.blocks else self.archive[hash_code]

    def get_longest(self):
        longest = {}
        index = 0
        for key in self.blocks:
            if self.blocks[key]['index'] > index:
                longest = {}
                index = self.blocks[key]['index']
            if self.blocks[key]['index'] == index:
                longest[key] = self.blocks[key]
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
        while tip:
            block = self.blocks[tip]
            chain[tip] = block
            tip = block['header']['previous']
        return chain

    def do_archive(self, transactions, tip):
        if tip in self.blocks:
            branch = self.get_chain(tip)
            branch_tx = {}
            for key in branch:
                block = branch[key]
                for tx_key in block['transactions']:
                    branch_tx[tx_key] = block['transactions'][tx_key]
            for key in self.blocks:
                if key not in branch:
                    block = self.blocks[key]
                    for tx_key in block['transactions']:
                        if tx_key not in branch_tx:
                            transactions[tx_key] = block['transactions'][tx_key]
                    self.archive[key] = block
            self.blocks = branch

    def create(self, transactions):
        with self.lock:
            key, value = self.choose_tip()
            self.do_archive(transactions, key)
            transactions_hash_code = hashlib.sha256(str(transactions).encode('utf-8')).hexdigest()
            block = {'index': value['index'] + 1 if key else 0,
                     'owner': self.me,
                     'header': {'hash_code': transactions_hash_code,
                                'previous': key,
                                'nonce': 0},
                     'transactions': transactions}
            block['hash_code'] = hashlib.sha256(str(block['header']).encode('utf-8')).hexdigest()
        self.add(block)
        self.broadcast_queue.put(block)

    def retrieve_from_archive(self, key):
        while key in self.archive:
            self.blocks[key] = self.archive[key]
            del self.archive[key]
            key = self.blocks[key]['header']['previous']

    def loop_pending(self):
        to_delete = []
        for key in self.pending:
            self.retrieve_from_archive(key)
            if self.pending[key]['header']['previous'] in self.blocks:
                self.blocks[key] = self.pending[key]
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
            value = self.blocks.get(value['header']['previous'])
        return {'owners': owners}  #, 'txs': txs}
