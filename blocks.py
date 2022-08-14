from random import choice
from mongodb_storage import DBBridge
from threading import Lock


class Blocks:
    def __init__(self):
        self.connection = None
        self.db = None
        self.blocks = {}
        self.archive = {}
        self.pending = {}
        self.max_index = 0
        self.lock = Lock()

    def init_db(self, name):
        self.connection = DBBridge(None, 27017, f'Nakamoto_{name}')
        self.db = self.connection.get_root_collection('blocks')

    def get(self, hash_code):
        block = self.blocks.get(hash_code)
        if not block:
            block = self.archive.get(hash_code)
        if not block:
            block = self.db[hash_code].get_dict()
        return block

    def get_longest(self):
        longest = {k: v for k, v in self.blocks.items() if v['index'] == self.max_index}
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
        while tip in self.blocks:
            block = self.blocks[tip]
            chain[tip] = block
            tip = block['header']['previous']
        branches = {k: v for k, v in self.blocks.items() if k not in chain}
        return chain, branches

    def do_archive(self, transactions, tip):
        transaction_pool = {}
        to_delete = []
        stem_tx = {}
        if tip in self.blocks:
            stem, branches = self.get_chain(tip)
            for key, block in stem.items():
                for tx in block['transactions']:
                    stem_tx[tx['hash_code']] = tx
            for key, block in branches.items():
                for tx in block['transactions']:
                    if tx['hash_code'] not in stem_tx:
                        transaction_pool[tx['hash_code']] = tx
                self.archive[key] = block
                del self.blocks[key]
        for key, tx in transactions.items():
            if key in stem_tx:
                to_delete.append(key)
            else:
                transaction_pool[key] = tx
        return transaction_pool, to_delete

    def retrieve_from_archive(self, key):
        while key in self.archive:
            block = self.archive[key]
            if block['index'] <= self.max_index - 28:
                print('******************************* I made a wrong assumption ******************************')
            self.blocks[key] = block
            del self.archive[key]
            key = self.blocks[key]['header']['previous']

    def grow_tree(self, block):
        with self.lock:
            self.db[block['hash_code']] = block
        index = block['index']
        if index > self.max_index:
            self.max_index = index
            old = {k: v for k, v in self.blocks.items() if v['index'] < index-100}
            for key in old:
                del self.blocks[key]
            old = {k: v for k, v in self.archive.items() if v['index'] < index-30}
            for key in old:
                del self.archive[key]

    def loop_pending(self):
        to_delete = []
        for key, block in self.pending.items():
            previous = block['header']['previous']
            self.retrieve_from_archive(previous)
            if previous in self.blocks:
                self.blocks[key] = block
                self.grow_tree(block)
                to_delete.append(key)
        for key in to_delete:
            del self.pending[key]
        return len(to_delete) > 0

    def add_block(self, block):
        previous = block['header']['previous']
        self.retrieve_from_archive(previous)
        if not previous or previous in self.blocks:
            self.blocks[block['hash_code']] = block
            self.grow_tree(block)
            while self.pending and self.loop_pending():
                continue
            previous = None
        else:
            self.pending[block['hash_code']] = block
        return previous

    def find_my_previous(self, tip, owner, max_depth):
        count = 0
        block = None
        index = -1 if self.max_index <= max_depth else self.max_index - max_depth - 1
        while tip and tip in self.blocks and max_depth:
            block = self.blocks[tip]
            if block['owner'] == owner:
                break
            count += 1
            tip = block['header']['previous']
            max_depth -= 1
        return (count, block['difficulty']) if (block and block['owner'] == owner) else (-1, 0)

    def print(self):
        with self.lock:
            keys = [key for key in self.db]
            order = {k: i+3 for i, k in enumerate(keys)}
            order[None] = 1
            order['missing'] = 2
            print('[', end='')
            for key in self.db:
                block = self.db[key]
                prev = block['header']['previous']
                print(f'[{order[key]}, {order[prev] if prev in order else order["missing"]}]; ', end='')
            print('[1, 1]];')
