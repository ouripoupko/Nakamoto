from random import choice
from mongodb_storage import DBBridge


class Blocks:
    def __init__(self):
        self.db = None
        self.blocks = None
        self.archive = None
        self.pending = {}
        self.min_index = 0
        self.max_index = 0

    def init_db(self, name):
        self.db = DBBridge(None, 27017, f'Nakamoto_{name}')
        self.blocks = self.db.get_root_collection('blocks')
        self.archive = self.db.get_root_collection('archive')

    def get(self, hash_code):
        block = self.blocks[hash_code].get_dict()
        if not block:
            block = self.archive[hash_code].get_dict()
        return block

    def get_longest(self):
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
        while tip in blocks:
            block = blocks[tip]
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

    def add_block(self, block):
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
            previous = None
        else:
            self.pending[block['hash_code']] = block
        return previous

    def find_my_previous(self, tip, owner):
        count = 0
        block = None
        while tip:
            block = self.blocks[tip]
            if block['owner'] == owner:
                break
            count += 1
            tip = block['header']['previous']
        return count if block and block['owner'] == owner else None

