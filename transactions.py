import hashlib


class Transactions:
    def __init__(self):
        self.transactions = {}

    def add(self, transaction):
        hash_code = hashlib.sha256(str(transaction).encode('utf-8')).hexdigest()
        self.transactions[hash_code] = transaction

    def get_all(self):
        transactions = self.transactions
        self.transactions = {}
        return transactions
