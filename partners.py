import requests


class Partners:
    def __init__(self):
        self.partners = []

    def add_partner(self, address):
        self.partners.append(address)

    def broadcast(self, data):
        for partner in self.partners:
            requests.post(f'{partner}block/', json=data).json()
        pass

    def request(self, missing, partner):
        if partner in self.partners:
            return requests.get(f'{partner}block/', params={'hash_code': missing}).json()
