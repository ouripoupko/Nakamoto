from random import choice
from time import sleep
import requests

servers = [f'http://localhost:{index}/' for index in range(5000, 5100)]
n = 20000

for i in range(1):
    for index in range(n >> 0):
        server = choice(servers)
        reply = requests.post(f'{server}transaction/', json={'data': f'transaction_{str(index).zfill(5)}'}).json()
        print(index, server, reply['time'])
    sleep(10)

for server in servers:
    count = requests.get(f'{server}count/').json()['count']
    while count < n:
        print(count, server)
        sleep(2)
        count = requests.get(f'{server}count/').json()['count']

for server in servers[0:1]:
    print(server, requests.get(f'{server}count/').json()['longest'])
