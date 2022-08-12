from random import choice
from time import sleep
import requests

servers = [f'http://localhost:{index}/' for index in range(5000, 5100)]
n = 1000

for index in range(n):
    server = choice(servers)
    reply = requests.post(f'{server}transaction/', json={'data': f'transaction_{str(index).zfill(5)}'}).json()
    print(index, server, reply['time'])
    sleep(0.01)

while True:
    count = 0
    for server in servers:
        count += requests.get(f'{server}count/').json()['count']
    print('total count:', count)
    if count == 0:
        break
    sleep(1)
