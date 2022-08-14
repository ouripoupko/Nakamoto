from random import choice
from time import sleep
import requests

servers = [f'http://localhost:{index}/' for index in range(5000, 5100)]
n = 20000

for index in range(n):
    server = choice(servers)
    reply = requests.post(f'{server}transaction/', json={'data': f'transaction_{str(index).zfill(5)}'}).json()
    print(index, server, reply['time'])
    sleep(0.02)

while True:
    count = 0
    res = [0]*len(servers)
    for idx, server in enumerate(servers):
        res[idx] = requests.get(f'{server}count/').json()['count']
        count += res[idx]
    sleep(4)
    print(res)
    if count == 0:
        break
    sleep(10)
