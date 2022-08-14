from mongodb_storage import DBBridge
from datetime import datetime


if __name__ == '__main__':
    connection = DBBridge(None, 27017, 'Nakamoto_5000')
    db = connection.get_root_collection('blocks')
    blocks = db.get()
    del connection
    print('total number of blocks:', len(blocks))
    max_index = max([v['index'] for v in blocks.values()])
    print(max_index)
    max_blocks = [k for k, v in blocks.items() if v['index'] == max_index]
    print('number of tips:', len(max_blocks))
    key = max_blocks[0]
    stem = []
    while key:
        block = blocks[key]
        stem.insert(0, block)
        key = block['header']['previous']
    transactions = []
    timestamps = []
    for block in stem:
        for item in block['transactions']:
            transactions.append(item['transaction'])
            timestamps.append(datetime.strptime(block['header']['timestamp'], '%Y%m%d%H%M%S%f').timestamp())
    n = len(transactions)
    print('timestamps, transactions')
    for idx in range(n):
        print(timestamps[idx], transactions[idx], sep=', ')
    transactions.sort()
    print(n, 'transactions', sum([tx == f'transaction_{str(idx).zfill(5)}' for idx, tx in enumerate(transactions)]),
          'are ok')
    print('index, owner, timestamp')
    owners = {}
    for block in stem:
        print(block['index'], block['owner'], block['header']['timestamp'], sep=', ')
        owners[block['owner']] = owners.get(block['owner'], 0) + 1
    print(list(owners.values()))
    keys = blocks.keys()
    order = {k: i + 3 for i, k in enumerate(keys)}
    order[None] = 1
    order['missing'] = 2
    print('[', end='')
    for key in keys:
        prev = blocks[key]['header']['previous']
        print(f'[{order[key]}, {order[prev] if prev in order else order["missing"]}]; ', end='')
    print('[1, 1]];')

