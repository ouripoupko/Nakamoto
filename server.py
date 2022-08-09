import sys
import logging
from flask import Flask, request

from transactions import Transactions
from blocks import Blocks

app = Flask(__name__)
transactions = Transactions()
blocks = Blocks()


@app.route("/transaction/", methods=['GET', 'POST', 'PUT', 'DELETE'])
def get_transaction():
    transaction = request.get_json()
    transactions.add(transaction)
    time = blocks.create(transactions.get_all())
    return {'time': time}


@app.route("/block/", methods=['GET', 'POST', 'PUT', 'DELETE'])
def get_block():
    if request.method == 'POST':
        block = request.get_json()
        blocks.add(block)
        return {}
    if request.method == 'GET':
        hash_code = request.args.get('hash_code')
        return blocks.get(hash_code)


@app.route("/count/", methods=['GET', 'POST', 'PUT', 'DELETE'])
def get_count():
    return {'count': len(blocks.blocks) + len(blocks.archive), 'longest': blocks.get_chain_print()}


if __name__ == '__main__':
    port = int(sys.argv[1])
    for arg in sys.argv[2:]:
        if arg != port:
            blocks.add_partner(f'http://localhost:{arg}/')
    blocks.set_me(f'http://localhost:{port}/')
    logger = logging.getLogger('werkzeug')
    logger.setLevel(logging.ERROR)
    app.run(host='0.0.0.0', port=port, use_reloader=False)
