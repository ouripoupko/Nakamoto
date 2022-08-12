import sys
import logging
from flask import Flask, request

from cruncher import Cruncher
from partners import Partners
from blocks import Blocks

app = Flask(__name__)
partners = Partners()
blocks = Blocks()
cruncher = Cruncher(partners, blocks)


@app.route("/transaction/", methods=['GET', 'POST', 'PUT', 'DELETE'])
def get_transaction():
    transaction = request.get_json()
    time = cruncher.add_transaction(transaction['data'])
    return {'time': time}


@app.route("/block/", methods=['GET', 'POST', 'PUT', 'DELETE'])
def get_block():
    if request.method == 'POST':
        block = request.get_json()
        cruncher.add_block(block)
        return {}
    if request.method == 'GET':
        hash_code = request.args.get('hash_code')
        reply = blocks.get(hash_code)
        return reply


@app.route("/count/", methods=['GET', 'POST', 'PUT', 'DELETE'])
def get_count():
    sys.stdout.flush()
    return {'count': cruncher.count()}


if __name__ == '__main__':
    port = int(sys.argv[1])
    for arg in sys.argv[2:]:
        if arg != sys.argv[1]:
            partners.add_partner(f'http://localhost:{arg}/')
    cruncher.set_me(f'http://localhost:{port}/')
    logger = logging.getLogger('werkzeug')
    logger.setLevel(logging.ERROR)
    app.run(host='0.0.0.0', port=port, use_reloader=False)

# defects to fix
# 3- scrabble broadcasting to distribute payload
# 4- add random processing power variant
