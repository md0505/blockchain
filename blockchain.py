import hashlib
import json
from time import time
from urllib.parse import urlparse
from uuid import uuid4
from async import map_async

import requests
from flask import Flask, jsonify, request



class Blockchain:
    def __init__(self):
        self.current_transactions = []
        self.chain = []
        self.nodes = set()

        # Subnet fields
        SHARD_SIZE = 2                   # shard size
        self.leaders = [0]               # subnet leaders
        self.micronodes = []             # subnet members
        self.ledger_length = 0           # ledger length

        # Create the genesis block
        self.new_block(previous_hash='1', proof=100)

    def register_node(self, address):
        """
        Add a new node to the list of nodes

        :param address: Address of node. Eg. 'http://192.168.0.5:5000'
        """

        parsed_url = urlparse(address)
        if parsed_url.netloc:
            self.nodes.add(parsed_url.netloc)
        elif parsed_url.path:
            # Accepts an URL without scheme like '192.168.0.5:5000'.
            self.nodes.add(parsed_url.path)
        else:
            raise ValueError('Invalid URL')


    def register_micronodes(self, node_ids):
        """
        Add the list of micronodes

        :param node_ids: Node identifiers
        """
        self.micronodes = self.micronodes + node_ids
        return self.micronodes


    def valid_chain(self, chain):
        """
        Determine if a given blockchain is valid

        :param chain: A blockchain
        :return: True if valid, False if not
        """

        last_block = chain[0]
        current_index = 1

        while current_index < len(chain):
            block = chain[current_index]
            print("{}".format(last_block))
            print("{}".format(block))
            print("\n-----------\n")
            # Check that the hash of the block is correct
            last_block_hash = self.hash(last_block)
            if block['previous_hash'] != last_block_hash:
                return False

            # Check that the Proof of Work is correct
            if not self.valid_proof(last_block['proof'], block['proof'], last_block_hash):
                return False

            last_block = block
            current_index += 1

        return True

    def resolve_conflicts(self):
        """
        This is our consensus algorithm, it resolves conflicts
        by replacing our chain with the longest one in the network.

        :return: True if our chain was replaced, False if not
        """

        neighbours = self.nodes
        new_chain = None

        # We're only looking for chains longer than ours
        max_length = self.ledger_length

        # Grab and verify the chains from all the nodes in our network
        for node in neighbours:
            response = requests.get("http://{}/chain".format(node))

            if response.status_code == 200:
                length = response.json()['length']
                chain = response.json()['chain']

                # Check if the length is longer and the chain is valid
                if length > max_length and self.valid_chain(chain):
                    max_length = length
                    new_chain = chain

        # Replace our chain if we discovered a new, valid chain longer than ours
        if new_chain:
            if len(micronodes):
                self.set_subnet_chain(new_chain)
            else:
                self.chain = new_chain
            return True

        return False


    def set_subnet_chain(self, new_chain):
        """
        Replace subnet chain with new_chain

        :param new_chain: New block chain
        :return: None
        """
        for mn in self.micronodes:
            response = requests.get("http://"+mn+"/clear_chain")
        leaders = self.leaders
        for i, block in zip(range(len(new_chain)), new_chain):
            if i/SHARD_SIZE > len(leaders):
                for j in range(i/SHARD_SIZE - len(leaders)):
                    prev=leaders[len(leaders)-1] if len(leaders) else -1
                    leaders.append((prev + 1) % len(micronodes))
            mn = self.micronodes[leaders[i/SHARD_SIZE]]
            response = requests.post("http://"+mn+"/append_chain", data=block)

        return None


    def append_chain(self, block):
        """
        Append a new Block to the Blockchain

        :param data: The block
        :return: New Block
        """

        # Reset the current list of transactions
        self.current_transactions = []

        self.chain.append(block)
        return block


    def new_block(self, proof, previous_hash):
        """
        Create a new Block in the Blockchain

        :param proof: The proof given by the Proof of Work algorithm
        :param previous_hash: Hash of previous Block
        :return: New Block
        """

        leader = self.leaders[len(self.leaders)-1]
        block = {
                'index': len(self.chain) + 1,
                'timestamp': time(),
                'transactions': self.current_transactions,
                'proof': proof,
                'previous_hash': previous_hash or self.hash(self.chain[-1]),
                }
        if len(self.leaders) and node_identifier == self.micronodes[leader]:
            # Reset the current list of transactions
            self.current_transactions = []

            self.chain.append(block)
            # Elect a new leader
            if (len(self.chain) % SHARD_SIZE) == 0:
                self.leaders.append((leader + 1) % len(micronodes))

        elif len(self.micronodes)>0 and node_identifier == self.micronodes[0]:
            block=requests.post("http://"+leader+"/new_bock", data=block)
        self.ledger_length += 1
        return block

    def new_transaction(self, sender, recipient, amount):
        """
        Creates a new transaction to go into the next mined Block

        :param sender: Address of the Sender
        :param recipient: Address of the Recipient
        :param amount: Amount
        :return: The index of the Block that will hold this transaction
        """
        leader = self.leaders[len(self.leaders)-1]
        if len(self.leaders) and node_identifier == self.micronodes[leader]:
            self.current_transactions.append({
                'sender': sender,
                'recipient': recipient,
                'amount': amount,
            })
            return self.last_block['index'] + 1
        elif len(self.micronodes) and node_identifier == self.micronodes[0]:
            t = {'sender': sender, 'recipient': recipient, 'amount': amount}
            return requests.post("http://"+leader+"/new_transaction", data=t)
        return -1

    @property
    def last_block(self):
        leader = self.leaders[len(self.leaders)-1]
        if len(self.leaders) and node_identifier == self.micronodes[leader]:
            return self.chain[-1]
        else:
            return requests.post("http://"+leader+"/last_block", data=block)

    @property
    def full_chain(self):
        leader = self.leaders[len(self.leaders)-1]
        if len(self.micronodes) <= 1:
            return self.chain
        else:
            (fchain, ns) = ([], self.micronodes)
            get_chain = lambda i, mn: requests.get("http://"+mn+"/chain")
            for c in map_async(get_chain, ns):
                fchain.append(c)
            return fchain # TODO: sort by i in map_async() above


    @staticmethod
    def hash(block):
        """
        Creates a SHA-256 hash of a Block

        :param block: Block
        """

        # We must make sure that the Dictionary is Ordered, or we'll have inconsistent hashes
        block_string = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(block_string).hexdigest()

    def proof_of_work(self, last_block):
        """
        Simple Proof of Work Algorithm:

         - Find a number p' such that hash(pp') contains leading 4 zeroes
         - Where p is the previous proof, and p' is the new proof
         
        :param last_block: <dict> last Block
        :return: <int>
        """

        last_proof = last_block['proof']
        last_hash = self.hash(last_block)

        proof = 0
        while self.valid_proof(last_proof, proof, last_hash) is False:
            proof += 1

        return proof

    @staticmethod
    def valid_proof(last_proof, proof, last_hash):
        """
        Validates the Proof

        :param last_proof: <int> Previous Proof
        :param proof: <int> Current Proof
        :param last_hash: <str> The hash of the Previous Block
        :return: <bool> True if correct, False if not.

        """

        guess = "{},{},{}".format(last_proof, proof, last_hash).encode()
        guess_hash = hashlib.sha256(guess).hexdigest()
        return guess_hash[:4] == "0000"


# Instantiate the Node
app = Flask(__name__)

# Generate a globally unique address for this node
node_identifier = str(uuid4()).replace('-', '')

# Instantiate the Blockchain
blockchain = Blockchain()


@app.route('/mine', methods=['GET'])
def mine():
    # We run the proof of work algorithm to get the next proof...
    last_block = blockchain.last_block
    proof = blockchain.proof_of_work(last_block)

    # We must receive a reward for finding the proof.
    # The sender is "0" to signify that this node has mined a new coin.
    blockchain.new_transaction(
        sender="0",
        recipient=node_identifier,
        amount=1,
    )

    # Forge the new Block by adding it to the chain
    previous_hash = blockchain.hash(last_block)
    block = blockchain.new_block(proof, previous_hash)

    response = {
        'message': "New Block Forged",
        'index': block['index'],
        'transactions': block['transactions'],
        'proof': block['proof'],
        'previous_hash': block['previous_hash'],
    }
    return jsonify(response), 200


@app.route('/transactions/new', methods=['POST'])
def new_transaction():
    values = request.get_json()

    # Check that the required fields are in the POST'ed data
    required = ['sender', 'recipient', 'amount']
    if not all(k in values for k in required):
        return 'Missing values', 400

    # Create a new Transaction
    index = blockchain.new_transaction(values['sender'], values['recipient'], values['amount'])

    response = {'message':"Transaction will be added to Block {}".format(index)}
    return jsonify(response), 201


@app.route('/chain', methods=['GET'])
def full_chain():
    chain = blockchain.full_chain
    response = {
        'chain': chain,
        'length': len(chain),
    }
    return jsonify(response), 200


@app.route('/clear_chain', methods=['GET'])
def clear_chain():
    blockchain.chain = []
    response = {
        'chain': blockchain.chain,
        'length': len(blockchain.chain),
    }
    return jsonify(response), 200


@app.route('/append_chain', methods=['GET'])
def append_chain():
    values = request.get_json()
    block = values.get('block')
    blockchain.append_chain(block)
    response = {
        'chain': blockchain.chain,
        'length': len(blockchain.chain),
    }
    return jsonify(response), 200


@app.route('/nodes/register', methods=['POST'])
def register_nodes():
    values = request.get_json()

    nodes = values.get('nodes')
    if nodes is None:
        return "Error: Please supply a valid list of nodes", 400

    for node in nodes:
        blockchain.register_node(node)

    response = {
        'message': 'New nodes have been added',
        'total_nodes': list(blockchain.nodes),
    }
    return jsonify(response), 201


@app.route('/nodes/registersubnet', methods=['POST'])
def register_subnet():
    values = request.get_json()

    nodes = values.get('nodes')
    if nodes is None:
        return "Error: Please supply a valid list of nodes", 400

    node_ids = ids(nodes)
    if not node_identifier in node_ids:
        return "Node is not included in subnet", 200

    blockchain.register_micronodes(node_ids)

    response = {
        'message': 'New micronodes have been added',
        'total_nodes': list(blockchain.nodes + blockchain.micronodes),
    }
    return jsonify(response), 201


@app.route('/id', methods=['GET'])
def consensus():
    response = {
        'id': node_identifier
    }

    return jsonify(response), 200


@app.route('/nodes/resolve', methods=['GET'])
def consensus():
    replaced = blockchain.resolve_conflicts()

    if replaced:
        response = {
            'message': 'Our chain was replaced',
            'new_chain': blockchain.full_chain
        }
    else:
        response = {
            'message': 'Our chain is authoritative',
            'chain': blockchain.full_chain
        }

    return jsonify(response), 200


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('-p', '--port', default=5000, type=int, help='port to listen on')
    args = parser.parse_args()
    port = args.port

    app.run(host='0.0.0.0', port=port)
