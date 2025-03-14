"""Copyright 2020 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys
import logging
from timeit import default_timer as timer
import gc
import socket
import pickle
import select
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s : [%(levelname)s]  %(message)s',
    handlers=[
        logging.FileHandler('server_shed.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


class Server:
    """Federated server/aggregator that used to aggregate local models and carry out the federated
    learning process (With partition sheduling)
    """

    #pylint: disable=too-many-positional-arguments
    def __init__(self, model_weights, rounds, weights_path, graph_id, num_clients=2,
                 ip=socket.gethostname(), port=5000, header_length=10):

        # Parameters
        self.header_length = header_length
        self.ip = ip
        self.port = port
        self.num_clients = num_clients
        self.rounds = rounds

        self.weights_path = weights_path
        self.graph_id = graph_id

        # Global model
        self.global_weigths = model_weights

        self.global_modlel_ready = False

        self.weights = []
        self.partition_sizes = []
        self.training_cycles = 0

        self.stop_flag = False
        self.finished_client_count = 0

        # List of sockets for select.select()
        self.sockets_list = []
        self.clients = {}
        self.client_ids = {}

        # Craete server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(self.num_clients)

        self.sockets_list.append(self.server_socket)

    def update_model(self, new_weights, partition_size):
        """Update global model
        :param new_weights: new weights as a numpy array
        :param partition_size: graph partition sizes as a list
        :return: None
        """

        self.partition_sizes.extend(partition_size)

        for partition_sz, new_weight in zip(partition_size, new_weights):
            self.weights.append(partition_sz * new_weight)

        self.finished_client_count += 1

        if self.finished_client_count == self.num_clients:

            avg_weight = sum(self.weights) / sum(self.partition_sizes)

            self.weights = []
            self.partition_sizes = []

            self.finished_client_count = 0

            self.global_weigths = avg_weight

            self.training_cycles += 1

            weights_path = self.weights_path + 'weights_' + 'graphID:' + \
                self.graph_id + '_V' + str(self.training_cycles) + '.npy'
            np.save(weights_path, avg_weight)

            for soc in self.sockets_list[1:]:
                self.send_model(soc)

            logging.info(
                '____________________________ Training round %s done ____________________________',
                self.training_cycles)

    def send_model(self, client_socket):
        """Send global model to a client
        :param client_socket: client socket that global model should be sent
        :return: None
        """

        if self.rounds == self.training_cycles:
            self.stop_flag = True

        weights = np.array(self.global_weigths)

        data = {'STOP_FLAG': self.stop_flag, 'WEIGHTS': weights}

        data = pickle.dumps(data)
        data = bytes(f'{len(data):<{self.header_length}}', 'utf-8') + data

        client_socket.sendall(data)

        logging.info('Sent global model to client-%s at %s:%s',
                     self.client_ids[client_socket], *self.clients[client_socket])

    def receive(self, client_socket):
        """Recieve a local model weights from a client
        :param client_socket: client socket that a model weights  should be recieved
        :return: recieved local model weights as a numpy array
        """

        try:

            message_header = client_socket.recv(self.header_length)

            if len(message_header) == 0:
                logging.error('Client-%s closed connection at %s:%s',
                              self.client_ids[client_socket], *self.clients[client_socket])
                return False

            message_length = int(message_header.decode('utf-8').strip())

            full_msg = b''
            while True:
                msg = client_socket.recv(message_length)

                full_msg += msg

                if len(full_msg) == message_length:
                    break

            return pickle.loads(full_msg)

        except Exception:
            logging.error('Client-%s closed connection at %s:%s',
                          self.client_ids[client_socket], *self.clients[client_socket])
            return False

    def run(self):
        """Running server; Listening to clients sockets and act accordingly
        :return: None
        """

        while not self.stop_flag:

            read_sockets, _, exception_sockets = select.select(
                self.sockets_list, [], self.sockets_list)

            for notified_socket in read_sockets:

                if notified_socket == self.server_socket:

                    client_socket, client_address = self.server_socket.accept()
                    self.sockets_list.append(client_socket)
                    self.clients[client_socket] = client_address
                    self.client_ids[client_socket] = 'new'

                    logging.info(
                        'Accepted new connection at %s:%s', *client_address)

                    self.send_model(client_socket)

                else:

                    message = self.receive(notified_socket)

                    if message is False:
                        self.sockets_list.remove(notified_socket)
                        del self.clients[notified_socket]
                        continue

                    client_id = message['CLIENT_ID']
                    weights = message['WEIGHTS']
                    partition_size = message['PARTITION_SIEZES']
                    self.client_ids[notified_socket] = client_id

                    logging.info('Recieved model from client-%s at %s:%s',
                                 client_id, *self.clients[notified_socket])
                    self.update_model(weights, partition_size)

            for notified_socket in exception_sockets:
                self.sockets_list.remove(notified_socket)
                del self.clients[notified_socket]


if __name__ == '__main__':

    from models.supervised import Model

    arg_names = [
        'path_weights',
        'path_nodes',
        'path_edges',
        'graph_id',
        'partition_id',
        'num_clients',
        'num_rounds',
        'IP',
        'PORT'
    ]

    args = dict(zip(arg_names, sys.argv[1:]))

    logging.warning(
        '################################# New Training Session #################################')
    logging.info('Server started , graph ID %s, number of clients %s, number of rounds %s',
                 args['graph_id'], args['num_clients'], args['num_rounds'])

    if 'IP' not in args.keys() or args['IP'] == 'localhost':
        args['IP'] = socket.gethostname()

    if 'PORT' not in args.keys():
        args['PORT'] = 5000

    path_nodes = args['path_nodes'] + args['graph_id'] + \
        '_nodes_' + args['partition_id'] + '.csv'
    nodes = pd.read_csv(path_nodes, index_col=0)
    nodes = nodes.astype('float32')

    path_edges = args['path_edges'] + args['graph_id'] + \
        '_edges_' + args['partition_id'] + '.csv'
    edges = pd.read_csv(path_edges)
    edges = edges.astype({'source': 'uint32', 'target': 'uint32'})

    model = Model(nodes, edges)
    model.initialize()
    model_weights = model.get_weights()

    logging.info('Model initialized')

    server = Server(model_weights, rounds=int(args['num_rounds']),
                    weights_path=args['path_weights'], graph_id=args['graph_id'],
                    num_clients=int(args['num_clients']), ip=args['IP'], port=int(args['PORT']))

    del nodes
    del edges
    del model
    gc.collect()

    logging.info('Federated training started!')

    start = timer()
    server.run()
    end = timer()

    elapsed_time = end - start
    logging.info('Federated training done!')
    logging.info('Training report : Elapsed time %s seconds, graph ID %s, number of clients %s, \
                 number of rounds %s',
                 elapsed_time, args['graph_id'], args['num_clients'], args['num_rounds'])
