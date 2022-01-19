import logging
logging.basicConfig()
from kazoo.client import KazooClient
from kazoo.protocol.paths import join

import numpy as np
import threading
from multiprocessing import Process
from time import sleep, time


class Client(Process):
    def __init__(self, root: str, _id: int):
        super().__init__()
        self.path = f'{root}/{_id}'
        self.root = root
        self.id = _id

    def run(self):
        zk = KazooClient()
        zk.start()

        value = b'commit' if np.random.random() > 0.5 else b'abort'
        print(f'Client {self.id} request {value.decode()}')
        zk.create(self.path, value, ephemeral=True)

        @zk.DataWatch(self.path)
        def watch_myself(data, stat):
            if stat.version != 0:
                print(f'Client {self.id} do {data.decode()}')

        sleep(5)

        zk.stop()
        zk.close()


class Coordinator():
    def main(self):
        coordinator = KazooClient()
        coordinator.start()

        number_of_clients = 3
        duration = 20
        self.timer = None

        def check_clients():
            clients = coordinator.get_children('/mynode/pc2')
            commit_counter = 0
            abort_counter = 0
            for client in clients:
                commit_counter += int(coordinator.get(f'/mynode/pc2/{client}')[0] == b'commit')
                abort_counter += int(coordinator.get(f'/mynode/pc2/{client}')[0] == b'abort')

            target = b'commit' if commit_counter > abort_counter else b'abort'
            for client in clients:
                coordinator.set(f'/mynode/pc2/{client}', target)

        @coordinator.ChildrenWatch('/mynode/pc2')
        def watch_clients(clients):
            if len(clients) == 0:
                if self.timer is not None:
                    self.timer.cancel()
            else:
                if self.timer is not None:
                    self.timer.cancel()

                timer = threading.Timer(duration, check_clients)
                timer.daemon = True
                timer.start()

            if len(clients) < number_of_clients:
                print('Waiting for others.', clients)
            elif len(clients) == number_of_clients:
                print('Check clients.')
                timer.cancel()
                check_clients()

        root = '/mynode/pc2'
        for i in range(5):
            p = Client(root, i)
            p.start()


if __name__ == "__main__":
    Coordinator().main()
