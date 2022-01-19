import logging
logging.basicConfig()

from kazoo.client import KazooClient
from time import time, sleep
from multiprocessing import Process


class Philosopher(Process):
    def __init__(self, root: str, thinker: str, _id: int):
        super().__init__()
        self.path = f'{root}/{_id}'
        self.root = root
        self.thinker = thinker
        self.id = _id
        self.left_fork_id = _id
        self.right_fork_id = _id + 1 if _id + 1 < 5 else 0

        self.counter = 0

    def run(self):
        zk = KazooClient()
        zk.start()

        table_lock = zk.Lock(f'/{self.root}', self.id)
        left_thinker = zk.Lock(f'{self.root}/{self.thinker}/{self.left_fork_id}', self.id)
        right_thinker = zk.Lock(f'{self.root}/{self.thinker}/{self.right_fork_id}', self.id)

        start = time()
        while time() - start < 30:
            print(f'Philosopher {self.id} is thinking')
            # blocks waiting for lock acquisition
            with table_lock:
                if len(left_thinker.contenders()) == 0 and len(right_thinker.contenders()) == 0:
                    left_thinker.acquire()
                    right_thinker.acquire()

            if left_thinker.is_acquired:
                print(f'Philosopher {self.id} is eating')
                self.counter += 1
                sleep(1)
                left_thinker.release()
                right_thinker.release()
            sleep(0.5)

        print(self.id, self.counter)
        zk.stop()
        zk.close()


if __name__ == "__main__":
    print("started")
    master_zk = KazooClient()
    master_zk.start()
    for i in range(5):
        p = Philosopher('/table', 'thinker', i)
        p.start()