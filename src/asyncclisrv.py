

import zmq
import threading
import time
from random import choice
import msgpack

class CrowdSrv(object):
    def __init__(self, address='localhost:5555', identity=None):
        if identity is None:
            identity = 'worker-%d' % (choice([0,1,2,3,4,5,6,7,8,9]))
        self.identity = identity
        print 'Client %s started' % (identity)
        self.context = zmq.Context()
        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.IDENTITY, identity)
        socket.connect('tcp://' + address)
        self.socket = socket

    def hello(self):
        print "%s: hello" % self.identity
        msg = msgpack.packb([0, self.identity])
        self.socket.send(msg)
        msg = self.socket.recv()
        msg = msg[0:3]
        _, code = msgpack.unpackb(msg)
        if code != 0:
            raise Exception("problem with hello: %d" % code)

    def lock(self, path):
        print "%s: lock" % self.identity
        msg = msgpack.packb([1, path])
        self.socket.send(msg)
        msg = self.socket.recv()
        msg = msg[0:3]
        _, code = msgpack.unpackb(msg)
        if code != 0:
            raise Exception("problem with lock: %d" % code)

    def try_lock(self, path):
        print "%s: try lock" % self.identity
        msg = msgpack.packb([2, path])
        self.socket.send(msg)
        msg = self.socket.recv()
        msg = msg[0:3]
        _, code = msgpack.unpackb(msg)
        if code == 0:
            print "%s: locked" % self.identity
            return True
        elif code == 1:
            print "%s: lock failed" % self.identity
            return False
        else:
            raise Exception("problem with try_lock: %d" % code)

    def unlock(self, path):
        print "%s: unlock" % self.identity
        msg = msgpack.packb([3, path])
        self.socket.send(msg)
        msg = self.socket.recv()
        msg = msg[0:3]
        _, code = msgpack.unpackb(msg)
        if code != 0:
            raise Exception("problem with unlock: %d" % code)

    def keep_alive(self):
        print "%s: keep alive" % self.identity
        msg = msgpack.packb([4])
        self.socket.send(msg)
        msg = self.socket.recv()
        msg = msg[0:3]
        _, code = msgpack.unpackb(msg)
        if code != 0:
            raise Exception("problem with keep_alive: %d" % code)

    def bye(self):
        print "%s: bye" % self.identity
        msg = msgpack.packb([6])
        self.socket.send(msg)
        msg = self.socket.recv()
        self.socket.close()
        self.context.term()
        msg = msg[0:3]
        _, code = msgpack.unpackb(msg)
        if code != 0:
            raise Exception("problem with bye: %d" % code)


class ClientTask(threading.Thread):
    def __init__(self):
        threading.Thread.__init__ (self)

    def run(self):
        srv = CrowdSrv()

        srv.hello()

        for reqs in xrange(1000):
            print 'Req #%d sent..' % (reqs)
            if srv.try_lock("/a/b/c"):
                time.sleep(1)
                srv.unlock("/a/b/c")

            srv.keep_alive()

            srv.lock("/a/b/c")
            time.sleep(1)
            srv.unlock("/a/b/c")

            srv.keep_alive()

        srv.bye()


def main():
    """main function"""
    for i in xrange(3):
        client = ClientTask()
        client.start()

if __name__ == "__main__":
    main()
