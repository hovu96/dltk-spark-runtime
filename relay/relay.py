from waitress import serve
from flask import Flask, jsonify, request
import os
import logging
import threading
import time
from pqueue import Queue
from queue import Empty
import http
import signal
import sys
import socketserver
import json
import asyncio


class HTTPServer(object):

    def __init__(self, port):
        self.port = port
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def register_routes(self, app):
        pass

    def run(self):
        app = Flask(__name__)
        self.register_routes(app)
        serve(
            app,
            host="0.0.0.0",
            port=self.port,
            channel_timeout=100000,
            # threads=concurrent_algo_executions,
        )


class HTTPSource(HTTPServer):

    def __init__(self, queue, done):
        self.queue = queue
        self.done = done
        self.total = 0
        self.lock = threading.Lock()
        super().__init__(8888)

    def register_routes(self, app):

        @app.route('/ping', methods=['GET'])
        def ping():
            return '', http.HTTPStatus.OK

        @app.route('/push', methods=['POST'])
        def push():
            chunk = request.get_json()
            self.lock.acquire()
            self.total += len(chunk)
            self.lock.release()
            logging.info("received %s events (total=%s)" % (len(chunk), self.total))
            self.queue.put(chunk)
            return '', http.HTTPStatus.OK

        @app.route('/done', methods=['PUT'])
        def done():
            logging.info("mark as done")
            self.done.set()
            return '', http.HTTPStatus.OK


class HTTPSink(HTTPServer):

    def __init__(self, queue, done):
        self.queue = queue
        self.done = done
        self.lock = threading.Lock()
        self.total = 0
        super().__init__(8889)

    def register_routes(self, app):

        @app.route('/ping', methods=['GET'])
        def ping():
            return '', http.HTTPStatus.OK

        @app.route('/status', methods=['GET'])
        def status():
            if self.done.wait(timeout=10.0):
                self.queue.join()
                return '', http.HTTPStatus.GONE
            return '', http.HTTPStatus.OK

        @app.route('/pull', methods=['POST'])
        def pull():
            try:
                chunk = self.queue.get_nowait()
            except Empty:
                if self.done.is_set():
                    logging.info("pull -> GONE")
                    return '', http.HTTPStatus.GONE
                # logging.info("pull -> NO_CONTENT")
                return '', http.HTTPStatus.NO_CONTENT

            self.lock.acquire()
            self.total += len(chunk)
            self.lock.release()
            logging.info("sent %s events (total=%s)" % (len(chunk), self.total))

            #logging.info("pull -> CHUNK (%s events)" % len(chunk))
            # for e in chunk:
            #    logging.info("  %s" % e)
            queue.task_done()
            return jsonify(chunk)


class TCPSinkHandler(socketserver.StreamRequestHandler):
    def handle(self):
        logging.info("client connected")
        queue = self.server.queue
        done = self.server.done
        while True:
            events = queue.get()
            for e in events:
                line = json.dumps(e)
                # logging.info("event line: %s" % line)
                data = (line + "\n").encode()
                self.wfile.write(data)
            self.wfile.flush()
            logging.info("sent %s events" % len(events))
            queue.task_done()


class TCPSink(object):

    def __init__(self, queue, done):
        self.queue = queue
        self.done = done
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        with socketserver.TCPServer(('0.0.0.0', 8890), TCPSinkHandler) as server:
            server.queue = self.queue
            server.done = self.done
            server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"),
        format='%(asctime)s %(levelname)-8s %(message)s',
    )

    queue_path = "/data/queue"
    temp_path = "/data/tmp"

    if not os.path.exists(queue_path):
        os.makedirs(queue_path)
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    queue = Queue(queue_path, tempdir=temp_path)
    done = threading.Event()

    source = HTTPSource(queue, done)
    sink = HTTPSink(queue, done)
    streaming_sink = TCPSink(queue, done)

    # def signal_handler(signum, frame):
    #    logging.info('received signal %s' % signum)
    #    sys.exit(0)
    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)
    # signal.signal(signal.SIGKILL, signal_handler)

    while True:
        time.sleep(30)
