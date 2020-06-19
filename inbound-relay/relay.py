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
from guppy import hpy
import gc
import hdfs


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

    def __init__(self, queue, done, hdfs_sink_url, hdfs_path):
        self.queue = queue
        self.done = done
        self.total = 0
        self.buffer_index = 0
        self.lock = threading.Lock()
        self.hdfs_path = hdfs_path
        if hdfs_sink_url:
            self.hdfs_sink_client = hdfs.InsecureClient(hdfs_sink_url)
            self.hdfs_sink_client.delete(self.hdfs_path, recursive=True)
            self.hdfs_sink_client.makedirs(self.hdfs_path)
        else:
            self.hdfs_sink_client = None
        super().__init__(8888)

    def push(self):
        data = request.data
        self.lock.acquire()
        self.total += len(data)
        self.lock.release()
        logging.info("received %s bytes (total=%s)" % (len(data), self.total))
        if self.hdfs_sink_client:
            path = '%s/buffer_%s' % (self.hdfs_path, self.buffer_index)
            with self.hdfs_sink_client.write(path) as writer:
                writer.write(data)
        else:
            self.queue.put(data)
        self.lock.acquire()
        self.buffer_index += 1
        self.lock.release()
        return '', http.HTTPStatus.OK
        chunk = request.get_json(cache=False)
        self.lock.acquire()
        self.total += len(chunk)
        self.lock.release()
        logging.info("received %s events (total=%s)" % (len(chunk), self.total))
        self.queue.put(chunk)
        del chunk
        return '', http.HTTPStatus.OK

    def register_routes(self, app):

        app.add_url_rule('/push', view_func=self.push, methods=["POST"])

        @app.route('/ping', methods=['GET'])
        def ping():
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
            logging.info("sent %s bytes (total=%s)" % (len(chunk), self.total))

            # logging.info("pull -> CHUNK (%s events)" % len(chunk))
            # for e in chunk:
            #    logging.info("  %s" % e)
            queue.task_done()
            return chunk, 200
            # jsonify(chunk)


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


class HDFSSink(object):

    def __init__(self, queue, url):
        self.queue = queue
        self.url = url
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        buffer_index = 0
        client = hdfs.InsecureClient(self.url)
        client.delete("/test", recursive=True)
        client.makedirs("/test")
        while True:
            chunk = queue.get()
            logging.info("sending chunk of %s bytes to hdfs ..." % len(chunk))
            path = '/test/buffer_%s' % buffer_index
            with client.write(path) as writer:
                writer.write(chunk)
            buffer_index += 1
            queue.task_done()
        logging.info("buffer_count=%s" % buffer_index)


if __name__ == "__main__":
    # h = hpy()

    logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"),
        format='%(asctime)s %(levelname)-8s %(message)s',
    )

    hdfs_sink_url = os.environ.get("HDFS_SINK_URL", "")
    hdfs_path = os.environ.get("HDFS_PATH", "")

    queue_path = "/data/queue"
    temp_path = "/data/tmp"

    if not os.path.exists(queue_path):
        os.makedirs(queue_path)
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    queue = Queue(queue_path, tempdir=temp_path)
    done = threading.Event()

    source = HTTPSource(queue, done, hdfs_sink_url, hdfs_path)
    sink = HTTPSink(queue, done)
    streaming_sink = TCPSink(queue, done)

    #hdfs_sink_url = os.environ.get("HDFS_SINK_URL", "")
    # if hdfs_sink_url:
    #    hdfs_sink = HDFSSink(queue, hdfs_sink_url)

    # def signal_handler(signum, frame):
    #    logging.info('received signal %s' % signum)
    #    sys.exit(0)
    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)
    # signal.signal(signal.SIGKILL, signal_handler)

    while True:
        # logging.info("%s", h.heap())
        time.sleep(30)
