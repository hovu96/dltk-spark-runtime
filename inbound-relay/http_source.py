from waitress import serve
from flask import Flask, request
import logging
import threading
import http


class HTTPSource(object):

    def __init__(self, queue, source_done):
        self.queue = queue
        self.source_done = source_done
        self.total = 0
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        app = Flask(__name__)
        app.add_url_rule('/push', view_func=self.push_handler, methods=["POST"])
        app.add_url_rule('/ping', view_func=self.ping_handler, methods=["GET"])
        app.add_url_rule('/done', view_func=self.done_handler, methods=["PUT"])
        logging.info("HTTPSource: waiting for data from Splunk ...")
        serve(
            app,
            host="0.0.0.0",
            port=8888,
            channel_timeout=100000,
            # threads=concurrent_algo_executions,
        )

    def push_handler(self):
        data = request.data
        self.lock.acquire()
        self.total += len(data)
        self.lock.release()
        logging.info("HTTPSource: received chunk of %s bytes (total bytes: %s)" % (len(data), self.total))
        self.queue.put(data)
        return '', http.HTTPStatus.OK

    def ping_handler(self):
        return '', http.HTTPStatus.OK

    def done_handler(self):
        logging.info("HTTPSource: source is done (won't receive more data)")
        self.queue.put(None)
        self.source_done.set()
        return '', http.HTTPStatus.OK
