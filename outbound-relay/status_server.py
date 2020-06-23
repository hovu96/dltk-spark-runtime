from waitress import serve
from flask import Flask
import threading
import http


class StatusServer(object):

    def __init__(self, source_done, sink_done):
        self.source_done = source_done
        self.sink_done = sink_done
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        app = Flask(__name__)
        app.add_url_rule('/', view_func=self.get_handler, methods=["GET"])
        app.add_url_rule('/source_done', view_func=self.source_done_handler, methods=["PUT"])
        serve(
            app,
            host="0.0.0.0",
            port=8889,
            threads=1,
        )

    def get_handler(self):
        if not self.source_done.is_set():
            return 'receiving', http.HTTPStatus.OK
        if not self.sink_done.is_set():
            return 'sending', http.HTTPStatus.OK
        return 'done', http.HTTPStatus.OK

    def source_done_handler(self):
        self.source_done.set()
        return '', http.HTTPStatus.OK
