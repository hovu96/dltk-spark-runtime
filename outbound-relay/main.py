import os
import logging
import threading
import time
from queue import Queue

from status_server import StatusServer
from tcp_source import TCPSource
from http_sink import HTTPSink

if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"),
        format='%(asctime)s %(levelname)-8s %(message)s',
    )

    queue = Queue()
    source_done = threading.Event()
    sink_done = threading.Event()

    source = TCPSource(queue, source_done)
    sink = HTTPSink(queue, source_done)
    status_server = StatusServer(source_done, sink_done)

    logging.info("Main: waiting until all data is received by source...")
    source_done.wait()
    logging.info("Main: waiting until all data is sent by sink...")
    queue.join()
    logging.info("Main: mark sink as done")
    sink_done.set()

    while True:
        time.sleep(30)
