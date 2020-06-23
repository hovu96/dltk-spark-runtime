import os
import logging
import threading
import time
from queue import Empty, Queue
import sys

from hdfs_sink import HDFSSink
from http_source import HTTPSource
from tcp_sink import TCPSink
from status_server import StatusServer


if __name__ == "__main__":
    # h = hpy()

    logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"),
        format='%(asctime)s %(levelname)-8s %(message)s',
    )

    queue = Queue()
    source_done = threading.Event()
    sink_done = threading.Event()

    source = HTTPSource(queue, source_done)
    status_server = StatusServer(source_done, sink_done)

    sink_type = os.environ.get("SINK_TYPE", "")
    if sink_type == "TCP_SERVER":
        tcp_sink = TCPSink(queue)
    elif sink_type == "HDFS_CLIENT":
        hdfs_path = os.environ.get("HDFS_PATH", "")
        hdfs_base_url = os.environ.get("HDFS_SINK_URL", "")
        hdfs_sink = HDFSSink(queue, hdfs_base_url, hdfs_path)
    else:
        logging.error("unexpected sink type: %s" % sink_type)
        sys.exit(1)

    # def signal_handler(signum, frame):
    #    logging.info('received signal %s' % signum)
    #    sys.exit(0)
    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)
    # signal.signal(signal.SIGKILL, signal_handler)

    logging.info("waiting until all data is received...")
    source_done.wait()
    logging.info("waiting until all data is sent/tranferred...")
    queue.join()
    logging.info("mark sink as done")
    sink_done.set()

    while True:
        time.sleep(30)
