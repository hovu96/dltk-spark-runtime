import logging
import threading
import socketserver
import hdfs
import traceback


class TCPSource(object):

    def __init__(self, queue, source_done):
        self.queue = queue
        self.source_done = source_done
        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        queue = self.queue
        source_done = self.source_done

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                logging.info("TCPSource: client connected")
                try:
                    while True:
                        logging.info("TCPSource: waiting for data...")
                        size_line = self.rfile.readline().decode()
                        if len(size_line) == 0:
                            logging.info("TCPSource: done (won't receive more data)")
                            source_done.set()
                            break
                        size = int(size_line)
                        logging.info("TCPSource: receiving chunk of %s bytes ..." % size)
                        data = self.rfile.read(size)
                        queue.put(data)
                except:
                    err_msg = traceback.format_exc()
                    logging.error("TCPSource: error receiving data:\n%s" % err_msg)
                finally:
                    logging.info("TCPSource: client disconnected")

        with socketserver.TCPServer(('0.0.0.0', 8888), Handler) as server:
            server.serve_forever()
