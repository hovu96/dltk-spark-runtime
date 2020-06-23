from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.listener import StreamingListener
from waitress import serve
from flask import Flask, jsonify
import os
import logging
import time
import urllib
import http
import json
import threading
import socket
import io

app = Flask(__name__)


# @app.route('/execute/<method>', methods=['POST'])
# def execute(method):
#    import algo_code
#    method_impl = getattr(algo_code, method)
#    algo_result = method_impl(sc)
#    return jsonify(algo_result)


def wait_until_relay_done(relay_url):
    while True:
        time.sleep(1)
        status = get_status(relay_url)
        if status == 410:
            logging.info("queue status DONE")
            break


def receive_events_generator(relay_url):
    pull_url = urllib.parse.urljoin(relay_url, "pull")
    while True:
        try:
            request = urllib.request.Request(
                pull_url,
                method="POST",
                headers={}
            )
            response = urllib.request.urlopen(request)
            if response.getcode() == 204:
                time.sleep(1)
            else:
                # response_content_type = response.headers["Content-Type"] if "Content-Type" in response.headers else ""
                # if response_content_type != "application/json":
                #    raise Exception("Unexpected content type: %s" % response_content_type)
                response_bytes = response.read()
                logging.info("received chunk of %s bytes" % len(response_bytes))
                yield response_bytes
        except http.client.RemoteDisconnected as e:
            raise Exception("Inbound relay closed connection: %s" % e)
        except urllib.error.HTTPError as e:
            if e.code == 410:
                return
            raise Exception("Inbound relay HTTP error: %s" % e.code)


def receive_events(relay_url):
    all_events = []
    for events in receive_events_generator(relay_url):
        all_events.extend(events)
    return all_events


def get_relay_status(relay_hostname):
    relay_url = "http://%s:82/" % relay_hostname
    #logging.info("calling %s ..." % relay_url)
    request = urllib.request.Request(relay_url, method="GET")
    try:
        response = urllib.request.urlopen(request)
        response_bytes = response.read()
        response_text = response_bytes.decode()
        response_text, response.getcode()
        status, code = response_text, response.getcode()
    except urllib.error.HTTPError as e:
        status, code = "", e.code
    #logging.info("result %s (%s)" % (status, code))
    return status, code


def wait_for_relay_status(relay_hostname, target_status):
    logging.info("waiting for relay status '%s' ..." % target_status)
    retries = 0
    while True:
        status, code = get_relay_status(relay_hostname)
        if code != 200:
            raise Exception("HTTPError: %s" % code)
        if status != target_status:
            if retries > 600:
                raise Exception("Error sending ping: %s" % code)
        else:
            break
        retries += 1
        time.sleep(1)


def wait_for_relay_running(relay_hostname):
    logging.info("waiting for relay %s running ..." % (relay_hostname))
    retries = 0
    while True:
        _, code = get_relay_status(relay_hostname)
        if code == 200:
            break
        if code == 404 or code == 503 or code == 502 or code == 504:
            if retries > 600:
                raise Exception("Error sending ping: %s" % code)
        else:
            raise Exception("HTTPError: %s" % code)
        retries += 1
        time.sleep(1)


def get_status(relay_url):
    status_url = urllib.parse.urljoin(relay_url, "status")
    request = urllib.request.Request(
        status_url,
        method="GET",
    )
    try:
        response = urllib.request.urlopen(request)
        return response.getcode()
    except urllib.error.HTTPError as e:
        return e.code


def read_chunks_from_relay(hostname):
    logging.info("connecting to %s:81 ... " % hostname)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((hostname, 81))
    try:
        with s.makefile(mode="rb") as f:
            while True:
                logging.info("reading size line...")
                size_line = f.readline().decode()
                if len(size_line) == 0:
                    logging.info("no more chunks")
                    break
                size = int(size_line)
                logging.info("now reading %s bytes ..." % size)
                data = f.read(size)
                yield data
    finally:
        s.close()


def generate_lines_from_chunk(data):
    with io.BytesIO(data) as reader:
        for line in reader:
            yield line


def parse_events_from_lines(lines):
    return lines.map(lambda l: json.loads(l))


def distribute_and_parse_lines(spark_context, line_iterator):
    lines = spark_context.parallelize(line_iterator)
    return parse_events_from_lines(lines)


def generate_event_chunks_from_relay(spark_context, inbound_relay_hostname):
    wait_for_relay_running(inbound_relay_hostname)
    buffer_count = 0
    for data in read_chunks_from_relay(inbound_relay_hostname):
        buffer_count += 1
        line_iterator = generate_lines_from_chunk(data)
        yield distribute_and_parse_lines(spark_context, line_iterator)
    logging.info("got %s chunks" % (buffer_count))


def generate_lines_from_relay(hostname):
    line_count = 0
    buffer_count = 0
    for data in read_chunks_from_relay(hostname):
        buffer_count += 1
        for line in generate_lines_from_chunk(data):
            yield line
            line_count += 1
    logging.info("buffer_count=%s line_count=%s" % (buffer_count, line_count))


def generate_events_from_relay(spark_context, inbound_relay_hostname):
    wait_for_relay_running(inbound_relay_hostname)
    line_iterator = generate_lines_from_relay(inbound_relay_hostname)
    return distribute_and_parse_lines(spark_context, line_iterator)


def events_from_hdfs(spark_context, inbound_relay_hostname, hdfs_url):
    wait_for_relay_running(inbound_relay_hostname)
    wait_for_relay_status(inbound_relay_hostname, "done")
    lines = spark_context.textFile(hdfs_url)
    events = parse_events_from_lines(lines)
    return events


def send_chunks_to_relay(outbound_relay_hostname, chunk_iterator):
    logging.info("connecting to %s:81 ... " % outbound_relay_hostname)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((outbound_relay_hostname, 81))
    try:
        with s.makefile(mode="wb") as f:
            for data in chunk_iterator:
                chunk_size = len(data)
                logging.info("sending chunk (of %s bytes) to sink ..." % chunk_size)
                f.write(("%s\n" % chunk_size).encode())
                f.write(data)
                f.flush()
    finally:
        s.close()


def send_stream(relay_url, stream):
    # https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
    def send_partition(iter):
        events = []
        for record in iter:
            events.append(record)
            if len(events) > 100000:
                #output_chunk(relay_url, events)
                events = []
        if len(events) > 0:
            #output_chunk(relay_url, events)
            pass

    def send_rdd(rdd):
        rdd.foreachPartition(send_partition)
    stream.foreachRDD(send_rdd)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"),
        format='%(asctime)s %(levelname)-8s %(message)s',
    )

    search_id = os.getenv("DLTK_SEARCH_ID", "")
    logging.info("DLTK_SEARCH_ID=%s" % search_id)

    algo_name = os.getenv("DLTK_ALGO", "DLTK")
    logging.info("DLTK_ALGO=%s" % algo_name)
    spark_context = SparkContext(appName=algo_name)

    inbound_relay_sink = os.getenv("DLTK_INBOUND_RELAY_SINK", "")
    logging.info("DLTK_INBOUND_RELAY_SINK=%s" % inbound_relay_sink)

    outbound_relay_hostname = os.getenv("DLTK_OUTBOUND_RELAY")
    logging.info("DLTK_OUTBOUND_RELAY=%s" % outbound_relay_hostname)

    algo_method_name = os.getenv("DLTK_ALGO_METHOD")
    logging.info("DLTK_ALGO_METHOD=%s" % algo_method_name)
    algo_code = __import__("algo_code")
    method_impl = getattr(algo_code, algo_method_name)

    input_type = os.getenv("DLTK_INPUT_TYPE")
    if input_type == "rdd":
        backend_type = os.getenv("DLTK_INPUT_RDD_BACKEND")
        if backend_type == "hdfs":
            hdfs_url_base = os.getenv("DLTK_HDFS_URL", "")
            hdfs_path = os.getenv("DLTK_HDFS_PATH", "")
            hdfs_url = "%s/%s" % (hdfs_url_base.strip("/"), hdfs_path.strip("/"))
            input_events = events_from_hdfs(spark_context, inbound_relay_sink, hdfs_url)
            output_events = method_impl(spark_context, input_events)
        elif backend_type == "buffer":
            input_events = generate_events_from_relay(spark_context, inbound_relay_sink)
            output_events = method_impl(spark_context, input_events)
        elif backend_type == "iterator":
            input_events_iterator = generate_event_chunks_from_relay(spark_context, inbound_relay_sink)
            output_events = method_impl(spark_context, input_events_iterator)
        else:
            raise Exception("unsupported rdd backend \"%s\"" % backend_type)
    elif input_type == "dstream":
        pass
    else:
        raise Exception("unsupported input type \"%s\"" % input_type)

    # from pyspark.rdd import RDD
    # if isinstance(x, RDD):
    logging.info("algo returned %s events" % len(output_events))
    wait_for_relay_running(outbound_relay_hostname)
    send_chunks_to_relay(outbound_relay_hostname, [json.dumps(output_events).encode()])
    logging.info("sent %s events to outbound relay" % len(output_events))
    spark_context.stop()

    # elif input_mode == "streaming":
    #    batch_interval = int(os.getenv("DLTK_BATCH_INTERVAL", 1))
    #    logging.info("DLTK_BATCH_INTERVAL=%s" % batch_interval)
    #    receiver_count = int(os.getenv("DLTK_RECEIVER_COUNT", 2))
    #    logging.info("DLTK_RECEIVER_COUNT=%s" % receiver_count)
    #    wait_time_before_stop = int(os.getenv("DLTK_WAIT_TIME_BEFORE_STOP", 30))
    #    logging.info("DLTK_WAIT_TIME_BEFORE_STOP=%s" % wait_time_before_stop)
    #    checkpoint_url = os.getenv("DLTK_CHECKPOINT_URL", "")
    #    logging.info("DLTK_CHECKPOINT_URL=%s" % checkpoint_url)
    #    # https://spark.apache.org/docs/latest/streaming-programming-guide.html
    #    # https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#pyspark.streaming.StreamingContext
    #    streaming_context = StreamingContext(spark_context, batch_interval)
    #    if checkpoint_url:
    #        streaming_context.checkpoint(checkpoint_url)
    #    input_streams = []
    #    for i in range(receiver_count):
    #        logging.info("create new receiver")
    #        s = streaming_context.socketTextStream(inbound_relay_sink, 81)
    #        input_streams.append(s)
    #    input_stream = streaming_context.union(*input_streams)
    #    event_stream = input_stream.map(lambda line: json.loads(line))
    #    output_stream = method_impl(streaming_context, event_stream)
    #    send_stream(outbound_relay_source_url, output_stream)
    #    wait_for_relay_to_complete_startup(inbound_relay_sink_url)
    #    wait_for_relay_to_complete_startup(outbound_relay_source_url)
    #    streaming_context.start()##
    #    def wait_until_all_events_received():
    #        wait_until_relay_done(inbound_relay_sink_url)
    #    def background_poller():
    #        wait_until_all_events_received()
    #        logging.info("waiting to finish up...")
    #        time.sleep(wait_time_before_stop)
    #        logging.info("stopping context...")
    #        streaming_context.stop(stopSparkContext=False, stopGraceFully=True)
    #    # background_poller_thread = threading.Thread(target=background_poller, args=())
    #    # background_poller_thread.daemon = True
    #    # background_poller_thread.start()
    #    # streaming_context.awaitTermination()
    #    wait_until_all_events_received()
    #    logging.info("waiting to finish up...")
    #    time.sleep(wait_time_before_stop)
    #    logging.info("stopping context...")
    #    streaming_context.stop(stopSparkContext=True, stopGraceFully=True)
    #    close_output(outbound_relay_source_url)
    # else:
    #    logging.error("unsupported processing mode")
