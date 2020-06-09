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

app = Flask(__name__)


# @app.route('/execute/<method>', methods=['POST'])
# def execute(method):
#    import algo_code
#    method_impl = getattr(algo_code, method)
#    algo_result = method_impl(sc)
#    return jsonify(algo_result)


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
                #response_content_type = response.headers["Content-Type"] if "Content-Type" in response.headers else ""
                #if response_content_type != "application/json":
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


def wait_for_relay(relay_url):
    ping_url = urllib.parse.urljoin(relay_url, "ping")
    retries = 0
    while True:
        try:
            request = urllib.request.Request(ping_url, method="GET")
            response = urllib.request.urlopen(request)
            break
        except urllib.error.HTTPError as e:
            if e.code == 404 or e.code == 503 or e.code == 502 or e.code == 504:
                if retries > 600:
                    raise Exception("Error sending ping: %s" % e.code)
            else:
                raise Exception("HTTPError: %s" % e.code)
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


def output_chunk(relay_url, events):
    push_url = urllib.parse.urljoin(relay_url, "push")
    request = urllib.request.Request(
        push_url,
        method="POST",
        data=json.dumps(events).encode(),
        headers={
            "Content-Type": "application/json",
        }
    )
    urllib.request.urlopen(request)
    logging.info("sent %s events" % len(events))


def close_output(relay_url):
    done_url = urllib.parse.urljoin(relay_url, "done")
    request = urllib.request.Request(
        done_url,
        method="PUT",
        headers={}
    )
    response = urllib.request.urlopen(request)


def send_stream(relay_url, stream):
    # https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
    def send_partition(iter):
        events = []
        for record in iter:
            events.append(record)
            if len(events) > 100000:
                output_chunk(relay_url, events)
                events = []
        if len(events) > 0:
            output_chunk(relay_url, events)

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

    inbound_relay_sink_url = os.getenv("DLTK_INBOUND_RELAY_SINK_URL")
    logging.info("DLTK_INBOUND_RELAY_SINK_URL=%s" % inbound_relay_sink_url)

    inbound_relay_sink = os.getenv("DLTK_INBOUND_RELAY_SINK")
    logging.info("DLTK_INBOUND_RELAY_SINK=%s" % inbound_relay_sink)

    outbound_relay_source_url = os.getenv("DLTK_OUTBOUND_RELAY_SOURCE_URL")
    logging.info("DLTK_OUTBOUND_RELAY_SOURCE_URL=%s" % outbound_relay_source_url)

    algo_method_name = os.getenv("DLTK_ALGO_METHOD")
    logging.info("DLTK_ALGO_METHOD=%s" % algo_method_name)
    import algo_code
    method_impl = getattr(algo_code, algo_method_name)

    input_mode = os.getenv("DLTK_INPUT_MODE")
    logging.info("DLTK_INPUT_MODE=%s" % input_mode)
    if input_mode == "batching":
        wait_for_relay(inbound_relay_sink_url)
        input_events = receive_events(inbound_relay_sink_url)
        logging.info("received %s events" % len(input_events))
        logging.info("calling algo method \"%s\"..." % algo_method_name)
        output_events = method_impl(spark_context, input_events)
        logging.info("algo returned %s events" % len(output_events))
        wait_for_relay(outbound_relay_source_url)
        output_chunk(outbound_relay_source_url, output_events)
        logging.info("sent %s events to outbound relay" % len(output_events))
        close_output(outbound_relay_source_url)
        spark_context.stop()
    elif input_mode == "iterating":
        wait_for_relay(inbound_relay_sink_url)
        event_list_iterator = receive_events_generator(inbound_relay_sink_url)
        logging.info("calling algo method \"%s\"..." % algo_method_name)
        output_events = method_impl(spark_context, event_list_iterator)
        logging.info("algo returned %s events" % len(output_events))
        wait_for_relay(outbound_relay_source_url)
        output_chunk(outbound_relay_source_url, output_events)
        logging.info("sent %s events to outbound relay" % len(output_events))
        close_output(outbound_relay_source_url)
        spark_context.stop()
    elif input_mode == "streaming":
        batch_interval = int(os.getenv("DLTK_BATCH_INTERVAL", 1))
        logging.info("DLTK_BATCH_INTERVAL=%s" % batch_interval)
        receiver_count = int(os.getenv("DLTK_RECEIVER_COUNT", 2))
        logging.info("DLTK_RECEIVER_COUNT=%s" % receiver_count)
        wait_time_before_stop = int(os.getenv("DLTK_WAIT_TIME_BEFORE_STOP", 30))
        logging.info("DLTK_WAIT_TIME_BEFORE_STOP=%s" % wait_time_before_stop)
        checkpoint_url = os.getenv("DLTK_CHECKPOINT_URL", "")
        logging.info("DLTK_CHECKPOINT_URL=%s" % checkpoint_url)
        # https://spark.apache.org/docs/latest/streaming-programming-guide.html
        # https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#pyspark.streaming.StreamingContext
        streaming_context = StreamingContext(spark_context, batch_interval)
        if checkpoint_url:
            streaming_context.checkpoint(checkpoint_url)
        input_streams = []
        for i in range(receiver_count):
            logging.info("create new receiver")
            s = streaming_context.socketTextStream(inbound_relay_sink, 81)
            input_streams.append(s)
        input_stream = streaming_context.union(*input_streams)
        event_stream = input_stream.map(lambda line: json.loads(line))
        output_stream = method_impl(streaming_context, event_stream)
        send_stream(outbound_relay_source_url, output_stream)
        wait_for_relay(inbound_relay_sink_url)
        wait_for_relay(outbound_relay_source_url)
        streaming_context.start()
        def wait_until_all_events_received():
            while True:
                time.sleep(1)
                status = get_status(inbound_relay_sink_url)
                if status == 410:
                    logging.info("queue status DONE")
                    break
        def background_poller():
            wait_until_all_events_received()
            logging.info("waiting to finish up...")
            time.sleep(wait_time_before_stop)
            logging.info("stopping context...")
            streaming_context.stop(stopSparkContext=False, stopGraceFully=True)
        #background_poller_thread = threading.Thread(target=background_poller, args=())
        #background_poller_thread.daemon = True
        # background_poller_thread.start()
        # streaming_context.awaitTermination()
        wait_until_all_events_received()
        logging.info("waiting to finish up...")
        time.sleep(wait_time_before_stop)
        logging.info("stopping context...")
        streaming_context.stop(stopSparkContext=True, stopGraceFully=True)
        close_output(outbound_relay_source_url)
    else:
        logging.error("unsupported processing mode")
