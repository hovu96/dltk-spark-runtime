import urllib
import urllib.request
import logging
import time


def get_url(relay_hostname):
    relay_url = "http://%s:82/" % relay_hostname
    return relay_url


def get(relay_hostname):
    relay_url = get_url(relay_hostname)
    request = urllib.request.Request(relay_url, method="GET")
    try:
        response = urllib.request.urlopen(request)
        response_bytes = response.read()
        response_text = response_bytes.decode()
        response_text, response.getcode()
        status, code = response_text, response.getcode()
    except urllib.error.HTTPError as e:
        status, code = "", e.code
    return status, code


def wait_for_status(relay_hostname, target_status):
    logging.info("waiting for relay status '%s' ..." % target_status)
    retries = 0
    while True:
        status, code = get(relay_hostname)
        if code != 200:
            raise Exception("HTTPError: %s" % code)
        if status != target_status:
            if retries > 600:
                raise Exception("Error sending ping: %s" % code)
        else:
            break
        retries += 1
        time.sleep(1)


def wait_until_done(hostname):
    return wait_for_status(hostname, "done")


def wait_until_running(relay_hostname):
    logging.info("waiting for relay %s running ..." % (relay_hostname))
    retries = 0
    while True:
        _, code = get(relay_hostname)
        if code == 200:
            break
        if code == 404 or code == 503 or code == 502 or code == 504:
            if retries > 600:
                raise Exception("Error sending ping: %s" % code)
        else:
            raise Exception("HTTPError: %s" % code)
        retries += 1
        time.sleep(1)


def signal_output_done(relay_hostname):
    relay_url = get_url(relay_hostname)
    done_url = urllib.parse.urljoin(relay_url, "source_done")
    request = urllib.request.Request(done_url, method="PUT")
    try:
        urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        raise Exception("unexpected error: %s" % e)
