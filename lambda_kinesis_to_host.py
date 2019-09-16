import base64
import io
import gzip
import json
import logging
import ssl
import socket
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def write_data(hostname, port, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.connect((hostname, port))
        s.sendall(message.encode('utf-8'))
    except socket.error as e:
        s.close()
        raise Exception("Could not send data %s" % e)
    finally:
        s.close()


def get_settings():
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, 'resources/log_group_to_host.json')
    log_group_to_host = open(filename, 'r').read()
    return json.loads(log_group_to_host)


def send_data(event):

    log_group_to_host_json = get_settings()
    logging.info("Host: %s" % log_group_to_host_json)
    for host, log_group in log_group_to_host_json.items():
        logging.info("host: {}, log_group: {}".format(host, log_group))

    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"])
        file = io.BytesIO(payload)
        out_event = gzip.GzipFile(fileobj=file).read()
        clean_event = json.loads(out_event)
        log_group = clean_event["logGroup"]

        if log_group in log_group_to_host_json.keys():
            logging.info("Record: " + str(record))
            logging.info("Decoded payload: " + str(payload))
            logging.info("Unzipped: " + str(out_event))

            hostname = log_group_to_host_json[log_group]["hostname"]
            port = log_group_to_host_json[log_group]["port"]

            log_events = clean_event["logEvents"]

            for log_event in log_events:
                message = log_event["message"]
                logging.info("Message: " + str(message))
                logging.info("sending to ...." + log_group_to_host_json[log_group])

                write_data(hostname, port, message)


def lambda_handler(event, context):
    send_data(event)
