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

def get_settings():
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, 'resources/log_group_to_host.json')
    log_group_to_host = open(filename, 'r').read()
    return json.loads(log_group_to_host)

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


def lambda_handler(event, context):
    # capture the CloudWatch log data
    out_event = str(event['awslogs']['data'])
    # decode and unzip the log data
    decoded = base64.b64decode(out_event)
    file = io.BytesIO(decoded)
    out_event = gzip.GzipFile(fileobj=file).read()

    # Get the host configuartions

    log_group_to_host_json = get_settings()
    logging.info("Host: %s" % log_group_to_host_json)
    for host, log_group in log_group_to_host_json.items():
        logging.info("host: {}, log_group: {}".format(host, log_group))


    # convert the log data from JSON into a dictionary
    clean_event = json.loads(out_event)
    log_group = clean_event["logGroup"]
    
           
    if log_group in log_group_to_host_json.keys():

        logging.info("Message Log group - " + str(log_group))
        hostname = log_group_to_host_json[log_group]["hostname"]
        port = log_group_to_host_json[log_group]["port"]

        log_events = clean_event["logEvents"]

        for log_event in log_events:
            message = log_event["message"]
            '''
            Convert multiline events to single line events. Needed for Arcsight connectors to process!
            If host is not Arcsight, then this can be ignored.
            '''
            message = str(message).replace("\n","\t")
            message = str(message) + "\n"
            
            write_data(hostname, port, message)