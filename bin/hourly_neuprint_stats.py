import argparse
import datetime
import getpass
import json
import sys
import socket
from time import time
import colorlog
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}


def call_responder(server, endpoint, payload=''):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
        psyload: POST payload
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        if payload:
            headers = {"Content-type": "application/json",
                       "Authorization": "Bearer " + CONFIG[server]['bearer']}
            req = requests.post(url, headers=headers, json=payload)
        else:
            req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code != 200:
        LOGGER.critical('Status: %s', str(req.status_code))
        sys.exit(-1)
    else:
        return req.json()


def initialize_program():
    dbc = call_responder('config', 'config/rest_services')
    CONFIG = dbc['config']
    dbc = call_responder('config', 'config/servers')
    BROKERS = dbc['config']['Kafka']['broker_list']
    return(CONFIG, BROKERS)


def fetch_top_level(payload, datestruct, datasetn, suffix):
    traced = ['Prelim Roughly traced', 'Traced', 'Roughly traced']
    statuses = ['0.5assign', 'Anchor', 'Leaves', 'Orphan', 'Orphan hotknife',
                'Prelim Roughly traced', 'Traced', 'Unimportant', 'Roughly traced']
    # Neurons
    datestruct['NEURONS_TOTAL_traced'] = 0
    for status in statuses:
        key = status.lower().replace(' ', '_')
        payload = {"cypher": "MATCH (n:`" + datasetn + "`{status:\"" + status + "\"})" + suffix}
        response = call_responder('neuprint', 'custom/custom', payload)
        datestruct['NEURONS_' + key] = response['data'][0][0]
        if status in traced:
            datestruct['NEURONS_TOTAL_traced'] += response['data'][0][0]
    # Synapses
    completeness = traced + ['Leaves']
    statuses = completeness
    for ctype in ['complete', 'traced']:
        for ntype in ['pre', 'post']:
            datestruct['TOTAL_' + ntype + '_' + ctype] = 0
        datestruct['TOTAL_' + ctype] = 0
    # <status>.<pre|post>.<complete|traced>
    for status in statuses:
        key = status.lower().replace(' ', '_')
        for ntype in ['pre', 'post']:
                datestruct['.'.join([key, ntype])] = 0
    # Processing loop
    for status in statuses:
        key = status.lower().replace(' ', '_')
        synapses = {"total": 0, "complete": 0, "traced": 0}
        payload = {"cypher": "MATCH (n:`" + datasetn + "`{status:\"" + status + "\"})" +
                       "-[:Contains]->(:SynapseSet)-[:Contains]->(s:Synapse) RETURN count(s)"}
        response = call_responder('neuprint', 'custom/custom', payload)
        for ntype in ['Pre', 'Post']:
            payload = {"cypher": "MATCH (n:`" + datasetn + "`{status:\"" + status + "\"})" +
                       "-[:Contains]->(:SynapseSet)-[:Contains]->(s:" + ntype + "Syn) RETURN count(s)"}
            response = call_responder('neuprint', 'custom/custom', payload)
            print(key + ', ' + ntype + ' = ' + str(response['data'][0][0]))
            synapses['total'] += response['data'][0][0]
            datestruct['.'.join([key, ntype.lower()])] += response['data'][0][0]
            if status in completeness:
                datestruct['TOTAL_' + ntype.lower() + '_complete'] += response['data'][0][0]
                datestruct['TOTAL_complete'] += response['data'][0][0]
            if status in traced:
                datestruct['TOTAL_' + ntype.lower() + '_traced'] += response['data'][0][0]
                datestruct['TOTAL_traced'] += response['data'][0][0]
    payload = {"cypher": "MATCH (n:Meta:" + ARG.DATASET + ") RETURN n.totalPreCount, n.totalPostCount"}
    response = call_responder('neuprint', 'custom/custom', payload)
    datestruct['TOTAL_pre'] = response['data'][0][0]
    datestruct['TOTAL_post'] = response['data'][0][1]
    datestruct['TOTAL_synapses'] = response['data'][0][0] + response['data'][0][1]
    datestruct['TOTAL_pre_complete_percent'] = '%.3f' % (datestruct['TOTAL_pre'] / datestruct['TOTAL_pre_complete'] * 100.0)
    datestruct['TOTAL_post_complete_percent'] = '%.3f' % (datestruct['TOTAL_post'] / datestruct['TOTAL_post_complete'] * 100.0)
    datestruct['INCOMPLETE_pre'] = datestruct['TOTAL_pre'] - datestruct['TOTAL_pre_complete']
    datestruct['INCOMPLETE_post'] = datestruct['TOTAL_post'] - datestruct['TOTAL_post_complete']
    datestruct['INCOMPLETE_synapses'] = datestruct['TOTAL_synapses'] - datestruct['TOTAL_complete']


def process_data(dataset):
    if ARG.WRITE:
        producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                 key_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                 bootstrap_servers=BROKERS)
    payload = dict()
    status_counts = dict()
    datasetn = dataset + '-Neuron'
    suffix = ' RETURN count(n)'
    fetch_top_level(payload, status_counts, datasetn, suffix)
    kafka = {"client": 'hourly_neuprint_stats', "user": getpass.getuser(),
             "time": time(), "host": socket.gethostname()}
    kafka.update(status_counts)
    if ARG.WRITE:
        LOGGER.debug(json.dumps(kafka))
        future = producer.send('nptest', kafka, str(datetime.datetime.now()))
        try:
            future.get(timeout=10)
        except KafkaError:
            LOGGER.critical("Failed!")
    else:
        LOGGER.info(json.dumps(kafka))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Write dataset daily stats to config system")
    PARSER.add_argument('--dataset', dest='DATASET', action='store',
                        default='hemibrain', help='Dataset')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write record to config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    (CONFIG, BROKERS) = initialize_program()
    process_data(ARG.DATASET)
