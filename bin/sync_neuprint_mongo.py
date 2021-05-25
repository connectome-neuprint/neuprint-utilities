''' Get NeuPrint metadata
'''

import argparse
from datetime import datetime
import os
import socket
import sys
import time
import colorlog
from neuprint import Client, fetch_custom, fetch_meta
from pymongo import MongoClient
import requests
from tqdm import tqdm


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {'mongo': 0, 'neuprint': 0, 'delete': 0, 'insert': 0, 'update': 0}
JWT = 'NEUPRINT_APPLICATION_CREDENTIALS'
KEYS = dict()
# Database
DBM = ''
# Mapping of NeuPrint column number to Mongo name
COLUMN = ["name", "status", "neuronType", "neuronInstance"]

# pylint: disable=W0703

# -----------------------------------------------------------------------------

def call_responder(server, endpoint):
    """ Call a responder and return JSON
        Keyword arguments:
          server: server
          endpoint: endpoint
        Returns:
          JSON
    """
    url = CONFIG[server]['url'] + endpoint
    authenticate = server == 'neuprint'
    try:
        if authenticate:
            headers = {"Content-Type": "application/json",
                       "Authorization": "Bearer " + os.environ[JWT]}
        if authenticate:
            req = requests.get(url, headers=headers)
        else:
            req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize program
    """
    global CONFIG, DBM  # pylint: disable=W0603
    if JWT not in os.environ:
        LOGGER.error("Missing JSON Web Token - set in %s environment variable", JWT)
        sys.exit(-1)
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/db_config')
    # Connect to Mongo
    rwp = 'write' if ARG.WRITE else 'read'
    try:
        if ARG.MANIFOLD != 'local':
            client = MongoClient(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['host'])
        else:
            client = MongoClient()
        DBM = client.jacs
        if ARG.MANIFOLD == 'prod':
            DBM.authenticate(data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['user'],
                             data['config']['jacs-mongo'][ARG.MANIFOLD][rwp]['password'])
    except Exception as err:
        LOGGER.error('Could not connect to Mongo: %s', err)
        sys.exit(-1)


def generate_uid(deployment_context=2, last_uid=None):
    """ Gegerate a JACS-style UID
        Keyword arguments:
          deployment_context: deployment context [0]
          last_uid: last UID generated [None]
        Returns:
          UID
    """
    current_time_offset = 921700000000
    max_tries = 1023
    current_index = 0
    try:
        hostname = socket.gethostname()
        ipa = socket.gethostbyname(hostname)
    except Exception:
        ipa = socket.gethostbyname('localhost')
    ip_component = int(ipa.split('.')[-1]) & 0xFF
    #ip_component = 71 #PLUG
    next_uid = None
    while current_index <= max_tries and not next_uid:
        time_component = int(time.time()*1000) - current_time_offset
        LOGGER.debug("time_component: {0:b}".format(time_component))
        time_component = (time_component << 22)
        LOGGER.debug("current_index: {0:b}".format(current_index))
        LOGGER.debug("deployment_context: {0:b}".format(deployment_context))
        LOGGER.debug("ip_component: {0:b}".format(ip_component))
        next_uid = time_component + (current_index << 12) + (deployment_context << 8) + ip_component
        if last_uid and last_uid == next_uid:
            LOGGER.debug("Soft collision %d (%d)", last_uid, current_index)
            next_uid = None
            current_index += 1
        if not next_uid and (current_index > max_tries):
            LOGGER.debug("Hard collision %d (%d)", last_uid, current_index)
            time.sleep(0.5)
            current_index = 0
    if not next_uid:
        LOGGER.critical("Could not generate UID")
        sys.exit(-1)
    LOGGER.debug("UID: %d", next_uid)
    if next_uid in KEYS:
        LOGGER.critical("Duplicate UID %d", next_uid)
        sys.exit(-1)
    else:
        KEYS[next_uid] = 1
    return next_uid


def to_datetime(dt_string):
    """ Convert a string to a datetime object
        Keyword arguments:
          dt_string: datetime string
        Returns:
          datetime object
    """
    return datetime.strptime(dt_string.replace('T', ' '), "%Y-%m-%d %H:%M:%S")


def setup_dataset(dataset, published):
    """ Insert or update a dataset in Mongo
        Keyword arguments:
          dataset: dataset
          published: True=public, False=private
        Returns:
          last_uid: last UID assigned
          action: what to do with bodies in this dataset (ignore, insert, or update)
    """
    npc = Client(ARG.SERVER, dataset=dataset)
    result = fetch_meta(client=npc)
    if ':' in dataset:
        name, version = dataset.split(':')
        version = version.replace('v', '')
    else:
        name = dataset
        version = ''
    coll = DBM.emDataSet
    check = coll.find_one({"name": name, "version": version, "published": published})
    action = 'ignore'
    if not check:
        payload = {"class" : "org.janelia.model.domain.flyem.EMDataSet",
                   "ownerKey": "group:flyem", "readers": ["group:flyem"],
                   "writers": ["group:flyem"],
                   "name" : result['dataset'], "version": version,
                   "creationDate": to_datetime(result['lastDatabaseEdit']),
                   "updatedDate": to_datetime(result['lastDatabaseEdit']),
                   "published": published
                  }
        if published:
            payload['readers'] = ["group:flyem", "group:workstation_users"]
        last_uid = generate_uid()
        payload['_id'] = last_uid
        LOGGER.debug(payload)
        if ARG.WRITE:
            post_id = coll.insert_one(payload).inserted_id
        else:
            post_id = last_uid
        if post_id != last_uid:
            LOGGER.critical("Could not insert to Mongo with requested _id")
            sys.exit(0)
        LOGGER.info("Inserted dataset %s (UID: %s, datetime: %sß)", dataset, post_id,
                    result['lastDatabaseEdit'])
        action = 'insert'
    else:
        LOGGER.info("%s already exists in Mongo (UID: %s)", dataset, check['_id'])
        last_uid = check['_id']
        neuprint_dt = to_datetime(result['lastDatabaseEdit'])
        if neuprint_dt > check['updatedDate']:
            LOGGER.warning("Update required for %s (last changed %s)",
                           dataset, result['lastDatabaseEdit'])
            payload = {"updatedDate": neuprint_dt}
            if ARG.WRITE:
                coll.update_one({"_id": check['_id']},
                                {"$set": payload})
            action = 'update'
        else:
            if ARG.FORCE:
                action = 'update'
            else:
                LOGGER.info("No update required for %s (last changed %s)",
                            dataset, check['updatedDate'])
    return last_uid, action


def fetch_mongo_bodies(dataset, dataset_uid):
    """ Create a dictionary (keyed by body ID) and set of bodies from Mongo
        Keyword arguments:
          dataset: dataset
          dataset_uid: dataset UID
        Returns:
          rowdict: dictionary of rows from Mongo
          mongo_bodyset: set of body IDs
    """
    rows = DBM.emBody.find({"dataSetRef": 'EMDataSet#' + str(dataset_uid)})
    mongo_bodyset = set()
    rowdict = dict()
    for row in rows:
        mongo_bodyset.add(row['name'])
        rowdict[row['name']] = row
    LOGGER.info("%d Body IDs found in MongoDB %s", len(mongo_bodyset), dataset)
    return rowdict, mongo_bodyset


def fetch_neuprint_bodies(dataset):
    """ Create a dictionary (keyed by body ID) and set of bodies from NeuPrint
        Keyword arguments:
          dataset: dataset
        Returns:
          result: dictionary of rows from NeuPrint
          neuprint_bodyset: set of body IDs
    """
    query = """
    MATCH (n: Neuron)
    RETURN n.bodyId as bodyId, n.status as status, n.type as type, n.instance as instance
    ORDER BY n.type, n.instance
    """
    results = fetch_custom(query, dataset=dataset, format='json')
    LOGGER.info("%d Body IDs found in NeuPrint %s", len(results['data']), dataset)
    neuprint_bodyset = set()
    for row in results['data']:
        neuprint_bodyset.add(str(row[0]))
    return results['data'], neuprint_bodyset


def insert_body(payload, body, last_uid):
    """ Insert a single body into Mongo
        Keyword arguments:
          payload: initial insertion payload
          body: body record from NeuPrint
          last_uid: last generated UID
        Returns:
          Inserted UID
    """
    for idx, name in enumerate(COLUMN):
        payload[name] = None if body[idx] is None else str(body[idx])
    payload["creationDate"] = datetime.now()
    payload["updatedDate"] = datetime.now()
    LOGGER.debug(payload)
    last_uid = generate_uid(last_uid=last_uid)
    payload['_id'] = last_uid
    if ARG.WRITE:
        post_id = DBM.emBody.insert_one(payload).inserted_id
    else:
        time.sleep(.0005) # If we're not writing, the UID assignment is too fast
        post_id = last_uid
    if post_id != last_uid:
        LOGGER.error("Inserted UID %s does not match calculated UID %d", post_id, last_uid)
    LOGGER.debug("Inserted %s for %s", post_id, str(body[0]))
    COUNT['insert'] += 1
    return last_uid


def update_body(uid, payload):
    """ Update a single body in Mongo
        Keyword arguments:
          uid: Mongo body UID
          payload: update payload
        Returns:
          None
    """
    payload["updatedDate"] = datetime.now()
    if ARG.WRITE:
        result = DBM.emBody.update_one({"_id": uid},
                                       {"$set": payload})
        COUNT['update'] += result.modified_count
    else:
        COUNT['update'] += 1


def get_body_payload_initial(dataset, dataset_uid):
    """ Create an initial body payload
        Keyword arguments:
          dataset: dataset
          dataset_uid: dataset UID
        Returns:
          body_payload: initial body payload
    """
    data_set_ref = "EMDataSet#%d" % (dataset_uid)
    body_payload = {"class": "org.janelia.model.domain.flyem.EMBody",
                    "dataSetIdentifier": dataset,
                    "ownerKey": "group.flyem", "readers": ["group.flyem"],
                    "writers": ["group.flyem"], "dataSetRef": data_set_ref,
                   }
    return body_payload


def insert_bodies(bodies, published, dataset, dataset_uid):
    """ Insert NeuPrint bodies in Mongo
        Keyword arguments:
          bodies: list of bodies from NeuPrint
          published: True=public, False=private
          dataset: dataset
          dataset_uid: dataset UID from Mongo
        Returns:
          None
    """
    last_uid = None
    body_payload = get_body_payload_initial(dataset, dataset_uid)
    if published:
        body_payload['readers'] = ["group:flyem", "group:workstation_users"]
    for body in tqdm(bodies):
        payload = dict(body_payload)
        last_uid = insert_body(payload, body, last_uid)


def update_bodies(bodies, published, dataset, dataset_uid, neuprint_bodyset):
    """ Insert or update a dataset in Mongo
        Keyword arguments:
          bodies: list of bodies from NeuPrint
          published: True=public, False=private
          dataset: dataset
          dataset_uid: dataset UID from Mongo
          neuprint_bodyset: set of NeuPrint body IDs
        Returns:
          None
    """
    mbodies, mongo_bodyset = fetch_mongo_bodies(dataset, dataset_uid)
    COUNT['mongo'] += len(mbodies)
    # Bodies to add to Mongo
    diff = [int(bid) for bid in neuprint_bodyset.difference(mongo_bodyset)]
    if diff:
        LOGGER.warning("Bodies to add to Mongo: %d", len(diff))
        body_payload = get_body_payload_initial(dataset, dataset_uid)
        if published:
            body_payload['readers'] = ["group:flyem", "group:workstation_users"]
        last_uid = None
        for body in tqdm(bodies):
            if body[0] in diff:
                payload = dict(body_payload)
                last_uid = insert_body(payload, body, last_uid)
    # Bodies to remove from Mongo
    diff = mongo_bodyset.difference(neuprint_bodyset)
    if diff:
        LOGGER.warning("Bodies to remove from Mongo: %d", len(diff))
        for body in tqdm(diff):
            if ARG.WRITE:
                DBM.emBody.delete_one({"_id": mbodies[body]['_id'],
                                       "name": body})
            COUNT['delete'] += 1
    # Bodies to check for update
    intersection = neuprint_bodyset.intersection(mongo_bodyset)
    LOGGER.info("Bodies to check for update: %d", len(intersection))
    # bodies list: [46377, 'Assign', None, None]
    for body in tqdm(bodies):
        if str(body[0]) not in intersection:
            continue
        mrow = mbodies[str(body[0])]
        payload = {}
        # status, type, instance
        for idx in range(1, len(COLUMN)):
            if body[idx] != mrow[COLUMN[idx]]:
                LOGGER.info("Change %s from %s to %s for %s", COLUMN[idx],
                            mrow[COLUMN[idx]], body[idx], str(body[0]))
                payload[COLUMN[idx]] = body[idx]
        if payload:
            update_body(mrow["_id"], payload)


def process_dataset(dataset, published=True):
    """ Process a single dataset
        Keyword arguments:
          dataset: dataset
          published: True=public, False=private [True]
        Returns:
          None
    """
    dataset_uid, action = setup_dataset(dataset, published)
    if action == 'ignore':
        return
    bodies, neuprint_bodyset = fetch_neuprint_bodies(dataset)
    COUNT['neuprint'] += len(bodies)
    if action == 'insert':
        insert_bodies(bodies, published, dataset, dataset_uid)
    else:
        update_bodies(bodies, published, dataset, dataset_uid, neuprint_bodyset)


def get_metadata():
    """ Update data in MongoDB for datasets known to one NeuPrint server
        Keyword arguments:
          None
        Returns:
          None
    """
    if not ARG.SERVER:
        ARG.SERVER = "https://neuprint%s.janelia.org" % ('-pre' if ARG.NEUPRINT == 'pre' else '')
    CONFIG['neuprint'] = {'url': ARG.SERVER + '/api/'}
    response = call_responder('neuprint', 'dbmeta/datasets')
    for dataset in response:
        if ARG.RELEASE and ARG.RELEASE != dataset:
            continue
        process_dataset(dataset, 'neuprint-pre' not in ARG.SERVER)
    print(COUNT)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Get NeuPrint metadata")
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        help='NeuPrint release [optional]')
    PARSER.add_argument('--server', dest='SERVER', action='store',
                        help='NeuPrint custom server URL')
    PARSER.add_argument('--neuprint', dest='NEUPRINT', action='store',
                        choices=['prod', 'pre'], default='prod', help='NeuPrint instance')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        choices=['dev', 'prod', 'local'], default='local', help='Manifold')
    PARSER.add_argument('--force', dest='FORCE', action='store_true',
                        default=False, help='Force update')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to Mongo')
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

    initialize_program()
    get_metadata()
    sys.exit(0)