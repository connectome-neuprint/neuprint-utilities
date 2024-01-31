''' Get NeuPrint metadata and update 
'''

import argparse
from datetime import datetime, timezone
from operator import attrgetter
import os
import socket
import sys
import time
import MySQLdb
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC
from neuprint import Client, default_client, fetch_custom, fetch_meta, set_default_client


# Configuration
CONFIG = {'config': {}}
COUNT = {'mongo': 0, 'neuprint': 0, 'delete': 0, 'insert': 0, 'update': 0, 'published': 0}
JWT = 'NEUPRINT_APPLICATION_CREDENTIALS'
KEYS = {}
# Database
DBM = {}
EXISTING_BODY = {}
# Mapping of NeuPrint column number to Mongo name
COLUMN = ["name", "status", "statusLabel", "neuronType", "neuronInstance", "voxelSize"]
INT_COLUMN = ["voxelSize"]

# pylint: disable=W0703,C0209,W1203

# -----------------------------------------------------------------------------

def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


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
            req = requests.get(url, headers=headers, timeout=10)
        else:
            req = requests.get(url, timeout=10)
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
    if JWT not in os.environ:
        LOGGER.error("Missing JSON Web Token - set in %s environment variable", JWT)
        sys.exit(-1)
    # Connect to Mongo
    try:
        data = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught)
        terminate_program(err)
    rwp = 'write' if ARG.WRITE else 'read'
    dbo = attrgetter(f"jacs.{ARG.MANIFOLD}.{rwp}")(data)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DBM['jacs'] = JRC.connect_database(dbo)
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))


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
        LOGGER.debug(f"time_component: {time_component:b}")
        time_component = time_component << 22
        LOGGER.debug(f"current_index: {current_index:b}")
        LOGGER.debug(f"deployment_context: {deployment_context:b}")
        LOGGER.debug(f"ip_component: {ip_component:b}")
        next_uid = time_component + (current_index << 12) + (deployment_context << 8) + ip_component
        if last_uid and last_uid == next_uid:
            LOGGER.debug("Soft collision %d (%d)", last_uid, current_index)
            next_uid = None
            current_index += 1
        if not next_uid and (current_index > max_tries):
            LOGGER.debug("Hard collision %d (%d)", last_uid, current_index)
            time.sleep(0.75)
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
    dt_string = dt_string.replace("Timestamp:", "")
    if " /" in dt_string:
        dt_string = dt_string.split(" /")[0]
    dt_string = dt_string.split(".")[0]
    try:
        if 'Z' in dt_string:
            dt_string = dt_string.replace("Z", "+0000")
            dt_aware = datetime.strptime(dt_string.replace('T', ' '), "%Y-%m-%d %H:%M:%S.%f%z")
            dt_aware = dt_aware.replace(tzinfo=timezone.utc).astimezone(tz=None)
            return dt_aware.replace(tzinfo=None)
    except Exception as err:
        terminate_program(f"Could not parse datetime string {dt_string}")
    return datetime.strptime(dt_string.replace('T', ' '), "%Y-%m-%d %H:%M:%S")


def setup_dataset(dataset, published):
    """ Insert or update a data set in Mongo
        Keyword arguments:
          dataset: data set
          published: True=public, False=private
        Returns:
          last_uid: last UID assigned
          action: what to do with bodies in this data set (ignore, insert, or update)
    """
    LOGGER.info("Initializing Client for %s %s", ARG.SERVER, dataset)
    npc = Client(ARG.SERVER, dataset=dataset)
    set_default_client(npc)
    result = fetch_meta(client=npc)
    if ':' in dataset:
        name, version = dataset.split(':')
        version = version.replace('v', '')
    else:
        name = dataset
        version = ''
    LOGGER.info(f"Found NeuPrint dataset {name}:{version} ({result['uuid']}) " \
                + f"{result['lastDatabaseEdit']}")
    coll = DBM['jacs'].emDataSet
    check = coll.find_one({"name": name, "version": version, "published": published})
    action = 'ignore'
    emattr = JRC.get_config("em_datasets")
    if not check:
        if not hasattr(emattr, result['dataset']):
            LOGGER.error("%s does not have attributes defined", result['dataset'])
            return None, "ignore"
        payload = {"class" : "org.janelia.model.domain.flyem.EMDataSet",
                   "ownerKey": "group:flyem", "readers": ["group:flyem"],
                   "writers": ["group:flyem"],
                   "name" : result['dataset'], "version": version,
                   "uuid": result['uuid'],
                   "gender": attrgetter(f"{result['dataset']}.gender")(emattr),
                   "anatomicalArea": attrgetter(f"{result['dataset']}.anatomicalArea")(emattr),
                   "creationDate": to_datetime(result['lastDatabaseEdit']),
                   "updatedDate": to_datetime(result['lastDatabaseEdit']),
                   "active": True,
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
            sys.exit(-1)
        LOGGER.info("Inserted data set %s (UID: %s, datetime: %s)", dataset, post_id,
                    result['lastDatabaseEdit'])
        action = 'insert'
    else:
        LOGGER.info("%s already exists in Mongo (UID: %s)", dataset, check['_id'])
        last_uid = check['_id']
        neuprint_dt = to_datetime(result['lastDatabaseEdit'])
        if neuprint_dt > check['updatedDate'] or ARG.FORCE:
            LOGGER.warning("Update required for %s (last changed %s)",
                           dataset, result['lastDatabaseEdit'])
            payload = {"updatedDate": datetime.now(), "active": True,
                       "uuid": result["uuid"]}
            if ARG.WRITE:
                coll.update_one({"_id": check['_id']},
                                {"$set": payload})
            action = 'update'
        else:
            LOGGER.info("No update required for %s (last changed %s)",
                        dataset, check['updatedDate'])
    return last_uid, action


def fetch_mongo_bodies(dataset, dataset_uid):
    """ Create a dictionary (keyed by body ID) and set of bodies from Mongo
        Keyword arguments:
          dataset: data set
          dataset_uid: data set UID
        Returns:
          rowdict: dictionary of rows from Mongo
          mongo_bodyset: set of body IDs
    """
    rows = DBM['jacs'].emBody.find({"dataSetRef": 'EMDataSet#' + str(dataset_uid)})
    mongo_bodyset = set()
    rowdict = {}
    for row in rows:
        mongo_bodyset.add(row['name'])
        rowdict[row['name']] = row
    LOGGER.info("%d Body IDs found in MongoDB %s", len(mongo_bodyset), dataset)
    return rowdict, mongo_bodyset


def fetch_neuprint_bodies(dataset):
    """ Create a dictionary (keyed by body ID) and set of bodies from NeuPrint
        Keyword arguments:
          dataset: data set
        Returns:
          result: dictionary of rows from NeuPrint
          neuprint_bodyset: set of body IDs
    """
    query = """
    MATCH (n: Neuron)
    RETURN n.bodyId as bodyId,n.status as status,n.statusLabel as label,n.type as type,n.instance as instance,n.size as size
    ORDER BY n.type, n.instance
    """
    print(default_client())
    results = fetch_custom(query, format='json')
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
    # Uncomment this to check public data set for every body ID
    #if payload['dataSetIdentifier'] in EXISTING_BODY and
    #   str(body[0]) in EXISTING_BODY[payload['dataSetIdentifier']]:
    #    COUNT['published'] += 1
    #    return
    for idx, name in enumerate(COLUMN):
        if body[idx] is None:
            payload[name] = None
        elif name in INT_COLUMN:
            payload[name] = body[idx]
        else:
            payload[name] = str(body[idx])
    payload["creationDate"] = datetime.now()
    payload["updatedDate"] = datetime.now()
    LOGGER.debug(payload)
    last_uid = generate_uid(last_uid=last_uid)
    payload['_id'] = last_uid
    if ARG.WRITE:
        post_id = DBM['jacs'].emBody.insert_one(payload).inserted_id
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
        result = DBM['jacs'].emBody.update_one({"_id": uid},
                                               {"$set": payload})
        COUNT['update'] += result.modified_count
    else:
        COUNT['update'] += 1


def get_body_payload_initial(dataset, dataset_uid, published):
    """ Create an initial body payload
        Keyword arguments:
          dataset: data set
          dataset_uid: data set UID
          published: True=public, False=private
        Returns:
          body_payload: initial body payload
    """
    data_set_ref = f"EMDataSet#{dataset_uid}"
    body_payload = {"class": "org.janelia.model.domain.flyem.EMBody",
                    "dataSetIdentifier": dataset,
                    "ownerKey": "group:flyem", "readers": ["group:flyem"],
                    "writers": ["group:flyem"], "dataSetRef": data_set_ref,
                   }
    if published:
        body_payload['readers'] = ["group:flyem", "group:workstation_users"]
    return body_payload


def insert_bodies(bodies, published, dataset, dataset_uid):
    """ Insert NeuPrint bodies in Mongo
        Keyword arguments:
          bodies: list of bodies from NeuPrint
          published: True=public, False=private
          dataset: data set
          dataset_uid: data set UID from Mongo
        Returns:
          None
    """
    last_uid = None
    body_payload = get_body_payload_initial(dataset, dataset_uid, published)
    for body in tqdm(bodies):
        payload = dict(body_payload)
        last_uid = insert_body(payload, body, last_uid)


def update_bodies(bodies, published, dataset, dataset_uid, neuprint_bodyset):
    """ Insert or update a data set in Mongo
        Keyword arguments:
          bodies: list of bodies from NeuPrint
          published: True=public, False=private
          dataset: datas et
          dataset_uid: data set UID from Mongo
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
        body_payload = get_body_payload_initial(dataset, dataset_uid, published)
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
            LOGGER.debug("Delete %s %s", mbodies[body]['_id'], body)
            if ARG.WRITE:
                DBM['jacs'].emBody.delete_one({"_id": mbodies[body]['_id'],
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
        # status, status label, type, instance
        for idx in range(1, len(COLUMN)):
            if COLUMN[idx] not in mrow:
                LOGGER.info("Added new column %s (%s) to %s", COLUMN[idx], body[idx], str(body[0]))
                payload[COLUMN[idx]] = body[idx]
            elif body[idx] != mrow[COLUMN[idx]]:
                LOGGER.debug("Change %s from %s to %s for %s", COLUMN[idx],
                             mrow[COLUMN[idx]], body[idx], str(body[0]))
                payload[COLUMN[idx]] = body[idx]
        if payload:
            update_body(mrow["_id"], payload)


def process_dataset(dataset, published=True):
    """ Process a single data set
        Keyword arguments:
          dataset: data set
          published: True=public, False=private [True]
        Returns:
          None
    """
    dataset_uid, action = setup_dataset(dataset, published)
    if action == 'ignore' and not ARG.FORCE:
        return
    bodies, neuprint_bodyset = fetch_neuprint_bodies(dataset)
    COUNT['neuprint'] += len(bodies)
    # Show a list of statuses
    if ARG.VERBOSE:
        status = {}
        for body in bodies:
            if not (body[3] and body[4]):
                continue
            try:
                pos = body[4].index(body[3])
                if pos:
                    status[body[2]] = 1
            except Exception:
                status[body[2]] = 1
        print(f"Statuses: {', '.join(list(status.keys()))}")
    if action == 'insert':
        insert_bodies(bodies, published, dataset, dataset_uid)
    else:
        update_bodies(bodies, published, dataset, dataset_uid, neuprint_bodyset)


def get_public_body_ids():
    """ Populate EXISTING_BODY with a list of public data sets
        Keyword arguments:
          None
        Returns:
          None
    """
    save_server = ARG.SERVER
    ARG.SERVER = 'https://neuprint.janelia.org'
    LOGGER.info("Finding public body IDs")
    CONFIG['neuprint'] = {'url': ARG.SERVER + '/api/'}
    response = call_responder('neuprint', 'dbmeta/datasets')
    for dataset in response:
        # For now, just sage the names of the data set
        EXISTING_BODY[dataset] = 1
        # Client(ARG.SERVER, dataset=dataset)
        # bodies, neuprint_bodyset = fetch_neuprint_bodies(dataset)
        # EXISTING_BODY[dataset] = neuprint_bodyset
    ARG.SERVER = save_server


def get_metadata():
    """ Update data in MongoDB for data sets known to one NeuPrint server
        Keyword arguments:
          None
        Returns:
          None
    """
    if not ARG.SERVER:
        ARG.SERVER = f"https://neuprint{'-pre' if ARG.NEUPRINT == 'pre' else ''}.janelia.org"
    if ARG.NEUPRINT == "pre":
        get_public_body_ids()
    CONFIG['neuprint'] = {'url': ARG.SERVER + '/api/'}
    response = call_responder('neuprint', 'dbmeta/datasets')
    for dataset in response:
        if ARG.RELEASE and ARG.RELEASE != dataset:
            continue
        if ARG.NEUPRINT == "pre" and dataset in EXISTING_BODY:
            LOGGER.warning("Skipping prepublishing release %s", dataset)
            continue
        process_dataset(dataset, '-pre' not in ARG.SERVER)
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
                        choices=['dev', 'prod', 'local'], default='dev', help='Manifold')
    PARSER.add_argument('--force', dest='FORCE', action='store_true',
                        default=False, help='Force update')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to Mongo')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
    get_metadata()
    sys.exit(0)
