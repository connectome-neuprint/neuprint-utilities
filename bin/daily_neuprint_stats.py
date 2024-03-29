import argparse
import datetime
import json
import sys
import colorlog
import requests


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
            headers = {"Content-type": "application/json", "Authorization": "Bearer " + CONFIG[server]['bearer']}
            LOGGER.debug(payload)
            req = requests.post(url, headers=headers, json=payload)
        else:
            req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code != 200:
        msg = f"{url} returned a {req.status_code}"
        if payload:
            msg += f"\n{payload}"
        LOGGER.error(msg)
        sys.exit(-1)
    else:
        return req.json()


def initialize_program():
    """ Initialize databases """
    global CONFIG
    dbc = call_responder('config', 'config/rest_services')
    CONFIG = dbc['config']


def fetch_top_level(payload, datestruct, datasetn, suffix):
    payload['dataset'] = ARG.DATASET
    # 0.5 Assign
    payload = {"cypher": "MATCH (n :" + datasetn + "{status:\"0.5assign\"})" + suffix}
    response = call_responder('neuprint', 'custom/custom', payload)
    datestruct['0.5assign'] = response['data'][0][0]
    LOGGER.info('0.5assign: ' + str(datestruct['0.5assign']))
    # Anchors
    payload['cypher'] = "MATCH (n :" + datasetn + "{status:\"Anchor\"})" + suffix
    response = call_responder('neuprint', 'custom/custom', payload)
    datestruct['anchor'] = response['data'][0][0]
    LOGGER.info('anchor: ' + str(datestruct['anchor']))
    # Traced neurons
    #payload['cypher'] = "MATCH (n:`" + datasetn + "`) WHERE (n.status=\"Roughly traced\" OR n.status=\"Prelim Roughly traced\" OR n.status=\"Traced\")" + suffix
    payload['cypher'] = "MATCH (n :" + datasetn + ") WHERE (n.status=\"Roughly traced\" OR n.status=\"Prelim Roughly traced\" OR n.status=\"Traced\")" + suffix
    response = call_responder('neuprint', 'custom/custom', payload)
    datestruct['traced'] = response['data'][0][0]
    LOGGER.info('traced: ' + str(datestruct['traced']))


def process_data(dataset):
    payload = dict()
    datestruct = dict()
    datasetn = 'Neuron'
    suffix = ' RETURN count(n)'
    fetch_top_level(payload, datestruct, datasetn, suffix)
    # Top ROIs
    payload['cypher'] = "MATCH (n:" + dataset + "_Meta) RETURN n.superLevelRois"
    response = call_responder('neuprint', 'custom/custom', payload)
    rois = response['data'][0][0]
    # ROI info
    payload['cypher'] = "MATCH (n:Meta) RETURN n.roiInfo"
    response = call_responder('neuprint', 'custom/custom', payload)
    totaldict = json.loads(response['data'][0][0])
    #rois = totaldict.keys()
    roidict = dict()
    count = 1
    for roi in rois:
        LOGGER.info('Fetching data for ROI ' + roi + ' (' + str(count) + '/' + str(len(rois)) + ')')
        # 0.5 Assign
        payload['cypher'] = "MATCH (n:`" + datasetn + "`{`" + roi + "`:true}) WHERE n.status=\"0.5assign\"" + suffix
        response = call_responder('neuprint', 'custom/custom', payload)
        roidict[roi] = {'0.5assign': response['data'][0][0]}
        # Anchor
        payload['cypher'] = "MATCH (n:`" + datasetn + "`{`" + roi + "`:true}) WHERE n.status=\"Anchor\"" + suffix
        response = call_responder('neuprint', 'custom/custom', payload)
        roidict[roi].update({'anchor': response['data'][0][0]})
        # Complete
        payload['cypher'] = "MATCH (n:`" + datasetn + "`{`" + roi + "`:true}) WHERE (n.status=\"Roughly traced\" OR n.status=\"Prelim Roughly traced\" OR n.status=\"Traced\" OR n.status=\"Leaves\")" + suffix
        response = call_responder('neuprint', 'custom/custom', payload)
        roidict[roi].update({'complete': response['data'][0][0]})
        # Traced neurons
        payload['cypher'] = "MATCH (n:`" + datasetn + "`{`" + roi + "`:true}) WHERE (n.status = \"Roughly traced\" OR n.status = \"Prelim Roughly traced\" OR n.status = \"Leaves\" OR n.status = \"Traced\") WITH apoc.convert.fromJsonMap(n.roiInfo) AS roiInfo RETURN sum(roiInfo.`" + roi + "`.pre), sum(roiInfo.`" + roi + "`.post)"
        response = call_responder('neuprint', 'custom/custom', payload)
        roidict[roi].update({'presynaptic': response['data'][0][0], 'postsynaptic': response['data'][0][1]})
        # Denomionators
        if roi in totaldict:
            roidict[roi].update({'presynaptic_total': totaldict[roi]['pre'], 'postsynaptic_total': totaldict[roi]['post']})
        count += 1
    datestruct['rois'] = roidict
    LOGGER.info('Found data for ' + str(len(roidict)) + ' ROIs')
    LOGGER.debug(datestruct)
    if ARG.WRITE:
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        LOGGER.warning("Writing " + dataset + " data to config system for " +
                       today)
        datestruct['update_date'] = str(datetime.datetime.now())
        try:
            requests.post(CONFIG['config']['url'] + 'importjson/neuprint_' +
                          dataset + '/' + today,
                          {"config": json.dumps(datestruct)})
        except requests.exceptions.RequestException as err:
            LOGGER.critical(err)
            sys.exit(-1)


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
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    initialize_program()
    process_data(ARG.DATASET)
