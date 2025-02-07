""" get skeletons.py
    Get skeletons from Neuprint for a specified dataset.
"""

import argparse
import collections
import json
from operator import attrgetter
import os
import sys
import dask
from dask.diagnostics import ProgressBar
import inquirer
import requests
from tqdm import tqdm
from neuprint import Client, fetch_meta, fetch_neurons, utils
from neuprint import NeuronCriteria as NC
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Environment
JWT = "NEUPRINT_APPLICATION_CREDENTIALS"
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# Globals
LOGGER = ARG = REST = None

# -----------------------------------------------------------------------------

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        if getattr(msg, '__notes__', None):
            msg += f"\n----------------------\n{msg.__notes__}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    if JWT not in os.environ:
        terminate_program(f"Missing JSON Web Token - set in {JWT} environment variable")


def get_dataset(server):
    ''' Allow the user to select a dataset
        Keyword arguments:
          server: Neuprint server URL
        Returns:
          None
    '''
    try:
        datasets = utils.available_datasets(server)
    except Exception as err:
        terminate_program(err)
    if len(datasets) == 1:
        ARG.DATASET = datasets[0]
        return
    questions = [inquirer.List('dataset', message="Select dataset", choices=datasets)]
    answers = inquirer.prompt(questions)
    if not answers:
        terminate_program("No dataset selected")
    ARG.DATASET = answers['dataset']


@dask.delayed
def handle_single_skeleton(npc, bid, path):
    ''' Create a skeleton for a single body ID and export
        Keyword arguments:
          npc: neuprint client
          bid: body ID
          path: export path
        Returns:
          None
    '''
    if ARG.WRITE:
        try:
            npc.fetch_skeleton(bid, format='swc', export_path=path)
        except requests.HTTPError:
            LOGGER.warning(f"Could not fetch skeleton for {bid}")
        except Exception as err:
            LOGGER.error(f"Could not fetch skeleton for {bid}")
            terminate_program(err)


def get_skeletons():
    ''' Find skeletons for Traced neurons and optionally write SWCs
        Keyword arguments:
          None
        Returns:
          None
    '''
    which = "neuprint" if ARG.NEUPRINT == "prod" else f"neuprint-{ARG.NEUPRINT}"
    server = attrgetter(f"{which}.url")(REST).replace("/api/", "")
    if not ARG.DATASET:
        get_dataset(server)
    try:
        npc = Client(server, dataset=ARG.DATASET)
        response = fetch_meta(client=npc)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Fetching body IDs for {ARG.DATASET}")
    try:
        criteria = NC(status=['Traced'])
        neuron_df, _ = fetch_neurons(criteria, client=npc)
    except Exception as err:
        terminate_program(err)
    bodyids = neuron_df['bodyId'].tolist()
    LOGGER.info(f"Body IDs: {len(bodyids):,}")
    COUNT["found"] = len(bodyids)
    parallel = []
    for bid in tqdm(bodyids, desc="Preparing jobs"):
        COUNT["written"] += 1
        path = f"{ARG.BASE}/{bid}.swc"
        parallel.append(dask.delayed(handle_single_skeleton)(npc, bid, path))
    print("Skeleton creation")
    with ProgressBar():
        dask.compute(*parallel, num_workers=ARG.WORKERS)
    if ARG.WRITE:
        with open(f"{ARG.BASE}/metadata.json", "w", encoding="ascii") as stream:
            json.dump(response, stream, indent=2)
    print(f"Dataset:          {ARG.DATASET}")
    print(f"UUID:             {response['uuid']}")
    print(f"Body IDs found:   {COUNT['found']:,}")
    print(f"Body IDs written: {COUNT['written']:,}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run complete: no skeletons written")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Get skeletons from Neuprint")
    PARSER.add_argument('--base', dest='BASE', action='store',
                        required=True, help='Base directory to write to')
    PARSER.add_argument('--dataset', dest='DATASET', action='store',
                        help='NeuPrint dataset [optional]')
    PARSER.add_argument('--neuprint', dest='NEUPRINT', action='store',
                        choices=['prod', 'pre', 'cns'], default='prod',
                        help='NeuPrint instance [prod]')
    PARSER.add_argument('--workers', dest='WORKERS', action='store',
                        type=int, default=8, help='Number of workers [8]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to Mongo')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        REST = JRC.get_config("rest_services")
    except Exception as gerr:
        terminate_program(gerr)
    initialize_program()
    get_skeletons()
    terminate_program()
