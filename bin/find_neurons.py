""" find_neurons.py
    Find neurons specified in an input file.
"""

import argparse
import collections
from operator import attrgetter
import os
import sys
import inquirer
from neuprint import Client, fetch_neurons, utils
from neuprint import NeuronCriteria as NC
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Environment
JWT = "DSG_SA_NEUPRINT_NEURONBRIDGE_TOKEN"
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

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


def get_neurons():
    ''' Find neurons for Traced neurons
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
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"Fetching neurons for {ARG.DATASET}")
    try:
        criteria = NC()
        neuron_df, _ = fetch_neurons(criteria, client=npc)
        names = dict(enumerate(list(neuron_df.type.unique())))
    except Exception as err:
        terminate_program(err)
    with open(ARG.FILE, "r", encoding="ascii") as stream:
        for line in stream:
            name = line.strip()
            COUNT['read'] += 1
            if name in names.values():
                COUNT['found'] += 1
            else:
                COUNT['notfound'] += 1
                LOGGER.error(f"Neuron {name} was not found")
    print(f"Dataset:           {ARG.DATASET}")
    print(f"Neurons read:      {COUNT['read']:,}")
    print(f"Neurons found:     {COUNT['found']:,}")
    print(f"Neurons not found: {COUNT['notfound']:,}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Find neurons in Neuprint")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        required=True, help='File of neuron names')
    PARSER.add_argument('--dataset', dest='DATASET', action='store',
                        help='NeuPrint dataset [optional]')
    PARSER.add_argument('--neuprint', dest='NEUPRINT', action='store',
                        choices=['prod', 'pre', 'cns'], default='prod', help='NeuPrint instance')
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
    get_neurons()
    terminate_program()
