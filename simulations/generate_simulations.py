import uuid

from dss_dispatcher.simulation import simulation_with
from simulations.topology import Topology


def read_topologies(topologies_path: str):
    """ Reads a topologies file and returns a list with those topologies """
    with open(topologies_path) as file:
        return [Topology(*line.strip().split("|")) for line in file if line]


def read_destinations(destinations_path: str) -> list:
    """ Reads a destinations file and returns a list with those destinations """
    with open(destinations_path) as file:
        return [int(line.strip()) for line in file if line]


def generate_simulations(topologies: list, destinations: list,
                         repetitions: int, min_delay: int, max_delay: int,
                         threshold: int, reportnodes: bool) -> list:
    """
    Generates a simulation for each topology and each destination.

    :param topologies:   list containing the topologies
    :param destinations: list containing the destinations
    :param repetitions:  number of repetitions for each simulation
    :param min_delay:    minimum message delay for each simulation
    :param max_delay:    maximum message delay for each simulation
    :param threshold:    threshold value for each simulation
    :param reportnodes:  enable/disable report nodes feature
    :return: a list of generated simulations
    """
    simulations = []
    for topology in topologies:
        for destination in destinations:
            simulations.append(simulation_with(
                "",  # this is ignored
                topology.name,
                destination,
                repetitions,
                min_delay,
                max_delay,
                threshold,
                topology.stubs,
                seed=None,
                reportnodes=reportnodes,
                id=str(uuid.uuid4())
            ))

    return simulations
