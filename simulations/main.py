"""
SSBGP-DSS Simulations

Usage:
  ssbgp-dss-simulations generate <topologies> <destinations> <priority>
            [--reportnodes] [--c=<repetitions>] [--min=<min_delay>]
            [--max=<max_delay>] [--th=<threshold>] [ --db=<db_path>]

  ssbgp-dss-simulations (-h | --help)

Options:
  -h --help          Show this screen.
  --version          Show version.
  --c=<repetitions>  Number of repetitions [default: 100].
  --min=<min_delay>  Minimum message delay [default: 10].
  --max=<max_delay>  Maximum message delay [default: 1000].
  --th=<threshold>   Threshold value [default: 2000000].
  --db=<db_path>     Path to the DB file [default: simulations.db].
  --reportnodes      Enable report nodes data individually.

"""
from docopt import docopt

from dss_dispatcher.database import SimulationDB
from simulations.generate_simulations import read_destinations, read_topologies, \
    generate_simulations


def add_simulations(database: SimulationDB, simulations: list, priority: int):
    """ Adds a list of simulations to the queue """
    with database.connect() as connection:
        for simulation in simulations:
            connection.insert_simulation(simulation)
            connection.insert_in_queue(simulation.id, priority)


def main():
    args = docopt(__doc__)

    if args['generate']:
        priority = int(args['<priority>'])
        db_path = args['--db']

        topologies = read_topologies(args['<topologies>'])
        destinations = read_destinations(args['<destinations>'])

        print("Found %d topologies and %s destinations" %
              (len(topologies), len(destinations)))
        print("Generating simulations...")
        simulations = generate_simulations(
            topologies=topologies,
            destinations=destinations,
            repetitions=int(args['--c']),
            min_delay=int(args['--min']),
            max_delay=int(args['--max']),
            threshold=int(args['--th']),
            reportnodes=args['--reportnodes']
        )

        add_simulations(SimulationDB(db_path), simulations, priority)

        print("Done! %d simulations were added with priority %d" %
              (len(simulations), priority))


if __name__ == '__main__':
    main()
