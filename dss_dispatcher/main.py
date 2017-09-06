"""
SSBGP-DSS Dispatcher

Usage:
  ssbgp-dss-dispatcher <data_dir> [--port=<port>]
  ssbgp-dss-dispatcher (-h | --help)

Options:
  -h --help      Show this screen.
  --version      Show version.
  --port=<port>  Port to bind to [default: 32014].

"""
import logging
import os
import sys

from docopt import docopt

from dss_dispatcher.__version__ import version
from dss_dispatcher.database import SimulationDB
from dss_dispatcher.dispatch_service import DispatchService
from dss_dispatcher.dispatcher import Dispatcher


LOG_FORMAT = '%(asctime)s - %(levelname)s: %(message)s'

logger = logging.getLogger('')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def main():
    args = docopt(__doc__, version=version)

    data_dir = args['<data_dir>']
    if not os.path.isdir(data_dir):
        logger.error("data directory does not exist: %s" % data_dir)
        sys.exit(1)

    port = int(args['--port'])
    db_path = os.path.join(data_dir, "simulations.db")

    service = DispatchService(Dispatcher(SimulationDB(db_path)),
                              bind_address=('', port))

    try:
        logger.info("bound to port %d" % port)
        logger.info("running...")
        service.serve_forever()

    except KeyboardInterrupt:
        print()
        logger.info("shutting down the service...")
        service.shutdown()
        service.server_close()
        logger.info("shutdown successful")


if __name__ == '__main__':
    main()
