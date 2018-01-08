"""
SSBGP-DSS Dispatcher

Usage:
  dss-dispatcher <install_dir> [--port=<port>]
  dss-dispatcher (-h | --help)

Options:
  -h --help      Show this screen.
  --version      Show version.
  --port=<port>  Port to bind to [default: 32014].

"""
import logging
import sys
from logging.config import fileConfig
from pathlib import Path

from docopt import docopt
from pkg_resources import resource_filename

from dss_dispatcher.__version__ import version
from dss_dispatcher.database import SimulationDB
from dss_dispatcher.dispatch_service import DispatchService
from dss_dispatcher.dispatcher import Dispatcher

# Use root logger
logger = logging.getLogger('')


def main():
    args = docopt(__doc__, version=version)
    port = int(args['--port'])
    install_dir = Path(args['<install_dir>'])

    # Setup the loggers
    logs_config = resource_filename(__name__, 'logs.ini')
    fileConfig(logs_config)

    if not install_dir.is_dir():
        logger.error(f"install directory does not exist: {install_dir}")
        sys.exit(1)

    service = DispatchService(
        Dispatcher(SimulationDB(db_file=str(install_dir / "simulations.db"))),
        bind_address=('', port)
    )

    try:
        logger.info("bound to port %d" % port)
        logger.info("running...")
        service.serve_forever()

    except KeyboardInterrupt:
        logger.info("shutting down the service...")
        service.shutdown()
        service.server_close()
        logger.info("shutdown successful")


if __name__ == '__main__':
    main()
