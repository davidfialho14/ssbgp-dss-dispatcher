import logging
import uuid
from datetime import datetime

from dss_dispatcher.database import SimulationDB, EntryExistsError, EntryNotFoundError
from dss_dispatcher.simulation import Simulation


class OperationError(Exception):
    """ Raised for an operation error that cannot be handled """

    def __init__(self, message, raised):
        self.message = message
        self.raised = raised


logger = logging.getLogger('dispatcher')


class Dispatcher:
    """
    The dispatcher distributes simulations to multiple simulators which execute them.

    Its job is to manage a queue of simulations and assign each of them to simulators to be
    executed.

    When a simulator is available for processing, it asks the dispatcher for a simulation to run.
    The dispatcher looks into the simulation queue and takes the next simulation in the queue and
    sends it to the simulator to be executed. Once the simulator finishes the simulation,
    it notifies the dispatcher. The dispatcher then marks the simulation as `complete` stores a
    record of when was the simulation complete and which simulator executed the simulation.

    In order to ask for simulations, a simulator must register with the dispatcher first.
    Registering will give the simulator an ID that will uniquely identify it in the system.
    """

    def __init__(self, database: SimulationDB):
        self._database = database

    def register(self) -> str:
        """
        Registers a simulator with the system and returns the unique ID of the newly registered
        simulator.
        """
        with self._database.connect() as connection:
            simulator_id = None
            while simulator_id is None:
                # Generate a new ID for the new simulator
                simulator_id = str(uuid.uuid4())

                try:
                    connection.insert_simulator(simulator_id)

                except EntryExistsError:
                    # There is already a simulator with that ID
                    # Try again
                    simulator_id = None
                    continue

            logger.info(f"registered new simulator {simulator_id}")
            return simulator_id

    def next_simulation(self, simulator_id: str) -> Simulation:
        """
        It pops the next simulation in the queue, assigns it to the simulator with the specified
        ID, and returns the simulation parameters.
        """
        with self._database.connect() as db:
            try:
                logger.info(f"simulator {simulator_id} requested a simulation")

                # Obtain the simulation associated with the given simulator (if there is one)
                simulation = db.running_simulation(simulator_id)

                if not simulation:
                    # Retrieve the next simulation from the queue
                    simulation = db.next_simulation()

                    if simulation:
                        try:
                            # Assign simulation to the given simulator and remove it from the queue
                            db.insert_in_running(simulation.id, simulator_id)
                            db.delete_from_queue(simulation.id)

                        except EntryNotFoundError:
                            # There is no simulator registered with the given ID
                            # Indicate there are no simulations in the queue
                            simulation = None

                logger.info(f"responded with {simulation.id}")

                return simulation

            except Exception as error:
                db.rollback()
                logger.exception("unexpected error")
                raise OperationError(str(error), error)

    def notify_finished(self, simulator_id: str, simulation_id: str):
        """
        Informs the dispatcher that the simulation with the specified ID was executed.

        :param simulator_id:  ID of the simulator that executed the simulation
        :param simulation_id: ID of the simulation that was executed
        """
        with self._database.connect() as connection:
            connection.delete_from_running(simulation_id)
            connection.insert_in_complete(simulation_id, simulator_id, datetime.now())

        logger.info(f"simulator {simulator_id} finished {simulation_id}")
