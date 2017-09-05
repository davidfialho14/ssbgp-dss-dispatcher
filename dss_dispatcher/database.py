import sqlite3
from contextlib import contextmanager

from datetime import datetime
from pkg_resources import resource_filename, Requirement

from dss_dispatcher.simulation import Simulation, simulation_with


class EntryExistsError(Exception):
    """
    Raised when trying to insert an entry that already exists in the database.
    """


class EntryNotFoundError(Exception):
    """
    Raised when trying to access an entry that does NOT exist in the database.
    """


class SimulationDatabase:
    """ Abstraction o access the simulations database """

    CREATE_TABLES_SCRIPT = resource_filename(
        Requirement.parse("ssbgp-dss-dispatcher"), 'dss_dispatcher/tables.sql')

    DATETIME_FORMAT = "%Y-%m-%d_%H:%M:%S"

    def __init__(self, db_file: str):
        self._db_file = db_file

        # Load the create tables script
        with open(self.CREATE_TABLES_SCRIPT) as file:
            script = file.read()

        # Create tables if necessary
        with self._connect() as connection:
            connection.cursor().executescript(script)

    # region Modify methods

    def insert_simulation(self, simulation: Simulation):
        """
        Inserts a new simulation in the database.

        :param simulation: simulation to insert
        :raise EntryExistsError: if a simulation with the same ID already exists
        """
        try:
            with self._connect() as connection:
                connection.cursor().execute(
                    "INSERT INTO simulation VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", simulation)

        except sqlite3.IntegrityError as error:

            if 'UNIQUE constraint failed' in str(error):
                raise EntryExistsError("Simulation already exists in the "
                                       "database")

            # Just re-raise any other errors
            raise

    def moveto_queue(self, simulation_id: str, priority: int):
        """
        Inserts the simulation with the specified ID in the `queue` table.

        It assigns the simulation a priority value, used to sort simulations
        in queue. Simulations with an higher priority will be executed first.
        An higher value corresponds to an higher priority.

        :param simulation_id: ID of the simulation to move
        :param priority:      priority value
        :raise EntryNotFoundError: if the simulation does not exist
        :raise EntryExistsError: if a simulation with the same ID is already
        in the queue table
        """
        try:
            with self._connect() as connection:
                connection.cursor().execute("INSERT INTO queue VALUES (?, ?)",
                                            (simulation_id, priority))

        except sqlite3.IntegrityError as error:

            if 'FOREIGN KEY constraint failed' in str(error):
                raise EntryNotFoundError("Database does not contain simulation "
                                         "with ID `%s`" % simulation_id)

            elif 'UNIQUE constraint failed' in str(error):
                raise EntryExistsError("Simulation already exists in the "
                                       "database")

            # Just re-raise any other errors
            raise

    def moveto_running(self, simulation_id: str, simulator_id: str):
        """
        Removes a simulation from the `queue` table and inserts it in the
        `running` table.

        :param simulation_id: ID of the simulation to move
        :param simulator_id:  ID of the simulator executing the simulation
        """
        # Remove simulation from `queue` table
        with self._connect() as connection:
            connection.cursor().execute(
                "DELETE FROM queue WHERE simulation_id=?", (simulation_id,))

            # Insert simulation into `running` table
            try:
                connection.cursor().execute("INSERT INTO running VALUES (?, ?)",
                                            (simulator_id, simulation_id))

            except sqlite3.IntegrityError as error:

                if 'FOREIGN KEY constraint failed' in str(error):
                    raise EntryNotFoundError(
                        "Database does not contain simulation "
                        "with ID `%s`" % simulation_id)

                elif 'UNIQUE constraint failed' in str(error):
                    raise EntryExistsError("Simulation already exists in the "
                                           "database")

                # Just re-raise any other errors
                raise

    def moveto_complete(self, simulation_id: str, simulator_id: int,
                        finish_datetime: datetime):
        """
        Removes a simulation from the `running` table and inserts it in the
        `complete` table.

        :param simulation_id:   ID of the simulation to move
        :param simulator_id:    ID of the simulator that executed the simulation
        :param finish_datetime: date and time when the simulation finished
        """
        # Remove simulation from `running` table
        with self._connect() as connection:
            connection.cursor().execute(
                "DELETE FROM running WHERE simulation_id=?", (simulation_id,))

            # Insert simulation into `complete` table
            try:
                connection.cursor().execute(
                    "INSERT INTO complete VALUES (?, ?, ?)",
                    (simulator_id, simulation_id,
                     finish_datetime.strftime(self.DATETIME_FORMAT)))

            except sqlite3.IntegrityError as error:

                if 'FOREIGN KEY constraint failed' in str(error):
                    raise EntryNotFoundError(
                        "Database does not contain simulation "
                        "with ID `%s`" % simulation_id)

                elif 'UNIQUE constraint failed' in str(error):
                    raise EntryExistsError("Simulation already exists in the "
                                           "database")

                # Just re-raise any other errors
                raise

    def insert_simulator(self, id: str):
        """
        Inserts a new simulator in the database.

        :param id: ID of the simulator (mus tbe unique)
        :raise EntryExistsError: if a simulator with the same ID already exists
        """
        try:
            with self._connect() as connection:
                connection.cursor().execute(
                    "INSERT INTO simulator VALUES (?)", (id,))

        except sqlite3.IntegrityError as error:

            if 'UNIQUE constraint failed' in str(error):
                raise EntryExistsError("Simulator already exists in the "
                                       "database")

            # Just re-raise any other errors
            raise

    # endregion

    # region Access methods

    # noinspection PyTypeChecker
    def all_simulations(self):
        """
        Generator that returns each simulation that was inserted in the
        database. This includes queued, running, and complete simulations.
        """
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM simulation")

            row = cursor.fetchone()
            while row:
                yield self._simulation_fromrow(row)
                row = cursor.fetchone()

    # noinspection PyTypeChecker
    def queued_simulations(self):
        """
        Generator that returns each simulation in the `queue` table ordered
        by priority: simulations with higher priority first. Along with the
        simulation it also returns the corresponding priority.
        """
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT * FROM simulation JOIN queue ON id == simulation_id;")

            row = cursor.fetchone()
            while row:
                yield self._simulation_fromrow(row), row['priority']
                row = cursor.fetchone()

    # noinspection PyTypeChecker
    def running_simulations(self):
        """
        Generator that returns each simulation in the `running` table in no
        particular order. Along with the simulation it also returns the
        ID of the simulator executing the simulation.
        """
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT * FROM simulation JOIN running ON id == simulation_id;")

            row = cursor.fetchone()
            while row:
                yield self._simulation_fromrow(row), row['simulator_id']
                row = cursor.fetchone()

    # noinspection PyTypeChecker
    def complete_simulations(self):
        """
        Generator that returns each simulation in the `complete` table in no
        particular order. Along with the simulation it also returns the
        ID of the simulator the executed the simulation and the finish datetime.
        """
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT * FROM simulation JOIN complete ON id == simulation_id;")

            row = cursor.fetchone()
            while row:
                simulation = self._simulation_fromrow(row)
                finish_datetime = datetime.strptime(row['finish_datetime'],
                                                    self.DATETIME_FORMAT)

                yield simulation, row['simulator_id'], finish_datetime
                row = cursor.fetchone()

    def next_simulation(self) -> Simulation:
        """
        Returns the simulation in the queue with the highest priority. If
        there are multiple simulations with the same priority value,
        it returns one of them.

        :return: simulation from the queue with the highest priority
        """

    # noinspection PyTypeChecker
    def simulators(self):
        """
        Generator that returns the IDs of each simulator included in the
        `simulator` table.
        """
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM simulator")

            row = cursor.fetchone()
            while row:
                yield row['id']
                row = cursor.fetchone()

    # endregion

    # region Helper private methods

    @contextmanager
    def _connect(self) -> sqlite3.Connection:
        """ Connects to the DB and yields the connection """
        with sqlite3.connect(database=self._db_file) as connection:
            # Enable foreign keys because they are not enabled by default
            connection.execute('PRAGMA foreign_keys=ON')

            # Use a row factory to return the query results
            # This allows provides access to each column by name
            connection.row_factory = sqlite3.Row

            yield connection

            # endregion

    @staticmethod
    def _simulation_fromrow(row) -> Simulation:
        return simulation_with(
            id=row['id'],
            report_path=row['report_path'],
            topology=row['topology'],
            destination=row['destination'],
            repetitions=row['repetitions'],
            min_delay=row['min_delay'],
            max_delay=row['max_delay'],
            threshold=row['threshold'],
            stubs_file=row['stubs_file'],
            seed=row['seed'],
        )
