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


class DeleteError(Exception):
    """
    Raised when trying to delete an entry that can not be deleted.
    """


class Connection:
    """
    An abstraction for a DB connection that provides specific methods to
    perform specific operations relevant to the simulations DB.

    All operations performed on a connection are only committed either when
    the commit() method is called or when the connection is closed.
    """

    # Datetime format used to store the simulation's finish timestamps
    DATETIME_FORMAT = "%Y-%m-%d_%H:%M:%S"

    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

        # Enable foreign keys because they are not enabled by default
        self._connection.execute('PRAGMA foreign_keys=ON')

        # Use a row factory to return the query results
        # This allows provides access to each column by name
        self._connection.row_factory = sqlite3.Row

    def insert_simulator(self, id: str):
        """
        Inserts a new simulator in the database.

        :param id: ID of the new simulator
        :raise EntryExistsError: if the DB contains a simulator with the same ID
        """
        try:
            self._connection.cursor().execute(
                "INSERT INTO simulator VALUES (?)", (id,))

        except sqlite3.IntegrityError as error:
            if _is_unique_constraint(error):
                raise EntryExistsError("DB already contains simulator "
                                       "with ID `%s`" % id)

            # Just re-raise any other errors
            raise

    def insert_simulation(self, simulation: Simulation):
        """
        Inserts a new simulation in the database.

        :param simulation: simulation to insert
        :raise EntryExistsError: if the DB contains a simulation with the
        same ID of the simulation to be inserted
        """
        try:
            self._connection.cursor().execute(
                "INSERT INTO simulation VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", simulation)

        except sqlite3.IntegrityError as error:
            if _is_unique_constraint(error):
                raise EntryExistsError("DB already contains simulation "
                                       "with ID `%s`" % simulation.id)

            # Just re-raise any other errors
            raise

    def insert_in_queue(self, simulation_id: str, priority: int):
        """
        Inserts the simulation with the specified ID in the `queue` table.
        It associates the simulation with a priority value.

        To insert a simulation in the `queue` table it must have already been
        inserted in the DB.

        :param simulation_id: ID of the simulation to insert in `queue`
        :param priority:      priority value to assign the simulation
        :raise EntryNotFoundError: if the simulation does not exist
        :raise EntryExistsError: if a simulation with the same ID is already
        in the `queue` table
        """
        try:
            self._connection.cursor().execute(
                "INSERT INTO queue VALUES (?, ?)",
                (simulation_id, priority))

        except sqlite3.IntegrityError as error:
            if _is_foreign_key_constraint(error):
                raise EntryNotFoundError("DB does not contain simulation "
                                         "with ID `%s`" % simulation_id)
            elif _is_unique_constraint(error):
                raise EntryExistsError("Table `queue` already contains "
                                       "simulation with ID `%s`" %
                                       simulation_id)

            # Just re-raise any other errors
            raise

    def insert_in_running(self, simulation_id: str, simulator_id: str):
        """
        Inserts the simulation with the specified ID in the `running` table.
        It associates the simulation with the ID of the simulator that was
        assigned to execute it.

        To insert a simulation in the `running` table, the simulation and the
        simulator must have already been inserted in the DB.

        :param simulation_id: ID of the simulation to insert in `running`
        :param simulator_id:  ID of the simulator to execute the simulation
        :raise EntryNotFoundError: if the DB does not contain a simulation
        and/or a simulator with the specified IDs
        :raise EntryExistsError: if a simulation with the same ID is already
        in the `running` table
        """
        try:
            self._connection.cursor().execute(
                "INSERT INTO running VALUES (?, ?)",
                (simulator_id, simulation_id))

        except sqlite3.IntegrityError as error:

            if _is_foreign_key_constraint(error):
                raise EntryNotFoundError("DB does not contain simulation "
                                         "with ID `%s`" % simulation_id)
            elif _is_unique_constraint(error):
                raise EntryExistsError("Table `running` already contains "
                                       "simulation with ID `%s`" %
                                       simulation_id)

            # Just re-raise any other errors
            raise

    def insert_in_complete(self, simulation_id: str, simulator_id: str,
                           finish_datetime: datetime):
        """
        Inserts the simulation with the specified ID in the `complete` table.
        It associates the simulation with the ID of the simulator that
        executed the simulation and the datetime at which it finished executing.

        To insert a simulation in the `complete` table, the simulation and the
        simulator must have already been inserted in the DB.

        :param simulation_id:   ID of the simulation to insert in `complete`
        :param simulator_id:    ID of the simulator to execute the simulation
        :param finish_datetime: datetime when the simulation finished executing
        :raise EntryNotFoundError: if the DB does not contain a simulation
        and/or a simulator with the specified IDs
        :raise EntryExistsError: if a simulation with the same ID is already
        in the `complete` table
        """
        try:
            self._connection.cursor().execute(
                "INSERT INTO complete VALUES (?, ?, ?)",
                (simulator_id, simulation_id,
                 finish_datetime.strftime(self.DATETIME_FORMAT)))

        except sqlite3.IntegrityError as error:
            if _is_foreign_key_constraint(error):
                raise EntryNotFoundError("DB does not contain simulation "
                                         "with ID `%s`" % simulation_id)
            elif _is_unique_constraint(error):
                raise EntryExistsError("Table `complete` already contains "
                                       "simulation with ID `%s`" %
                                       simulation_id)

            # Just re-raise any other errors
            raise

    def delete_simulation(self, simulation_id: str):
        """
        Deletes a simulation from the DB. It deletes the simulation from all
        tables of the DB.

        If the DB does not contain a simulation with the specified ID,
        then the method has no effect.

        :param simulation_id: ID of the simulation to delete
        """
        self._connection.cursor().execute(
            "DELETE FROM simulation WHERE id=?", (simulation_id,))

    def delete_from_queue(self, simulation_id: str):
        """
        Deletes a simulation from the `queue` table.

        If the `queue` table does not contain a simulation with the specified
        ID, then the method has no effect.

        :param simulation_id: ID of the simulation to delete
        """
        self._connection.cursor().execute(
            "DELETE FROM queue WHERE simulation_id=?", (simulation_id,))

    def delete_from_running(self, simulation_id: str):
        """
        Deletes a simulation from the `queue` table.

        If the `queue` table does not contain a simulation with the specified
        ID, then the method has no effect.

        :param simulation_id: ID of the simulation to delete
        """
        self._connection.cursor().execute(
            "DELETE FROM running WHERE simulation_id=?", (simulation_id,))

    # WARNING: Usually, we don't want to delete simulations from the
    # `complete` table

    # noinspection PyTypeChecker
    def simulators(self):
        """
        Generator that returns the IDs of each simulator included in the
        `simulator` table.
        """
        cursor = self._connection.cursor()
        cursor.execute("SELECT * FROM simulator")

        row = cursor.fetchone()
        while row:
            yield row['id']
            row = cursor.fetchone()

    def all_simulations(self):
        """
        Generator that returns each simulation that was inserted in the
        database. This includes queued, running, and complete simulations.
        """
        cursor = self._connection.cursor()
        cursor.execute("SELECT * FROM simulation")

        row = cursor.fetchone()
        while row:
            yield _simulation_fromrow(row)
            row = cursor.fetchone()

    # noinspection PyTypeChecker
    def queued_simulations(self):
        """
        Generator that returns each simulation in the `queue` table ordered
        by priority: simulations with higher priority first. Along with the
        simulation it also returns the corresponding priority.
        """
        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * FROM simulation JOIN queue ON id == simulation_id;")

        row = cursor.fetchone()
        while row:
            yield _simulation_fromrow(row), row['priority']
            row = cursor.fetchone()

    # noinspection PyTypeChecker
    def running_simulations(self):
        """
        Generator that returns each simulation in the `running` table in no
        particular order. Along with the simulation it also returns the
        ID of the simulator executing the simulation.
        """
        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * FROM simulation JOIN running ON id == simulation_id;")

        row = cursor.fetchone()
        while row:
            yield _simulation_fromrow(row), row['simulator_id']
            row = cursor.fetchone()

    # noinspection PyTypeChecker
    def complete_simulations(self):
        """
        Generator that returns each simulation in the `complete` table in no
        particular order. Along with the simulation it also returns the
        ID of the simulator the executed the simulation and the finish datetime.
        """
        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * FROM simulation JOIN complete ON id == simulation_id;")

        row = cursor.fetchone()
        while row:
            simulation = _simulation_fromrow(row)
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

    def commit(self):
        """ Commits the current operations """
        self._connection.commit()

    def rollback(self):
        """ Rolls back the operations performed since the last commit """
        self._connection.rollback()

    def close(self):
        """ Closes the connection. Afterwards, the connection cannot be used """
        self._connection.close()

    def execute_script(self, fp):
        """ Executes a script from an opened file """
        script = fp.read()
        self._connection.cursor().executescript(script)


class SimulationDatabase:
    """ Abstraction o access the simulations database """

    # Path to script used to create the DB tables
    CREATE_TABLES_SCRIPT = resource_filename(
        Requirement.parse("ssbgp-dss-dispatcher"), 'dss_dispatcher/tables.sql')

    def __init__(self, db_file):
        """
        Initializes a new simulation DB.

        :param db_file: path to DB file
        """
        self._db_file = db_file

        # Create tables if necessary
        with open(self.CREATE_TABLES_SCRIPT) as file:
            with self.connect() as connection:
                connection.execute_script(file)

    @contextmanager
    def connect(self) -> Connection:
        """
        Connects to the database and yields a connection. It closes the
        connection when it is called to exit.
        """
        with sqlite3.connect(database=self._db_file) as connection:
            yield Connection(connection)


def _is_unique_constraint(error: sqlite3.IntegrityError):
    return 'UNIQUE constraint failed' in str(error)


def _is_foreign_key_constraint(error: sqlite3.IntegrityError):
    return 'FOREIGN KEY constraint failed' in str(error)


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
