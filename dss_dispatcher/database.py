import sqlite3
from contextlib import contextmanager
from datetime import datetime

from pkg_resources import resource_filename

from dss_dispatcher.simulation import Simulation


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

    # region Insert Methods

    def insert_simulator(self, id: str):
        """
        Inserts a new simulator in the database.

        :param id: ID of the new simulator
        :raise EntryExistsError: if the DB contains a simulator with the same ID
        """
        self._insert_in('simulator', id)

    def insert_simulation(self, simulation: Simulation):
        """
        Inserts a new simulation in the database.

        :param simulation: simulation to insert
        :raise EntryExistsError: if the DB contains a simulation with the
        same ID of the simulation to be inserted
        """
        self._insert_in('simulation', *simulation)

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
        self._insert_in('queue', simulation_id, priority)

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
        self._insert_in('running', simulation_id, simulator_id)

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
        self._insert_in('complete', simulation_id, simulator_id,
                        finish_datetime.strftime(self.DATETIME_FORMAT))

    # endregion

    # region Delete Methods

    def delete_simulation(self, simulation_id: str):
        """
        Deletes a simulation from the DB. It deletes the simulation from all
        tables of the DB.

        If the DB does not contain a simulation with the specified ID,
        then the method has no effect.

        :param simulation_id: ID of the simulation to delete
        """
        self._delete_from("simulation", simulation_id)

    def delete_from_queue(self, simulation_id: str):
        """
        Deletes a simulation from the `queue` table.

        If the `queue` table does not contain a simulation with the specified
        ID, then the method has no effect.

        :param simulation_id: ID of the simulation to delete
        """
        self._delete_from("queue", simulation_id)

    def delete_from_running(self, simulation_id: str):
        """
        Deletes a simulation from the `queue` table.

        If the `queue` table does not contain a simulation with the specified
        ID, then the method has no effect.

        :param simulation_id: ID of the simulation to delete
        """
        self._delete_from("running", simulation_id)

    # endregion

    # region List Methods

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

    # WARNING: Usually, we don't want to delete simulations from the
    # `complete` table

    # noinspection PyTypeChecker
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
            "SELECT * FROM simulation JOIN queue "
            "ON simulation.id == queue.id;")

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
            "SELECT * FROM simulation JOIN running "
            "ON simulation.id == running.id;")

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
            "SELECT * FROM simulation JOIN complete "
            "ON simulation.id == complete.id")

        row = cursor.fetchone()
        while row:
            simulation = _simulation_fromrow(row)
            finish_datetime = datetime.strptime(row['finish_datetime'],
                                                self.DATETIME_FORMAT)

            yield simulation, row['simulator_id'], finish_datetime
            row = cursor.fetchone()

    # endregion

    def next_simulation(self) -> Simulation:
        """
        Returns the simulation in the queue with the highest priority. If
        there are multiple simulations with the same priority value,
        it returns one of them.

        :return: simulation from the queue with the highest priority or None
        if the `queue` table is empty
        """
        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * FROM simulation JOIN queue ON simulation.id == queue.id "
            "WHERE priority IN (SELECT max(priority) FROM queue);")

        row = cursor.fetchone()
        if row:
            return _simulation_fromrow(row)

    def running_simulation(self, simulator_id: str) -> Simulation:
        """
        Returns the simulation associated with the specified simulator in the
        `running` table.

        :param simulator_id: ID of simulator to obtain simulation for
        :return: simulation associated with the specified simulator or None
        if there is not one
        """
        cursor = self._connection.cursor()
        cursor.execute(
            "SELECT * "
            "FROM simulation JOIN running ON simulation.id = running.id "
            "WHERE simulator_id = ?", (simulator_id, ))

        row = cursor.fetchone()
        if row:
            return _simulation_fromrow(row)

    def execute_script(self, fp):
        """ Executes a script from an opened file """
        script = fp.read()
        self._connection.cursor().executescript(script)

    def commit(self):
        """ Commits the current operations """
        self._connection.commit()

    def rollback(self):
        """ Rolls back the operations performed since the last commit """
        self._connection.rollback()

    def close(self):
        """ Closes the connection. Afterwards, the connection cannot be used """
        self._connection.close()

    def _insert_in(self, table: str, id, *values):
        """
        Inserts an entry into a table with the specified name.

        :param table:  name of the table to insert entry to
        :param id:     ID to assign the entry
        :param values: extra values the entry includes
        :raise EntryExistsError: if the exists in the table
        :raise EntryNotFoundError: if a required entry does not exist in the
        database. This can be a simulation or a simulator.
        """
        try:
            expected_values = ",".join("?" for i in range(len(values) + 1))
            all_values = [id]
            all_values.extend(values)

            self._connection.cursor().execute(
                "INSERT INTO %s VALUES (%s)" % (table, expected_values),
                all_values)

        except sqlite3.IntegrityError as error:
            if _is_foreign_key_constraint(error):
                raise EntryNotFoundError("DB `%s` does not contain entry "
                                         "with ID `%s`" % (table, id))

            elif _is_unique_constraint(error):
                raise EntryExistsError("Table `%s` already contains entry with "
                                       "ID `%s`" % (table, id))

            # Just re-raise any other errors
            raise
            # noinspection PyTypeChecker

    def _delete_from(self, table: str, entry_id: str):
        """
        Deletes the entry with the specified ID from the table with the
        specified name.

        :param table:     name of the table to delete entry from
        :param entry_id:  ID of the entry to delete
        """
        # Make sure that in all tables the id column is called `id`
        self._connection.cursor().execute(
            "DELETE FROM %s WHERE id=?" % table, (entry_id,))


class SimulationDB:
    """ Abstraction o access the simulations database """

    # Path to script used to create the DB tables
    CREATE_TABLES_SCRIPT = resource_filename(__name__, 'tables.sql')

    def __init__(self, db_file: str):
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
    return Simulation(
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
        enable_reportnodes=True if row['reportnodes'] == 1 else False
    )
