from datetime import datetime

from pytest import fixture, raises

from dss_dispatcher.database import SimulationDB, EntryExistsError, \
    EntryNotFoundError
from dss_dispatcher.simulation import simulation_with


@fixture
def database(tmpdir):
    return SimulationDB(str(tmpdir.join("file.db")))


def create_simulation(id: str):
    return simulation_with(
        id=id,
        report_path="reports",
        topology="topology.nf",
        destination=0,
        repetitions=1,
        min_delay=1,
        max_delay=1,
        threshold=1,
        stubs_file="topology.stubs",
        seed=1234
    )


# noinspection PyShadowingNames
def test_InsertSimulator_EmptyDB_TheDBContainsThatSimulator(database):
    with database.connect() as connection:
        simulator_id = "#sim1"
        connection.insert_simulator(simulator_id)

        assert simulator_id in list(connection.simulators())


# noinspection PyShadowingNames
def test_InsertMultipleSimulations_TheDBContainsAllSimulations(database):
    with database.connect() as connection:
        simulator_id1 = "#sim1"
        simulator_id2 = "#sim2"

        connection.insert_simulator(simulator_id1)
        connection.insert_simulator(simulator_id2)

        simulators = list(connection.simulators())
        assert simulator_id1 in simulators
        assert simulator_id2 in simulators


# noinspection PyShadowingNames
def test_InsertSimulation_EmptyDB_TheDBContainsThatSimulation(database):
    with database.connect() as connection:
        simulation = create_simulation("#1234")

        connection.insert_simulation(simulation)

        assert simulation in list(connection.all_simulations())


# noinspection PyShadowingNames
def test_InsertMultipleSimulators_TheDBContainsAllSimulators(database):
    with database.connect() as connection:
        simulation1 = create_simulation("#1234")
        simulation2 = create_simulation("#5678")

        connection.insert_simulation(simulation1)
        connection.insert_simulation(simulation2)

        assert simulation1 in list(connection.all_simulations())
        assert simulation2 in list(connection.all_simulations())


# noinspection PyShadowingNames
def test_InsertSameSimulationTwice_RaisesEntryExistsError(database):
    with database.connect() as connection:
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)

        with raises(EntryExistsError):
            connection.insert_simulation(simulation)


# noinspection PyShadowingNames
def test_InsertSimInQueue_SimExistsInDB_QueueContainsThatSim(database):
    with database.connect() as connection:
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)

        connection.insert_in_queue(simulation.id, priority=10)

        assert (simulation, 10) in list(connection.queued_simulations())


# noinspection PyShadowingNames
def test_InsertSimInQueue_SimNotInDB_RaisesEntryNotFoundError(database):
    with database.connect() as connection:
        simulation = create_simulation("#1234")

        with raises(EntryNotFoundError):
            connection.insert_in_queue(simulation.id, priority=10)


# noinspection PyShadowingNames
def test_InsertSimInQueue_SimAlreadyInQueue_RaisesEntryNotFoundError(database):
    with database.connect() as connection:
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)
        connection.insert_in_queue(simulation.id, priority=10)

        with raises(EntryExistsError):
            connection.insert_in_queue(simulation.id, priority=10)


# noinspection PyShadowingNames
def test_InsertSimInRunning_SimExistsInDB_RunningContainsThatSim(database):
    with database.connect() as connection:
        connection.insert_simulator("#sim1")
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)

        connection.insert_in_running(simulation.id, simulator_id="#sim1")

        assert (simulation, "#sim1") in list(connection.running_simulations())


# noinspection PyShadowingNames
def test_InsertSimInRunning_SimNotInDB_RaisesEntryNotFoundError(database):
    with database.connect() as connection:
        connection.insert_simulator("#sim1")
        simulation = create_simulation("#1234")

        with raises(EntryNotFoundError):
            connection.insert_in_running(simulation.id, simulator_id="#sim1")


# noinspection PyShadowingNames
def test_InsertSimInRunning_SimulatorNotInDB_RaisesEntryNotFoundError(database):
    with database.connect() as connection:
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)

        with raises(EntryNotFoundError):
            connection.insert_in_running(simulation.id, simulator_id="#sim1")


# noinspection PyShadowingNames
def test_InsertSimInRunning_SimInRunning_RaisesEntryNotFoundError(database):
    with database.connect() as connection:
        connection.insert_simulator("#sim1")
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)
        connection.insert_in_running(simulation.id, simulator_id="#sim1")

        with raises(EntryExistsError):
            connection.insert_in_running(simulation.id, simulator_id="#sim1")


# noinspection PyShadowingNames
def test_InsertSimInComplete_SimExistsInDB_CompleteContainsThatSim(database):
    with database.connect() as connection:
        connection.insert_simulator("#sim1")
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)
        finish_datetime = datetime(1900, 1, 1, 1, 1)

        connection.insert_in_complete(simulation.id, "#sim1", finish_datetime)

        assert (simulation, "#sim1", finish_datetime) in list(
            connection.complete_simulations())


# noinspection PyShadowingNames
def test_InsertSimInComplete_SimNotInDB_RaisesEntryNotFoundError(database):
    with database.connect() as connection:
        connection.insert_simulator("#sim1")
        simulation = create_simulation("#1234")

        with raises(EntryNotFoundError):
            connection.insert_in_complete(simulation.id, "#sim1",
                                          datetime(1900, 1, 1, 1, 1))


# noinspection PyShadowingNames
def test_InsertSimInComplete_SimulatorNotInDB_RaisesEntryNotFoundError(
        database):
    with database.connect() as connection:
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)

        with raises(EntryNotFoundError):
            connection.insert_in_complete(simulation.id, "#sim1",
                                          datetime(1900, 1, 1, 1, 1))


# noinspection PyShadowingNames
def test_InsertSimInComplete_SimInComplete_RaisesEntryNotFoundError(database):
    with database.connect() as connection:
        connection.insert_simulator("#sim1")
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)
        finish_datetime = datetime(1900, 1, 1, 1, 1)
        connection.insert_in_complete(simulation.id, "#sim1", finish_datetime)

        with raises(EntryExistsError):
            connection.insert_in_complete(simulation.id, "#sim1",
                                          finish_datetime)


# noinspection PyShadowingNames
def test_DeleteExistingSimulation_DBDoesNotContainThatSimInAnyTable(database):
    with database.connect() as connection:
        connection.insert_simulator("#sim1")
        simulation = create_simulation("#1234")
        connection.insert_simulation(simulation)
        connection.insert_in_queue(simulation.id, 10)
        connection.insert_in_running(simulation.id, "#sim1")
        finish_datetime = datetime(1900, 1, 1, 1, 1)
        connection.insert_in_complete(simulation.id, "#sim1", finish_datetime)

        connection.delete_simulation("#1234")

        assert simulation not in list(connection.all_simulations())
        assert (simulation, 10) not in list(connection.queued_simulations())
        assert (simulation, "#sim1") not in list(
            connection.running_simulations())
        assert (simulation, "#sim1", finish_datetime) not in list(
            connection.complete_simulations())


# noinspection PyShadowingNames
def test_NextSimulation_QueueContainsSimsWithPriority1and10and30_ReturnsSim30(
        database):
    with database.connect() as connection:
        connection.insert_simulator("#sim1")
        connection.insert_simulation(create_simulation("#1"))
        connection.insert_simulation(create_simulation("#2"))
        connection.insert_simulation(create_simulation("#3"))
        connection.insert_in_queue("#1", 10)
        connection.insert_in_queue("#2", 30)
        connection.insert_in_queue("#3", 1)

        assert connection.next_simulation() == create_simulation("#2")


# noinspection PyShadowingNames
def test_NextSimulation_QueueTableIsEmpty_ReturnNone(
        database):
    with database.connect() as connection:
        assert connection.next_simulation() is None
