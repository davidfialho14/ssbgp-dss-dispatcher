from pytest import fixture, raises

from dss_dispatcher.database import SimulationDatabase, EntryExistsError, \
    EntryNotFoundError
from dss_dispatcher.simulation import simulation_with


@fixture
def database(tmpdir):
    return SimulationDatabase(str(tmpdir.join("file.db")))


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
def test_InsertASimulation_EmptyDB_TheDBContainsThatSimulation(database):
    simulation = create_simulation("#1234")

    database.insert_simulation(simulation)

    assert simulation in list(database.all_simulations())


# noinspection PyShadowingNames
def test_InsertMultipleSimulations_TheDBContainsAllSimulations(database):
    simulation1 = create_simulation("#1234")
    simulation2 = create_simulation("#5678")

    database.insert_simulation(simulation1)
    database.insert_simulation(simulation2)

    assert simulation1 in list(database.all_simulations())
    assert simulation2 in list(database.all_simulations())


# noinspection PyShadowingNames
def test_InsertSameSimulationTwice_RaisesEntryExistsError(database):
    simulation = create_simulation("#1234")
    database.insert_simulation(simulation)

    with raises(EntryExistsError):
        database.insert_simulation(simulation)


# noinspection PyShadowingNames
def test_MoveExistingSimToEmptyQueue_QueueTableThatSingleSim(database):
    simulation = create_simulation("#1234")
    database.insert_simulation(simulation)
    database.insert_simulation(create_simulation("#5678"))
    database.insert_simulation(create_simulation("#1357"))

    database.moveto_queue(simulation.id, priority=10)

    queue_sims = list(database.queued_simulations())
    assert (simulation, 10) in queue_sims
    assert len(queue_sims) == 1


# noinspection PyShadowingNames
def test_MoveNonExistingSimToQueue_RaisesEntryNotFoundError(database):
    simulation = create_simulation("#1234")
    database.insert_simulation(create_simulation("#5678"))
    database.insert_simulation(create_simulation("#1357"))

    with raises(EntryNotFoundError):
        database.moveto_queue(simulation.id, priority=10)


# noinspection PyShadowingNames
def test_MoveSimToQueueThatIsAlreadyInTheQueue_RaisesEntryExistsError(database):
    simulation = create_simulation("#1234")
    database.insert_simulation(create_simulation("#1234"))
    database.moveto_queue(simulation.id, priority=10)

    with raises(EntryExistsError):
        database.moveto_queue(simulation.id, priority=10)


# noinspection PyShadowingNames
def test_InsertASimulator_EmptyDB_TheDBContainsThatSimulatorID(database):
    database.insert_simulator(id="#1234")

    assert "#1234" in list(database.simulators())


# noinspection PyShadowingNames
def test_InsertMultipleSimulators_TheDBContainsAllSimulators(database):
    database.insert_simulator(id="#1234")
    database.insert_simulator(id="#5678")

    simulators = list(database.simulators())
    assert "#1234" in simulators
    assert "#5678" in simulators


# noinspection PyShadowingNames
def test_MoveSimToRunningTable_RunningTableContainsSimAndQueueDoesNot(database):
    simulation = create_simulation("#1234")
    database.insert_simulation(simulation)
    database.insert_simulator(id="sim#1234")
    database.moveto_queue(simulation.id, priority=10)

    database.moveto_running(simulation.id, simulator_id="sim#1234")

    assert (simulation, "sim#1234") in list(database.running_simulations())
    assert (simulation, 10) not in list(database.queued_simulations())
