CREATE TABLE IF NOT EXISTS simulation (
  id          TEXT PRIMARY KEY,
  report_path TEXT NOT NULL,
  topology    TEXT NOT NULL,
  destination INT  NOT NULL,
  repetitions INT  NOT NULL,
  min_delay    INT  NOT NULL,
  max_delay    INT  NOT NULL,
  threshold   INT  NOT NULL,
  stubs_file  TEXT NOT NULL,
  seed        INT  NOT NULL
);


CREATE TABLE IF NOT EXISTS simulator (
  id TEXT PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS queue (
  simulation_id TEXT PRIMARY KEY,
  priority      INT NOT NULL,

  FOREIGN KEY (simulation_id) REFERENCES simulation
    ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS running (
  simulator_id  TEXT,
  simulation_id TEXT,

  -- Here we do not want to allow deleting simulators associated with
  -- simulations
  FOREIGN KEY (simulator_id) REFERENCES simulator
    ON DELETE NO ACTION,
  FOREIGN KEY (simulation_id) REFERENCES simulation
    ON DELETE CASCADE,

  PRIMARY KEY (simulator_id, simulation_id)
);


CREATE TABLE IF NOT EXISTS complete (
  simulator_id  TEXT,
  simulation_id TEXT,
  finish_datetime TEXT NOT NULL,

  -- Here we do not want to allow deleting simulators associated with
  -- simulations
  FOREIGN KEY (simulator_id) REFERENCES simulator
    ON DELETE NO ACTION,
  FOREIGN KEY (simulation_id) REFERENCES simulation
    ON DELETE CASCADE,

  PRIMARY KEY (simulator_id, simulation_id)
);
