-- TABLE
DROP TABLE IF EXISTS flight_installation CASCADE;
CREATE TABLE flight_installation (
  flight_installation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  flights_id UUID REFERENCES flights NOT NULL,
  installations_id UUID REFERENCES installations NOT NULL
);
CREATE INDEX flight_installation_source_id_idx ON flight_installation(source_id);
CREATE INDEX flight_installation_flights_id_idx ON flight_installation(flights_id);
CREATE INDEX flight_installation_installations_id_idx ON flight_installation(installations_id);

ALTER TABLE flight_installation ADD CONSTRAINT uniq_flight_instl_row UNIQUE(flights_id, installations_id);

-- VIEW
CREATE OR REPLACE VIEW flight_installation_view AS
  SELECT
    f.flight_installation_id AS flight_installation_id,
    fl.flight_date AS flight_date,
    fl.pilot AS pilot,
    fl.operator AS operator,
    fl.flight_hours AS flight_hours,
    fl.liftoff_time AS liftoff_time,
    fl.landing_time AS landing_time,
    fl.tachometer_start AS tachometer_start,
    fl.tachometer_end AS tachometer_end,
    fl.hobbs_start AS hobbs_start,
    fl.hobbs_end AS hobbs_end,
    fl.flight_notes AS flight_notes,
    i.install_date AS install_date,
    i.removal_date AS removal_date,
    i.dir_location AS dir_location,
    sc.name AS source_name
  FROM
    flight_installation f
    LEFT JOIN source sc ON f.source_id = sc.source_id
    LEFT JOIN flights fl ON f.flights_id = fl.flights_id
    LEFT JOIN installations i ON f.installations_id = i.installations_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_flight_installation (
  flight_installation_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  flight_hours FLOAT,
  liftoff_time TIME,
  landing_time TIME,
  tachometer_start FLOAT,
  tachometer_end FLOAT,
  hobbs_start FLOAT,
  hobbs_end FLOAT,
  flight_notes TEXT,
  install_date DATE,
  removal_date DATE,
  dir_location TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  flid UUID;
  iid UUID;
BEGIN

  IF( flight_installation_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO flight_installation_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_flights_id(flight_date, pilot, operator, liftoff_time) INTO flid;
  SELECT get_installations_id(install_date, removal_date, dir_location) INTO iid;

  INSERT INTO flight_installation (
    flight_installation_id, flights_id, installations_id, source_id
  ) VALUES (
    flight_installation_id, flid, iid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flight_installation (
  flight_installation_id_in UUID,
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  flight_hours_in FLOAT,
  liftoff_time_in TIME,
  landing_time_in TIME,
  tachometer_start_in FLOAT,
  tachometer_end_in FLOAT,
  hobbs_start_in FLOAT,
  hobbs_end_in FLOAT,
  flight_notes_in TEXT,
  install_date_in DATE,
  removal_date_in DATE,
  dir_location_in TEXT) RETURNS void AS $$
DECLARE
 flid UUID;
 iid UUID;
BEGIN
  SELECT get_flights_id(flight_date_in, pilot_in, operator_in, liftoff_time_in) INTO flid;
  SELECT get_installations_id(install_date_in, removal_date_in, dir_location_in) INTO iid;
  UPDATE flight_installation SET (
    flights_id, installations_id
  ) = (
    flid, iid
  ) WHERE
    flight_installation_id = flight_installation_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_flight_installation_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_flight_installation(
    flight_installation_id := NEW.flight_installation_id,
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    flight_hours := NEW.flight_hours,
    liftoff_time := NEW.liftoff_time,
    landing_time := NEW.landing_time,
    tachometer_start := NEW.tachometer_start,
    tachometer_end := NEW.tachometer_end,
    hobbs_start := NEW.hobbs_start,
    hobbs_end := NEW.hobbs_end,
    flight_notes := NEW.flight_notes,
    install_date := NEW.install_date,
    removal_date := NEW.removal_date,
    dir_location := NEW.dir_location,
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flight_installation_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_flight_installation(
    flight_installation_id_in := NEW.flight_installation_id,
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    flight_hours := NEW.flight_hours,
    liftoff_time := NEW.liftoff_time,
    landing_time := NEW.landing_time,
    tachometer_start := NEW.tachometer_start,
    tachometer_end := NEW.tachometer_end,
    hobbs_start := NEW.hobbs_start,
    hobbs_end := NEW.hobbs_end,
    flight_notes := NEW.flight_notes,
    install_date := NEW.install_date,
    removal_date := NEW.removal_date,
    dir_location := NEW.dir_location
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_flight_installation_id(
  flight_date date, pilot text, operator text, flight_hours float, liftoff_time time, landing_time time, tachometer_start FLOAT, tachometer_end FLOAT, hobbs_start FLOAT, hobbs_end FLOAT, flight_notes text, install_date date, removal_date date, dir_location text) RETURNS UUID AS $$
DECLARE
  fid UUID;
  flid UUID;
  iid UUID;
BEGIN
  SELECT get_flights_id(flight_date, pilot, operator, liftoff_time) INTO flid;
  SELECT get_installations_id(install_date, removal_date, dir_location) INTO iid;
  SELECT
    flight_installation_id INTO fid
  FROM
    flight_installation f
  WHERE
    flights_id = flid AND
    installations_id = iid;
  IF (fid IS NULL) THEN
    RAISE EXCEPTION 'Unknown flight_installation: flight_date="%" pilot="%" operator"%" flight_hours"%" liftoff_time="%" landing_time="%" tachometer_start="%" tachometer_end="%" hobbs_start="%" hobbs_end="%" flight_notes="%" install_date"%" removal_date"%" dir_location"%"',
    flight_date, pilot, operator, flight_hours, liftoff_time, landing_time, tachometer_start, tachometer_end, hobbs_start, hobbs_end, flight_notes, install_date, removal_date, dir_location;
  END IF;

  RETURN fid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER flight_installation_insert_trig
  INSTEAD OF INSERT ON
  flight_installation_view FOR EACH ROW
  EXECUTE PROCEDURE insert_flight_installation_from_trig();

CREATE TRIGGER flight_installation_update_trig
  INSTEAD OF UPDATE ON
  flight_installation_view FOR EACH ROW
  EXECUTE PROCEDURE update_flight_installation_from_trig();
