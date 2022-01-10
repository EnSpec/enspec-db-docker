-- TABLE
DROP TABLE IF EXISTS flights CASCADE;
CREATE TABLE flights (
  flights_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  flight_date DATE NOT NULL,
  pilot TEXT NOT NULL,
  operator TEXT NOT NULL,
  flight_hours FLOAT NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  flight_notes TEXT
);
CREATE INDEX flights_source_id_idx ON flights(source_id);

-- VIEW
CREATE OR REPLACE VIEW flights_view AS
  SELECT
    f.flights_id AS flights_id,
    f.flight_date  as flight_date,
    f.pilot  as pilot,
    f.operator  as operator,
    f.flight_hours  as flight_hours,
    f.start_time  as start_time,
    f.end_time  as end_time,
    f.flight_notes as flight_notes,

    sc.name AS source_name
  FROM
    flights f
LEFT JOIN source sc ON f.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_flights (
  flights_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  flight_hours FLOAT,
  start_time TIME,
  end_time TIME,
  flight_notes TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( flights_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO flights_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO flights (
    flights_id, flight_date, pilot, operator, flight_hours, start_time, end_time, flight_notes, source_id
  ) VALUES (
    flights_id, flight_date, pilot, operator, flight_hours, start_time, end_time, flight_notes, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flights (
  flights_id_in UUID,
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  flight_hours_in FLOAT,
  start_time_in TIME,
  end_time_in TIME,
  flight_notes_in TEXT) RETURNS void AS $$

BEGIN

  UPDATE flights SET (
    flight_date, pilot, operator, flight_hours, start_time, end_time, flight_notes
  ) = (
    flight_date_in, pilot_in, operator_in, flight_hours_in, start_time_in, end_time_in, flight_notes_in
  ) WHERE
    flights_id = flights_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_flights_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_flights(
    flights_id := NEW.flights_id,
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    flight_hours := NEW.flight_hours,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    flight_notes := NEW.flight_notes,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flights_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_flights(
    flights_id_in := NEW.flights_id,
    flight_date_in := NEW.flight_date,
    pilot_in := NEW.pilot,
    operator_in := NEW.operator,
    flight_hours_in := NEW.flight_hours,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    flight_notes_in := NEW.flight_notes
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_flights_id(flights_date_in date, pilot_in text, operator_in text, flight_hours_in float, start_time_in time, end_time_in time, flight_notes_in text) RETURNS UUID AS $$
DECLARE
  fid UUID;
BEGIN

  SELECT
    flights_id INTO fid
  FROM
    flights f
  WHERE
    flights_date = flights_date_in AND
    pilot = pilot_in AND
    operator = operator_in AND
    flight_hours = flight_hours_in AND
    start_time = start_time_in AND
    end_time = end_time_in;

  IF (fid IS NULL) THEN
    RAISE EXCEPTION 'Unknown flights: flight_date="%" pilot="%" operator="%" flight_hours="%" start_time="%" end_time="%" flight_notes="%"', flight_date, pilot_in, operator_in, flight_hours_in, start_time_in, end_time_in, flight_notes_in;
  END IF;

  RETURN fid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER flights_insert_trig
  INSTEAD OF INSERT ON
  flights_view FOR EACH ROW
  EXECUTE PROCEDURE insert_flights_from_trig();

CREATE TRIGGER flights_update_trig
  INSTEAD OF UPDATE ON
  flights_view FOR EACH ROW
  EXECUTE PROCEDURE update_flights_from_trig();
