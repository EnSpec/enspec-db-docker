-- TABLE
DROP TABLE IF EXISTS sessions CASCADE;
CREATE TABLE sessions (
  sessions_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  flights_id UUID REFERENCES flights NOT NULL,
  session_name TEXT NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  line_count FLOAT NOT NULL,
  session_notes TEXT
);
CREATE INDEX sessions_source_id_idx ON sessions(source_id);

ALTER TABLE sessions ADD CONSTRAINT uniq_ses_row UNIQUE(flights_id, session_name);

-- VIEW
CREATE OR REPLACE VIEW sessions_view AS
  SELECT
    s.sessions_id AS sessions_id,
    f.flight_date  as flight_date,
    f.pilot  as pilot,
    f.operator  as operator,
    f.liftoff_time  as liftoff_time,
    s.session_name  as session_name,
    s.start_time  as start_time,
    s.end_time  as end_time,
    s.line_count as line_count,
    s.session_notes as session_notes,
    sc.name AS source_name
  FROM
    sessions s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN flights f ON s.flights_id = f.flights_id;
--get_flight_line_count(s.sessions_id) as line_count,

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_sessions (
  sessions_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  liftoff_time TIME,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  session_notes TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  fl_id UUID;
BEGIN

  IF( sessions_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_flights_id(flight_date, pilot, operator, liftoff_time) INTO fl_id;

  INSERT INTO sessions (
    sessions_id, flights_id, session_name, start_time, end_time, line_count, session_notes, source_id
  ) VALUES (
    sessions_id, fl_id, session_name, start_time, end_time, line_count, session_notes, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- for Epicollect data entry
CREATE OR REPLACE FUNCTION insert_epi_sessions (
  sessions_id UUID,
  flights_id UUID,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  session_notes TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( sessions_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO sessions (
    sessions_id, flights_id, session_name, start_time, end_time, line_count, session_notes, source_id
  ) VALUES (
    sessions_id, flights_id, session_name, start_time, end_time, line_count, session_notes, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- for Epicollect data entry which also contain metadata to insert in sessions_metadata table
CREATE OR REPLACE FUNCTION insert_epi_sessions_and_metadata (
  sessions_id UUID,
  flights_id UUID,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  session_notes TEXT,
  target_groundspeed_kmh FLOAT,
  target_resolution FLOAT,
  target_altitude_agl FLOAT,
  sidelap FLOAT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( sessions_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO sessions_metadata (
    sessions_id, variables_id, units_id, value
  ) VALUES (
    sessions_id, get_lightweight_variables_id("target_groundspeed"), get_lightweight_units_id("kmh"), target_groundspeed_kmh
  );

  INSERT INTO sessions_metadata (
    sessions_id, variables_id, units_id, value
  ) VALUES (
    sessions_id, get_lightweight_variables_id("target_resolution"), get_lightweight_units_id("None"), target_resolution
  );

  INSERT INTO sessions_metadata (
    sessions_id, variables_id, units_id, value
  ) VALUES (
    sessions_id, get_lightweight_variables_id("target_altitude"), get_lightweight_units_id("agl"), target_altitude_agl
  );

  INSERT INTO sessions_metadata (
    sessions_id, variables_id, units_id, value
  ) VALUES (
    sessions_id, get_lightweight_variables_id("sidelap"), get_lightweight_units_id("None"), sidelap
  );

  INSERT INTO sessions (
    sessions_id, flights_id, session_name, start_time, end_time, line_count, session_notes, source_id
  ) VALUES (
    sessions_id, flights_id, session_name, start_time, end_time, line_count, session_notes, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions (
  sessions_id_in UUID,
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  liftoff_time_in TIME,
  session_name_in TEXT,
  start_time_in TIME,
  end_time_in TIME,
  line_count_in FLOAT,
  session_notes_in TEXT) RETURNS void AS $$
DECLARE
fl_id UUID;
BEGIN
  SELECT get_flights_id(flight_date_in, pilot_in, operator_in, liftoff_time_in) INTO fl_id;
  UPDATE sessions SET (
    flights_id, session_name, start_time, end_time, line_count, session_notes
  ) = (
    fl_id, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in
  ) WHERE
    sessions_id = sessions_id_in;
EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_sessions (
  sessions_id_in UUID,
  flights_id_in UUID,
  session_name_in TEXT,
  start_time_in TIME,
  end_time_in TIME,
  line_count_in FLOAT,
  session_notes_in TEXT) RETURNS void AS $$
DECLARE
fl_id UUID;
BEGIN
  UPDATE sessions SET (
    flights_id, session_name, start_time, end_time, line_count, session_notes
  ) = (
    flights_id_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in
  ) WHERE
    sessions_id = sessions_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_epi_sessions_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_sessions(
    flights_id := NEW.flights_id,
    sessions_id := NEW.sessions_id,
    session_name := NEW.session_name,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    line_count := NEW.line_count,
    session_notes := NEW.session_notes,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_sessions_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_sessions(
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    liftoff_time := NEW.liftoff_time,
    sessions_id := NEW.sessions_id,
    session_name := NEW.session_name,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    line_count := NEW.line_count,
    session_notes := NEW.session_notes,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_sessions_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_epi_sessions(
    flights_id_in := NEW.flights_id,
    sessions_id_in := NEW.sessions_id,
    session_name_in := NEW.session_name,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    line_count_in := NEW.line_count,
    session_notes_in := NEW.session_notes
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_sessions(
    flight_date_in := NEW.flight_date,
    pilot_in := NEW.pilot,
    operator_in := NEW.operator,
    liftoff_time_in := NEW.liftoff_time,
    sessions_id_in := NEW.sessions_id,
    session_name_in := NEW.session_name,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    line_count_in := NEW.line_count,
    session_notes_in := NEW.session_notes
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_epi_sessions_id(flights_id_in UUID, session_name_in text, start_time_in TIME, end_time_in TIME, line_count_in FLOAT, session_notes_in TEXT) RETURNS UUID AS $$
DECLARE
  sid UUID;
BEGIN

  SELECT
    sessions_id INTO sid
  FROM
    sessions s
  WHERE
    flights_id = flights_id_in AND
    session_name = session_name_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions: flights_id="%" session_name="%"', flights_id_in, session_name_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_lightweight_sessions_id(flights_id_in UUID, session_name_in text) RETURNS UUID AS $$
DECLARE
  sid UUID;
BEGIN

  SELECT
    sessions_id INTO sid
  FROM
    sessions s
  WHERE
    flights_id = flights_id_in AND
    session_name = session_name_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions: flights_id="%" session_name="%"', flights_id_in, session_name_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_sessions_id(flight_date_in date, pilot_in text, operator_in text, liftoff_time_in time, session_name_in text, start_time_in time, end_time_in time, line_count_in float, session_notes_in text) RETURNS UUID AS $$
DECLARE
  sid UUID;
  fl_id UUID;
BEGIN
  SELECT get_flights_id(flight_date_in, pilot_in, operator_in, liftoff_time_in) INTO fl_id;
  SELECT
    sessions_id INTO sid
  FROM
    sessions s
  WHERE
    flights_id = fl_id AND
    session_name = session_name_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions: flight_date="%" pilot="%" operator="%" liftoff_time="%" session_name="%" start_time="%" end_time="%" line_count="%" session_notes="%"', flight_date, pilot_in, operator_in, liftoff_time_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER sessions_insert_trig
  INSTEAD OF INSERT ON
  sessions_view FOR EACH ROW
  EXECUTE PROCEDURE insert_sessions_from_trig();

CREATE TRIGGER sessions_insert_epi_trig
  INSTEAD OF INSERT ON
  sessions_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_sessions_from_trig();

CREATE TRIGGER sessions_update_trig
  INSTEAD OF UPDATE ON
  sessions_view FOR EACH ROW
  EXECUTE PROCEDURE update_sessions_from_trig();

CREATE TRIGGER sessions_update_epi_trig
  INSTEAD OF UPDATE ON
  sessions_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_sessions_from_trig();
