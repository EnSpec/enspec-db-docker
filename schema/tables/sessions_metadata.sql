-- TABLE
DROP TABLE IF EXISTS sessions_metadata CASCADE;
CREATE TABLE sessions_metadata (
  sessions_metadata_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  sessions_id UUID REFERENCES sessions NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX sessions_metadata_source_id_idx ON sessions_metadata(source_id);
CREATE INDEX sessions_metadata_sessions_id_idx ON sessions_metadata(sessions_id);
CREATE INDEX sessions_metadata_variables_id_idx ON sessions_metadata(variables_id);
CREATE INDEX sessions_metadata_units_id_idx ON sessions_metadata(units_id);

-- VIEW
CREATE OR REPLACE VIEW sessions_metadata_view AS
  SELECT
    s.sessions_metadata_id AS sessions_metadata_id,
    f.flight_date  as flight_date,
    f.pilot  as pilot,
    f.operator  as operator,
    f.liftoff_time  as liftoff_time,
    sess.session_name as session_name,
    sess.start_time as start_time,
    sess.end_time as end_time,
    sess.line_count as line_count,
    sess.session_notes as session_notes,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,

    s.value  as value,
    sc.name AS source_name
  FROM
    sessions_metadata s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN sessions sess ON s.sessions_id = sess.sessions_id
LEFT JOIN flights f ON sess.flights_id = f.flights_id
LEFT JOIN variables v ON s.variables_id = v.variables_id
LEFT JOIN units u ON s.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_sessions_metadata (
  sessions_metadata_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  liftoff_time TIME,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  session_notes TEXT,
  variable_name TEXT,
  variable_type TEXT,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,

  value FLOAT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  sess_id UUID;
  v_id UUID;
  u_id UUID;
BEGIN

  IF( sessions_metadata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_metadata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_sessions_id(flight_date, pilot, operator, liftoff_time, session_name, start_time, end_time, line_count, session_notes) INTO sess_id;
  SELECT get_variables_id(variable_name, variable_type) INTO v_id;
  SELECT get_units_id(units_name, units_type, units_description) INTO u_id;

  INSERT INTO sessions_metadata (
    sessions_metadata_id, sessions_id, variables_id, units_id, value, source_id
  ) VALUES (
    sessions_metadata_id, sess_id, v_id, u_id, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_sessions_metadata (
  sessions_metadata_id UUID,
  flights_id UUID,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  session_notes TEXT,
  variable_name TEXT,
  variable_type TEXT,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,

  value FLOAT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  sess_id UUID;
  v_id UUID;
  u_id UUID;
BEGIN

  IF( sessions_metadata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_metadata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_epi_sessions_id(flights_id, session_name, start_time, end_time, line_count, session_notes) INTO sess_id;
  SELECT get_variables_id(variable_name, variable_type) INTO v_id;
  SELECT get_units_id(units_name, units_type, units_description) INTO u_id;

  INSERT INTO sessions_metadata (
    sessions_metadata_id, sessions_id, variables_id, units_id, value, source_id
  ) VALUES (
    sessions_metadata_id, sess_id, v_id, u_id, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions_metadata (
  sessions_metadata_id_in UUID,
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  liftoff_time_in TIME,
  session_name_in TEXT,
  start_time_in TIME,
  end_time_in TIME,
  line_count_in FLOAT,
  session_notes_in TEXT,
  variable_name_in TEXT,
  variable_type_in TEXT,
  units_name_in TEXT,
  units_type_in TEXT,
  units_description_in TEXT,
  value_in FLOAT) RETURNS void AS $$
DECLARE
  sess_id UUID;
  v_id UUID;
  u_id UUID;

BEGIN
  SELECT get_sessions_id(flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in) INTO sess_id;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO v_id;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO u_id;

  UPDATE sessions_metadata SET (
    sessions_id, variables_id, units_id, value
  ) = (
    sess_id, v_id, u_id, value_in
  ) WHERE
    sessions_metadata_id = sessions_metadata_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_sessions_metadata (
  sessions_metadata_id_in UUID,
  flights_id_in UUID,
  session_name_in TEXT,
  start_time_in TIME,
  end_time_in TIME,
  line_count_in FLOAT,
  session_notes_in TEXT,
  variable_name_in TEXT,
  variable_type_in TEXT,
  units_name_in TEXT,
  units_type_in TEXT,
  units_description_in TEXT,
  value_in FLOAT) RETURNS void AS $$
DECLARE
  sess_id UUID;
  v_id UUID;
  u_id UUID;

BEGIN
  SELECT get_epi_sessions_id(flights_id_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in) INTO sess_id;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO v_id;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO u_id;

  UPDATE sessions_metadata SET (
    sessions_id, variables_id, units_id, value
  ) = (
    sess_id, v_id, u_id, value_in
  ) WHERE
    sessions_metadata_id = sessions_metadata_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_sessions_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_sessions_metadata(
    sessions_metadata_id := NEW.sessions_metadata_id,
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    liftoff_time := NEW.liftoff_time,
    session_name := NEW.session_name,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    line_count := NEW.line_count,
    session_notes := NEW.session_notes,
    variable_name := NEW.variable_name,
    variable_type := NEW.variable_type,
    units_name := NEW.units_name,
    units_type := NEW.units_type,
    units_description := NEW.units_description,
    value := NEW.value,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_sessions_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_sessions_metadata(
    sessions_metadata_id := NEW.sessions_metadata_id,
    flights_id := NEW.flights_id,
    session_name := NEW.session_name,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    line_count := NEW.line_count,
    session_notes := NEW.session_notes,
    variable_name := NEW.variable_name,
    variable_type := NEW.variable_type,
    units_name := NEW.units_name,
    units_type := NEW.units_type,
    units_description := NEW.units_description,
    value := NEW.value,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_sessions_metadata(
    sessions_metadata_id_in := NEW.sessions_metadata_id,
    flight_date_in := NEW.flight_date,
    pilot_in := NEW.pilot,
    operator_in := NEW.operator,
    liftoff_time_in := NEW.liftoff_time,
    session_name_in := NEW.session_name,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    line_count_in := NEW.line_count,
    session_notes_in := NEW.session_notes,
    variable_name_in := NEW.variable_name,
    variable_type_in := NEW.variable_type,
    units_name_in := NEW.units_name,
    units_type_in := NEW.units_type,
    units_description_in := NEW.units_description,
    value_in := NEW.value
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_sessions_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_epi_sessions_metadata(
    sessions_metadata_id_in := NEW.sessions_metadata_id,
    flights_id_in := NEW.flights_id,
    session_name_in := NEW.session_name,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    line_count_in := NEW.line_count,
    session_notes_in := NEW.session_notes,
    variable_name_in := NEW.variable_name,
    variable_type_in := NEW.variable_type,
    units_name_in := NEW.units_name,
    units_type_in := NEW.units_type,
    units_description_in := NEW.units_description,
    value_in := NEW.value
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_sessions_metadata_id(
  flight_date_in date, pilot_in text, operator_in text, liftoff_time_in time,
  session_name_in text, start_time_in time, end_time_in time, line_count_in float, session_notes_in text,
  variable_name_in text, variable_type_in text, units_name_in text, units_type_in text, units_description_in text, value_in float) RETURNS UUID AS $$
DECLARE
  sid UUID;
  v_id UUID;
  u_id UUID;
  sess_id UUID;
BEGIN
  SELECT get_sessions_id(flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in) INTO sess_id;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO v_id;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO u_id;
  SELECT
    sessions_metadata_id INTO sid
  FROM
    sessions_metadata s
  WHERE
    sessions_id = sess_id AND
    variables_id = v_id AND
    units_id = u_id AND
    value = value_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions_metadata: session_name="%" start_time="%" end_time="%" line_count="%" session_notes="%"
    variable_name="%" variable_type="%" units_name="%" units_type="%" units_description="%" value="%"', session_name_in,
    start_time_in, end_time_in, line_count_in, session_notes_in, variable_name_in, variable_type_in, units_name_in,
    units_type_in, units_description_in, value_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_epi_sessions_metadata_id(
  flights_id_in UUID,
  session_name_in text, start_time_in time, end_time_in time, line_count_in float, session_notes_in text,
  variable_name_in text, variable_type_in text, units_name_in text, units_type_in text, units_description_in text, value_in float) RETURNS UUID AS $$
DECLARE
  sid UUID;
  v_id UUID;
  u_id UUID;
  sess_id UUID;
BEGIN
  SELECT get_epi_sessions_id(flights_id_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in) INTO sess_id;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO v_id;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO u_id;
  SELECT
    sessions_metadata_id INTO sid
  FROM
    sessions_metadata s
  WHERE
    sessions_id = sess_id AND
    variables_id = v_id AND
    units_id = u_id AND
    value = value_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions_metadata: session_name="%" start_time="%" end_time="%" line_count="%" session_notes="%"
    variable_name="%" variable_type="%" units_name="%" units_type="%" units_description="%" value="%"', session_name_in,
    start_time_in, end_time_in, line_count_in, session_notes_in, variable_name_in, variable_type_in, units_name_in,
    units_type_in, units_description_in, value_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;


-- RULES
CREATE TRIGGER sessions_metadata_insert_trig
  INSTEAD OF INSERT ON
  sessions_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE insert_sessions_metadata_from_trig();

CREATE TRIGGER sessions_metadata_update_trig
  INSTEAD OF UPDATE ON
  sessions_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE update_sessions_metadata_from_trig();

CREATE TRIGGER sessions_metadata_insert_epi_trig
  INSTEAD OF INSERT ON
  sessions_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_sessions_metadata_from_trig();

CREATE TRIGGER sessions_metadata_update_epi_trig
  INSTEAD OF UPDATE ON
  sessions_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_sessions_metadata_from_trig();
