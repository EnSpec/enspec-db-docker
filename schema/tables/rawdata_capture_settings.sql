-- TABLE
DROP TABLE IF EXISTS rawdata_capture_settings CASCADE;
CREATE TABLE rawdata_capture_settings (
  rawdata_capture_settings_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  instruments_id UUID REFERENCES instruments NOT NULL,
  rawdata_metadata_id UUID REFERENCES rawdata_metadata NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX rawdata_capture_settings_source_id_idx ON rawdata_capture_settings(source_id);
CREATE INDEX rawdata_capture_settings_instruments_id_idx ON rawdata_capture_settings(instruments_id);
CREATE INDEX rawdata_capture_settings_rawdata_metadata_id_idx ON rawdata_capture_settings(rawdata_metadata_id);
CREATE INDEX rawdata_capture_settings_variables_id_idx ON rawdata_capture_settings(variables_id);
CREATE INDEX rawdata_capture_settings_units_id_idx ON rawdata_capture_settings(units_id);

-- VIEW
CREATE OR REPLACE VIEW rawdata_capture_settings_view AS
  SELECT
    r.rawdata_capture_settings_id AS rawdata_capture_settings_id,
    f.flight_date AS flight_date,
    f.pilot AS pilot,
    f.operator AS operator,
    f.liftoff_time AS liftoff_time,
    s.session_name AS session_name,
    s.line_count AS line_count,
    fl.line_number AS line_number,
    i.make as make,
    i.model as model,
    rd.capture_time as capture_time,
    v.variable_name  as variable_name,
    u.units_name  as units_name,

    r.value as value,
    sc.name AS source_name
  FROM
    rawdata_capture_settings r
LEFT JOIN source sc ON r.source_id = sc.source_id
LEFT JOIN rawdata_metadata rd ON r.rawdata_metadata_id = rd.rawdata_metadata_id
LEFT JOIN flightlines fl ON fl.flightlines_id = rd.flightlines_id
LEFT JOIN sessions s ON s.sessions_id = fl.sessions_id
LEFT JOIN flights f ON s.flights_id = f.flights_id
LEFT JOIN instruments i ON r.instruments_id = i.instruments_id
LEFT JOIN variables v ON r.variables_id = v.variables_id
LEFT JOIN units u ON r.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rawdata_capture_settings (
  rawdata_capture_settings_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  liftoff_time TIME,
  session_name TEXT,
  line_count FLOAT,
  line_number FLOAT,
  capture_time TIME,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  variable_name TEXT,
  units_name TEXT,
  value FLOAT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  fid UUID;
  sid UUID;
  fl_id UUID;
  iid UUID;
  rdid UUID;
  vid UUID;
  uid UUID;
BEGIN

  IF( rawdata_capture_settings_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_capture_settings_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_flights_id(flight_date, pilot, operator, liftoff_time) INTO fid;
  SELECT get_epi_sessions_id(fid,session_name) INTO sid;
  SELECT get_epi_flightlines_id(sid, line_number) INTO fl_id;
  SELECT get_rawdata_metadata_id(fl_id, capture_time) INTO rdid;
  SELECT get_instruments_id(make, model) INTO iid;
  SELECT get_lightweight_variables_id(variable_name) INTO vid;
  SELECT get_lightweight_units_id(units_name) INTO uid;

  INSERT INTO rawdata_capture_settings (
    rawdata_capture_settings_id, instruments_id, rawdata_metadata_id, variables_id, units_id, value, source_id
  ) VALUES (
    rawdata_capture_settings_id, iid, rdid, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_rawdata_capture_settings (
  rawdata_capture_settings_id UUID,
  rawdata_metadata_id UUID,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  variable_name TEXT,
  units_name TEXT,
  value FLOAT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  iid UUID;
  vid UUID;
  uid UUID;
BEGIN

  IF( rawdata_capture_settings_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_capture_settings_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_instruments_id(make, model) INTO iid;
  SELECT get_lightweight_variables_id(variable_name) INTO vid;
  SELECT get_lightweight_units_id(units_name) INTO uid;

  INSERT INTO rawdata_capture_settings (
    rawdata_capture_settings_id, instruments_id, rawdata_metadata_id, variables_id, units_id, value, source_id
  ) VALUES (
    rawdata_capture_settings_id, iid, rawdata_metadata_id, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rawdata_capture_settings (
  rawdata_capture_settings_id_in UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  liftoff_time TIME,
  session_name TEXT,
  line_count FLOAT,
  line_number FLOAT,
  capture_time TIME,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  variable_name TEXT,
  units_name TEXT,
  units_description_in TEXT,
  value_in FLOAT) RETURNS void AS $$
DECLARE
fid UUID;
sid UUID;
fl_id UUID;
iid UUID;
rdid UUID;
vid UUID;
uid UUID;
BEGIN

  SELECT get_flights_id(flight_date, pilot, operator, liftoff_time) INTO fid;
  SELECT get_epi_sessions_id(fid,session_name) INTO sid;
  SELECT get_epi_flightlines_id(sid, line_number) INTO fl_id;
  SELECT get_rawdata_metadata_id(fl_id, capture_time) INTO rdid;
  SELECT get_instruments_id(make, model) INTO iid;
  SELECT get_lightweight_variables_id(variable_name) INTO vid;
  SELECT get_lightweight_units_id(units_name) INTO uid;

  UPDATE rawdata_capture_settings SET (
    instruments_id, rawdata_metadata_id, variables_id, units_id, value
  ) = (
    iid, rdid, vid, uid, value_in
  ) WHERE
    rawdata_capture_settings_id = rawdata_capture_settings_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_epi_rawdata_capture_settings (
  rawdata_capture_settings_id_in UUID,
  rawdata_metadata_id UUID,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  variable_name TEXT,
  units_name TEXT,
  units_description_in TEXT,
  value_in FLOAT) RETURNS void AS $$
DECLARE
iid UUID;
vid UUID;
uid UUID;
BEGIN
  SELECT get_instruments_id(make, model) INTO iid;
  SELECT get_lightweight_variables_id(variable_name) INTO vid;
  SELECT get_lightweight_units_id(units_name) INTO uid;

  UPDATE rawdata_capture_settings SET (
    instruments_id, rawdata_metadata_id, variables_id, units_id, value
  ) = (
    iid, rawdata_metadata_id, vid, uid, value_in
  ) WHERE
    rawdata_capture_settings_id = rawdata_capture_settings_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_rawdata_capture_settings_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_rawdata_capture_settings(
    rawdata_capture_settings_id := NEW.rawdata_capture_settings_id,
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    liftoff_time := NEW.liftoff_time,
    session_name := NEW.session_name,
    line_count := NEW.line_count,
    line_number := NEW.line_number,
    capture_time := NEW.capture_time,
    make := NEW.make,
    model := NEW.model,
    variable_name := NEW.variable_name,
    units_name := NEW.units_name,
    value := NEW.value,
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_rawdata_capture_settings_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_rawdata_capture_settings(
    rawdata_capture_settings_id := NEW.rawdata_capture_settings_id,
    rawdata_metadata_id := NEW.rawdata_metadata_id,
    make := NEW.make,
    model := NEW.model,
    variable_name := NEW.variable_name,
    units_name := NEW.units_name,
    value := NEW.value,
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_rawdata_capture_settings_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_rawdata_capture_settings(
    rawdata_capture_settings_id_in := NEW.rawdata_capture_settings_id,
    flight_date_in := NEW.flight_date,
    pilot_in := NEW.pilot,
    operator_in := NEW.operator,
    liftoff_time_in := NEW.liftoff_time,
    session_name_in := NEW.session_name,
    line_count_in := NEW.line_count,
    line_number_in := NEW.line_number,
    capture_time_in := NEW.capture_time,
    make_in := NEW.make,
    model_in := NEW.model,
    variable_name_in := NEW.variable_name,
    units_name_in := NEW.units_name,
    value_in := NEW.value
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_rawdata_capture_settings_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_epi_rawdata_capture_settings(
    rawdata_capture_settings_id_in := NEW.rawdata_capture_settings_id,
    rawdata_metadata_id_in := NEW.rawdata_metadata_id,
    make_in := NEW.make,
    model_in := NEW.model,
    variable_name_in := NEW.variable_name,
    units_name_in := NEW.units_name,
    value_in := NEW.value
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_rawdata_capture_settings_id(
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  liftoff_time_in TIME,
  session_name_in TEXT,
  line_count_in FLOAT,
  line_number_in FLOAT,
  capture_time_in TIME,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  variable_name_in TEXT,
  units_name_in TEXT,
  units_description_in TEXT,
  value_in FLOAT
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  fid UUID;
  sid UUID;
  fl_id UUID;
  iid UUID;
  rdid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_flights_id(flight_date_in, pilot_in, operator_in, liftoff_time_in) INTO fid;
  SELECT get_epi_sessions_id(fid,session_name_in) INTO sid;
  SELECT get_epi_flightlines_id(sid, line_number_in) INTO fl_id;
  SELECT get_rawdata_metadata_id(fl_id, capture_time_in) INTO rdid;
  SELECT get_instruments_id(make_in, model_in) INTO iid;
  SELECT get_lightweight_variables_id(variable_name_in) INTO vid;
  SELECT get_lightweight_units_id(units_name_in) INTO uid;
  SELECT
    rawdata_capture_settings_id INTO rid
  FROM
    rawdata_capture_settings r
  WHERE
    instruments_id = iid AND
    rawdata_metadata_id = rdid AND
    variables_id = vid AND
    units_id = uid AND
    value = value_in;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_capture_settings: flight_date="%" pilot="%" operator="%" liftoff_time="%"
    session_name="%" line_number="%" capture_time="%" make="%" model="%"
    variable_name="%" units_name="%" value="%"', flight_date_in, pilot_in, operator_in, liftoff_time_in,
    session_name_in, line_count_in, capture_time_in, make_in, model_in,
    variable_name_in, units_name_in, value_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_epi_rawdata_capture_settings_id(
  rawdata_metadata_id_in UUID,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  variable_name_in TEXT,
  units_name_in TEXT,
  value_in FLOAT
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  iid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_instruments_id(make_in, model_in) INTO iid;
  SELECT get_lightweight_variables_id(variable_name_in) INTO vid;
  SELECT get_lightweight_units_id(units_name_in) INTO uid;
  SELECT
    rawdata_capture_settings_id INTO rid
  FROM
    rawdata_capture_settings r
  WHERE
    rawdata_metadata_id = rawdata_metadata_id_in AND
    instruments_id = iid AND
    variables_id = vid AND
    units_id = uid AND
    value = value_in;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_capture_settings: rawdata_metadata_id="%" make="%" model="%"
    variable_name="%" units_name="%" value="%"', rawdata_metadata_id_in, make_in, model_in,
    variable_name_in, units_name_in, value_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;


-- RULES
CREATE TRIGGER rawdata_capture_settings_insert_trig
  INSTEAD OF INSERT ON
  rawdata_capture_settings_view FOR EACH ROW
  EXECUTE PROCEDURE insert_rawdata_capture_settings_from_trig();

CREATE TRIGGER rawdata_capture_settings_epi_insert_trig
  INSTEAD OF INSERT ON
  rawdata_capture_settings_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_rawdata_capture_settings_from_trig();

CREATE TRIGGER rawdata_capture_settings_update_trig
  INSTEAD OF UPDATE ON
  rawdata_capture_settings_view FOR EACH ROW
  EXECUTE PROCEDURE update_rawdata_capture_settings_from_trig();

CREATE TRIGGER rawdata_capture_settings_epi_update_trig
  INSTEAD OF UPDATE ON
  rawdata_capture_settings_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_rawdata_capture_settings_from_trig();
