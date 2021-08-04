-- TABLE
DROP TABLE IF EXISTS rawdata_metadata CASCADE;
CREATE TABLE rawdata_metadata (
  rawdata_metadata_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  sessions_id UUID REFERENCES sessions NOT NULL,
  instruments_id UUID REFERENCES instruments NOT NULL,
  rawdata_id UUID REFERENCES rawdata NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX rawdata_metadata_source_id_idx ON rawdata_metadata(source_id);
CREATE INDEX rawdata_metadata_sessions_id_idx ON rawdata_metadata(sessions_id);
CREATE INDEX rawdata_metadata_instruments_id_idx ON rawdata_metadata(instruments_id);
CREATE INDEX rawdata_metadata_rawdata_id_idx ON rawdata_metadata(rawdata_id);
CREATE INDEX rawdata_metadata_variables_id_idx ON rawdata_metadata(variables_id);
CREATE INDEX rawdata_metadata_units_id_idx ON rawdata_metadata(units_id);

-- VIEW
CREATE OR REPLACE VIEW rawdata_metadata_view AS
  SELECT
    r.rawdata_metadata_id AS rawdata_metadata_id,
    s.session_name  as session_name,
    s.start_time  as start_time,
    s.end_time  as end_time,
    s.line_count  as line_count,
    s.bad_lines  as bad_lines,
    i.make as make,
    i.model as model,
    i.serial_number as serial_number,
    i.type as type,
    rd.line_id  as line_id,
    rd.line_no  as line_no,
    rd.quality  as line_quality,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,

    r.value as value,
    sc.name AS source_name
  FROM
    rawdata_metadata r
LEFT JOIN source sc ON r.source_id = sc.source_id
LEFT JOIN sessions s ON r.sessions_id = s.sessions_id
LEFT JOIN instruments i ON r.instruments_id = i.instruments_id
LEFT JOIN rawdata rd ON r.rawdata_id = rd.rawdata_id
LEFT JOIN variables v ON r.variables_id = v.variables_id
LEFT JOIN units u ON r.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rawdata_metadata (
  rawdata_metadata_id UUID,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  bad_lines TEXT,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  serial_number TEXT,
  type INSTRUMENT_TYPE,
  line_id FLOAT,
  line_no FLOAT,
  line_quality RAWDATA_QUALITY,
  variable_name TEXT,
  variable_type TEXT,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,

  value FLOAT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  sid UUID;
  iid UUID;
  rdid UUID;
  vid UUID;
  uid UUID;
BEGIN

  IF( rawdata_metadata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_metadata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_sessions_id(session_name, start_time, end_time, line_count, bad_lines) INTO sid;
  SELECT get_instruments_id(make, model, serial_number, type) INTO iid;
  SELECT get_rawdata_id(line_id, line_no, line_quality) INTO rdid;
  SELECT get_variables_id(variable_name, variable_type) INTO vid;
  SELECT get_units_id(units_name, units_type, units_description) INTO uid;

  INSERT INTO rawdata_metadata (
    rawdata_metadata_id, sessions_id, instruments_id, rawdata_id, variables_id, units_id, value, source_id
  ) VALUES (
    rawdata_metadata_id, sid, iid, rdid, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rawdata_metadata (
  rawdata_metadata_id_in UUID,
  session_name_in TEXT,
  start_time_in TIME,
  end_time_in TIME,
  line_count_in FLOAT,
  bad_lines_in TEXT,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE,
  line_id_in FLOAT,
  line_no_in FLOAT,
  line_quality_in RAWDATA_QUALITY,
  variable_name_in TEXT,
  variable_type_in TEXT,
  units_name_in TEXT,
  units_type_in TEXT,
  units_description_in TEXT,
  value_in FLOAT) RETURNS void AS $$
DECLARE
sid UUID;
iid UUID;
rdid UUID;
vid UUID;
uid UUID;

BEGIN
  SELECT get_sessions_id(session_name_in, start_time_in, end_time_in, line_count_in, bad_lines_in) INTO sid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;
  SELECT get_rawdata_id(line_id_in, line_no_in, line_quality_in) INTO rdid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  UPDATE rawdata_metadata SET (
    sessions_id, instruments_id, rawdata_id, variables_id, units_id, value
  ) = (
    sid, iid, rdid, vid, uid, value_in
  ) WHERE
    rawdata_metadata_id = rawdata_metadata_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_rawdata_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_rawdata_metadata(
    rawdata_metadata_id := NEW.rawdata_metadata_id,
    session_name := NEW.session_name,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    line_count := NEW.line_count,
    bad_lines := NEW.bad_lines,
    make := NEW.make,
    model := NEW.model,
    serial_number := NEW.serial_number,
    type := NEW.type,
    line_id := NEW.line_id,
    line_no := NEW.line_no,
    quality := NEW.quality,
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

CREATE OR REPLACE FUNCTION update_rawdata_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_rawdata_metadata(
    rawdata_metadata_id_in := NEW.rawdata_metadata_id,
    session_name_in := NEW.session_name,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    line_count_in := NEW.line_count,
    bad_lines_in := NEW.bad_lines,
    make_in := NEW.make,
    model_in := NEW.model,
    serial_number_in := NEW.serial_number,
    type_in := NEW.type,
    line_id_in := NEW.line_id,
    line_no_in := NEW.line_no,
    quality_in := NEW.quality,
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
CREATE OR REPLACE FUNCTION get_rawdata_metadata_id(session_name_in text, start_time_in time, end_time_in time, line_count_in float, bad_lines_in text,
  make_in INSTRUMENT_MAKE, model_in INSTRUMENT_MODEL, serial_number_in TEXT, type_in INSTRUMENT_TYPE,
  line_id_in float, line_no_in float, quality_in RAWDATA_QUALITY,
  variable_name_in text, variable_type_in text, units_name_in text, units_type_in text, units_description_in text, value_in float) RETURNS UUID AS $$
DECLARE
  rid UUID;
  sid UUID;
  iid UUID;
  rdid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_sessions_id(session_name_in, start_time_in, end_time_in, line_count_in, bad_lines_in) INTO sid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;
  SELECT get_rawdata_id(line_id_in, line_no_in, line_quality_in) INTO rdid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  SELECT
    rawdata_metadata_id INTO rid
  FROM
    rawdata_metadata r
  WHERE
    sessions_id = sid AND
    instruments_id = iid AND
    rawdata_id = rdid AND
    variables_id = vid AND
    units_id = uid AND
    value = value_in;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_metadata: session_name="%" start_time="%" end_time="%" line_count="%" bad_lines="%"
    serial_number="%" line_id="%" line_no="%" quality="%" variable_name="%" variable_type="%" units_name="%"
    units_type="%" units_description="%" value="%"', session_name_in, start_time_in, end_time_in, line_count_in,
    bad_lines_in, serial_number_in, line_id_in, line_no_in, quality_in, variable_name_in, variable_type_in,
    units_name_in, units_type_in, units_description_in, value_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER rawdata_metadata_insert_trig
  INSTEAD OF INSERT ON
  rawdata_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE insert_rawdata_metadata_from_trig();

CREATE TRIGGER rawdata_metadata_update_trig
  INSTEAD OF UPDATE ON
  rawdata_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE update_rawdata_metadata_from_trig();
