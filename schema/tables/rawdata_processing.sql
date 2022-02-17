-- TABLE
DROP TABLE IF EXISTS rawdata_processing CASCADE;
CREATE TABLE rawdata_processing (
  rawdata_processing_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  rawdata_metadata_id UUID REFERENCES rawdata_metadata NOT NULL,
  processing_events_id UUID REFERENCES processing_events NOT NULL
);
CREATE INDEX rawdata_processing_source_id_idx ON rawdata_processing(source_id);
CREATE INDEX rawdata_processing_rawdata_metadata_id_idx ON rawdata_processing(rawdata_metadata_id);
CREATE INDEX rawdata_processing_processing_events_id_idx ON rawdata_processing(processing_events_id);

-- VIEW
CREATE OR REPLACE VIEW rawdata_processing_view AS
  SELECT
    r.rawdata_processing_id AS rawdata_processing_id,
    f.flight_date AS flight_date,
    f.pilot AS pilot,
    f.operator AS operator,
    f.liftoff_time AS liftoff_time,
    s.session_name AS session_name,
    s.line_count AS line_count,
    fl.line_number AS line_number,
    rd.capture_time AS capture_time,
    rd.quality AS quality,
    rd.cold_storage AS cold_storage,
    rd.hot_storage AS hot_storage,
    rd.hot_storage_expiration AS hot_storage_expiration,
    pe.system AS system,
    pe.software_version AS software_version,
    pe.job_type AS job_type,
    pe.input_dir AS input_dir,
    pe.proc_params AS proc_params,

    sc.name AS source_name
  FROM
    rawdata_processing r
LEFT JOIN source sc ON r.source_id = sc.source_id
LEFT JOIN rawdata_metadata rd ON r.rawdata_metadata_id = rd.rawdata_metadata_id
LEFT JOIN flightlines fl ON fl.flightlines_id = rd.flightlines_id
LEFT JOIN sessions s ON s.sessions_id = fl.sessions_id
LEFT JOIN flights f ON s.flights_id = f.flights_id
LEFT JOIN processing_events pe ON r.processing_events_id = pe.processing_events_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rawdata_processing (
  rawdata_processing_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  liftoff_time TIME,
  session_name TEXT,
  line_count FLOAT,
  line_number FLOAT,
  capture_time TIME,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  system TEXT,
  software_version TEXT,
  job_type TEXT,
  input_dir TEXT,
  proc_params TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  rdid UUID;
  peid UUID;
BEGIN
  SELECT get_rawdata_metadata_id(flight_date, pilot, operator, liftoff_time, session_name, line_count, line_number, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration) INTO rdid;
  SELECT get_processing_events_id(system, software_version, job_type, input_dir, proc_params) INTO peid;

  IF( rawdata_processing_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_processing_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata_processing (
    rawdata_processing_id, rawdata_metadata_id, processing_events_id, source_id
  ) VALUES (
    rawdata_processing_id, rdid, peid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_rawdata_processing (
  rawdata_processing_id UUID,
  flightlines_id UUID,
  capture_time TIME,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  system TEXT,
  software_version TEXT,
  job_type TEXT,
  input_dir TEXT,
  proc_params TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  rdid UUID;
  peid UUID;
BEGIN
  SELECT get_epi_rawdata_metadata_id(flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration) INTO rdid;
  SELECT get_processing_events_id(system, software_version, job_type, input_dir, proc_params) INTO peid;

  IF( rawdata_processing_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_processing_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata_processing (
    rawdata_processing_id, rawdata_metadata_id, processing_events_id, source_id
  ) VALUES (
    rawdata_processing_id, rdid, peid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_lightweight_rawdata_processing (
  rawdata_processing_id UUID,
  flightlines_id UUID,
  capture_time TIME,
  system TEXT,
  software_version TEXT,
  job_type TEXT,
  input_dir TEXT,
  proc_params TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  rdid UUID;
  peid UUID;
BEGIN
  SELECT get_lightweight_rawdata_metadata_id(flightlines_id, capture_time) INTO rdid;
  SELECT get_processing_events_id(system, software_version, job_type, input_dir, proc_params) INTO peid;

  IF( rawdata_processing_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_processing_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata_processing (
    rawdata_processing_id, rawdata_metadata_id, processing_events_id, source_id
  ) VALUES (
    rawdata_processing_id, rdid, peid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_rawdata_processing (
  rawdata_processing_id_in UUID,
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  liftoff_time_in TIME,
  session_name_in TEXT,
  line_count_in FLOAT,
  line_number_in FLOAT,
  capture_time_in TIME,
  quality_in RAWDATA_QUALITY,
  cold_storage_in TEXT,
  hot_storage_in TEXT,
  hot_storage_expiration_in DATE,
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT) RETURNS void AS $$
DECLARE
rdid_in UUID;
peid_in UUID;

BEGIN
  SELECT get_rawdata_metadata_id(flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, line_count_in, line_number_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in) INTO rdid_in;
  SELECT get_processing_events_id(system_in, software_version_in, job_type_in, input_dir_in, proc_params_in) INTO peid_in;

  UPDATE rawdata_processing SET (
    rawdata_metadata_id, processing_events_id
  ) = (
    rdid_in, peid_in
  ) WHERE
    rawdata_processing_id = rawdata_processing_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_epi_rawdata_processing (
  rawdata_processing_id_in UUID,
  flightlines_id_in UUID,
  capture_time_in TIME,
  quality_in RAWDATA_QUALITY,
  cold_storage_in TEXT,
  hot_storage_in TEXT,
  hot_storage_expiration_in DATE,
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT) RETURNS void AS $$
DECLARE
rdid_in UUID;
peid_in UUID;

BEGIN
  SELECT get_epi_rawdata_metadata_id(flightlines_id_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in) INTO rdid_in;
  SELECT get_processing_events_id(system_in, software_version_in, job_type_in, input_dir_in, proc_params_in) INTO peid_in;

  UPDATE rawdata_processing SET (
    rawdata_metadata_id, processing_events_id
  ) = (
    rdid_in, peid_in
  ) WHERE
    rawdata_processing_id = rawdata_processing_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_lightweight_rawdata_processing (
  rawdata_processing_id_in UUID,
  flightlines_id_in UUID,
  capture_time_in TIME,
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT) RETURNS void AS $$
DECLARE
rdid_in UUID;
peid_in UUID;

BEGIN
  SELECT get_lightweight_rawdata_metadata_id(flightlines_id_in, capture_time_in) INTO rdid_in;
  SELECT get_processing_events_id(system_in, software_version_in, job_type_in, input_dir_in, proc_params_in) INTO peid_in;

  UPDATE rawdata_processing SET (
    rawdata_metadata_id, processing_events_id
  ) = (
    rdid_in, peid_in
  ) WHERE
    rawdata_processing_id = rawdata_processing_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_rawdata_processing_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_rawdata_processing(
    rawdata_processing_id := NEW.rawdata_processing_id,
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    liftoff_time := NEW.liftoff_time,
    session_name := NEW.session_name,
    line_count := NEW.line_count,
    line_number := NEW.line_number,
    capture_time := NEW.capture_time,
    quality := NEW.quality,
    cold_storage := NEW.cold_storage,
    hot_storage := NEW.hot_storage,
    hot_storage_expiration := NEW.hot_storage_expiration,
    system := NEW.system,
    software_version := NEW.software_version,
    job_type := NEW.job_type,
    input_dir := NEW.input_dir,
    proc_params := NEW.proc_params,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_rawdata_processing_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_rawdata_processing(
    rawdata_processing_id := NEW.rawdata_processing_id,
    flightlines_id := NEW.flightlines_id,
    capture_time := NEW.capture_time,
    quality := NEW.quality,
    cold_storage := NEW.cold_storage,
    hot_storage := NEW.hot_storage,
    hot_storage_expiration := NEW.hot_storage_expiration,
    system := NEW.system,
    software_version := NEW.software_version,
    job_type := NEW.job_type,
    input_dir := NEW.input_dir,
    proc_params := NEW.proc_params,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_rawdata_processing_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_rawdata_processing(
    rawdata_processing_id_in := NEW.rawdata_processing_id,
    flight_date_in := NEW.flight_date,
    pilot_in := NEW.pilot,
    operator_in := NEW.operator,
    liftoff_time_in := NEW.liftoff_time,
    session_name_in := NEW.session_name,
    line_count_in := NEW.line_count,
    line_number_in := NEW.line_number,
    capture_time_in := NEW.capture_time,
    quality_in := NEW.quality,
    cold_storage_in := NEW.cold_storage,
    hot_storage_in := NEW.hot_storage,
    hot_storage_expiration_in := NEW.hot_storage_expiration,
    system_in := NEW.system,
    software_version_in := NEW.software_version,
    job_type_in := NEW.job_type,
    input_dir_in := NEW.input_dir,
    proc_params_in := NEW.proc_params
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_rawdata_processing_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_epi_rawdata_processing(
    rawdata_processing_id_in := NEW.rawdata_processing_id,
    flightlines_id_in := NEW.flightlines_id,
    capture_time_in := NEW.capture_time,
    quality_in := NEW.quality,
    cold_storage_in := NEW.cold_storage,
    hot_storage_in := NEW.hot_storage,
    hot_storage_expiration_in := NEW.hot_storage_expiration,
    system_in := NEW.system,
    software_version_in := NEW.software_version,
    job_type_in := NEW.job_type,
    input_dir_in := NEW.input_dir,
    proc_params_in := NEW.proc_params
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_rawdata_processing_id(
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  liftoff_time TIME,
  session_name TEXT,
  line_count FLOAT,
  line_number FLOAT,
  capture_time TIME,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  system TEXT,
  software_version TEXT,
  job_type TEXT,
  input_dir TEXT,
  proc_params TEXT
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  rdid UUID;
  peid UUID;
BEGIN
  SELECT get_rawdata_metadata_id(flight_date, pilot, operator,liftoff_time, session_name, line_count,line_number, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration) INTO rdid;
  SELECT get_processing_events_id(system, software_version, job_type, input_dir, proc_params) INTO peid;
  SELECT
    rawdata_processing_id INTO rid
  FROM
    rawdata_processing r
  WHERE
    rawdata_metadata_id = rdid AND
    processing_events_id = peid;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_processing: quality="%" cold_storage="%" hot_storage="%" hot_storage_expiration="%" system="%" software_version="%" job_type="%" input_dir="%" proc_params="%"',
    quality, cold_storage, hot_storage, hot_storage_expiration, system, software_version, job_type, input_dir, proc_params;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_epi_rawdata_processing_id(
  flightlines_id UUID,
  capture_time TIME,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  system TEXT,
  software_version TEXT,
  job_type TEXT,
  input_dir TEXT,
  proc_params TEXT
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  rdid UUID;
  peid UUID;
BEGIN
  SELECT get_lightweight_rawdata_metadata_id(flightlines_id, capture_time) INTO rdid;
  SELECT get_processing_events_id(system, software_version, job_type, input_dir, proc_params) INTO peid;
  SELECT
    rawdata_processing_id INTO rid
  FROM
    rawdata_processing r
  WHERE
    rawdata_metadata_id = rdid AND
    processing_events_id = peid;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_processing: quality="%" cold_storage="%" hot_storage="%" hot_storage_expiration="%" system="%" software_version="%" job_type="%" input_dir="%" proc_params="%"',
    quality, cold_storage, hot_storage, hot_storage_expiration, system, software_version, job_type, input_dir, proc_params;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER rawdata_processing_insert_trig
  INSTEAD OF INSERT ON
  rawdata_processing_view FOR EACH ROW
  EXECUTE PROCEDURE insert_rawdata_processing_from_trig();

CREATE TRIGGER rawdata_processing_insert_epi_trig
  INSTEAD OF INSERT ON
  rawdata_processing_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_rawdata_processing_from_trig();

CREATE TRIGGER rawdata_processing_update_trig
  INSTEAD OF UPDATE ON
  rawdata_processing_view FOR EACH ROW
  EXECUTE PROCEDURE update_rawdata_processing_from_trig();

CREATE TRIGGER rawdata_processing_update_epi_trig
  INSTEAD OF UPDATE ON
  rawdata_processing_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_rawdata_processing_from_trig();
