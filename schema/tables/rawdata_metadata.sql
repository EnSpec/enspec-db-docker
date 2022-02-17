-- TABLE
DROP TABLE IF EXISTS rawdata_metadata CASCADE;
CREATE TABLE rawdata_metadata (
  rawdata_metadata_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  flightlines_id UUID REFERENCES flightlines NOT NULL,
  capture_time TIME NOT NULL,
  quality RAWDATA_QUALITY NOT NULL,
  cold_storage TEXT,
  hot_storage TEXT NOT NULL,
  hot_storage_expiration DATE
);
CREATE INDEX rawdata_metadata_source_id_idx ON rawdata_metadata(source_id);

ALTER TABLE rawdata_metadata ADD CONSTRAINT uniq_rd_row UNIQUE(flightlines_id, capture_time);

-- VIEW
CREATE OR REPLACE VIEW rawdata_metadata_view AS
  SELECT
    r.rawdata_metadata_id AS rawdata_metadata_id,
    f.flight_date AS flight_date,
    f.pilot AS pilot,
    f.operator AS operator,
    f.liftoff_time AS liftoff_time,
    s.session_name AS session_name,
    s.line_count AS line_count,
    fl.line_number AS line_number,
    r.capture_time AS capture_time,
    r.quality  as quality,
    r.cold_storage as cold_storage,
    r.hot_storage as hot_storage,
    r.hot_storage_expiration as hot_storage_expiration,
    sc.name AS source_name
  FROM
    rawdata_metadata r
LEFT JOIN flightlines fl ON fl.flightlines_id = r.flightlines_id
LEFT JOIN sessions s ON s.sessions_id = fl.sessions_id
LEFT JOIN flights f ON s.flights_id = f.flights_id
LEFT JOIN source sc ON r.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rawdata_metadata (
  rawdata_metadata_id UUID,
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
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  fl_id UUID;
  sid UUID;
  fid UUID;
BEGIN
  IF( rawdata_metadata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_metadata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_flights_id(flight_date, pilot, operator, liftoff_time) INTO fid;
  SELECT get_lightweight_sessions_id(fid,session_name) INTO sid;
  SELECT get_epi_flightlines_id(sid, line_number) INTO fl_id;

  INSERT INTO rawdata_metadata (
    rawdata_metadata_id, flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration, source_id
  ) VALUES (
    rawdata_metadata_id, fl_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_rawdata_metadata (
  rawdata_metadata_id UUID,
  flightlines_id UUID,
  capture_time TIME,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( rawdata_metadata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_metadata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata_metadata (
    rawdata_metadata_id, flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration, source_id
  ) VALUES (
    rawdata_metadata_id, flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_rawdata_metadata (
  rawdata_metadata_id_in UUID,
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
  hot_storage_expiration_in DATE) RETURNS void AS $$
DECLARE
fid UUID;
sid UUID;
fl_id UUID;
BEGIN
  SELECT get_flights_id(flight_date_in, pilot_in, operator_in, liftoff_time_in) INTO fid;
  SELECT get_lightweight_sessions_id(fid,session_name_in) INTO sid;
  SELECT get_lightweight_flightlines_id(sid, line_number_in) INTO fl_id;

  UPDATE rawdata_metadata SET (
    flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration
  ) = (
    fl_id, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in
  ) WHERE
    rawdata_metadata_id = rawdata_metadata_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_rawdata_metadata (
  rawdata_metadata_id_in UUID,
  flightlines_id_in UUID,
  capture_time_in TIME,
  quality_in RAWDATA_QUALITY,
  cold_storage_in TEXT,
  hot_storage_in TEXT,
  hot_storage_expiration_in DATE) RETURNS void AS $$
BEGIN

  UPDATE rawdata_metadata SET (
    flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration
  ) = (
    flightlines_id_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in
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
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_rawdata_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_rawdata_metadata(
    rawdata_metadata_id := NEW.rawdata_metadata_id,
    flightlines_id := NEW.flightlines_id,
    capture_time := NEW.capture_time,
    quality := NEW.quality,
    cold_storage := NEW.cold_storage,
    hot_storage := NEW.hot_storage,
    hot_storage_expiration := NEW.hot_storage_expiration,
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
    hot_storage_expiration_in := NEW.hot_storage_expiration
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_rawdata_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_epi_rawdata_metadata(
    rawdata_metadata_id_in := NEW.rawdata_metadata_id,
    flightlines_id_in := NEW.flightlines_id,
    capture_time_in := NEW.capture_time,
    quality_in := NEW.quality,
    cold_storage_in := NEW.cold_storage,
    hot_storage_in := NEW.hot_storage,
    hot_storage_expiration_in := NEW.hot_storage_expiration
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_epi_rawdata_metadata_id(flightlines_id_in UUID, capture_time_in TIME, quality_in RAWDATA_QUALITY, cold_storage_in TEXT, hot_storage_in TEXT, hot_storage_expiration_in DATE) RETURNS UUID AS $$
DECLARE
  rid UUID;
BEGIN
  SELECT
    rawdata_metadata_id INTO rid
  FROM
    rawdata_metadata r
  WHERE
      flightlines_id = flightlines_id_in AND
      capture_time = capture_time_in;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_metadata: flightlines_id="%" capture_time="%" quality="%" cold_storage="%" hot_storage="%" hot_storage_expiration="%"', flightlines_id_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_lightweight_rawdata_metadata_id(flightlines_id UUID, capture_time_in TIME) RETURNS UUID AS $$
DECLARE
  rid UUID;
BEGIN
  SELECT
    rawdata_metadata_id INTO rid
  FROM
    rawdata_metadata r
  WHERE
    flightlines_id = flightlines_id_in AND
    capture_time = capture_time_in;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_metadata: flightlines_id="%" capture_time="%"', flightlines_id_in, capture_time_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_rawdata_metadata_id(flight_date_in DATE,
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
hot_storage_expiration_in DATE) RETURNS UUID AS $$
DECLARE
  rid UUID;
  fid UUID;
  sid UUID;
  fl_id UUID;
BEGIN
  SELECT get_flights_id(flight_date_in, pilot_in, operator_in, liftoff_time_in) INTO fid;
  SELECT get_lightweight_sessions_id(fid,session_name_in) INTO sid;
  SELECT get_epi_flightlines_id(sid, line_number_in) INTO fl_id;
  SELECT
    rawdata_metadata_id INTO rid
  FROM
    rawdata_metadata r
  WHERE
    flightlines_id = fl_id AND
    capture_time = capture_time_in;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_metadata: flight_date="%" pilot="%" operator="%" liftoff_time="%" session_name="%" line_count="%" line_number="%" capture_time="%"', flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, line_count_in, line_number_in, capture_time_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER rawdata_metadata_insert_trig
  INSTEAD OF INSERT ON
  rawdata_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE insert_rawdata_metadata_from_trig();

CREATE TRIGGER rawdata_metadata_epi_insert_trig
  INSTEAD OF INSERT ON
  rawdata_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_rawdata_metadata_from_trig();

CREATE TRIGGER rawdata_metadata_update_trig
  INSTEAD OF UPDATE ON
  rawdata_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE update_rawdata_metadata_from_trig();

CREATE TRIGGER rawdata_metadata_epi_update_trig
  INSTEAD OF UPDATE ON
  rawdata_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_rawdata_metadata_from_trig();
