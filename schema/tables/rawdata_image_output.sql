-- TABLE
DROP TABLE IF EXISTS rawdata_image_output CASCADE;
CREATE TABLE rawdata_image_output (
  rawdata_image_output_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  rawdata_metadata_id UUID REFERENCES rawdata_metadata NOT NULL,
  image_output_id UUID REFERENCES image_output NOT NULL
);
CREATE INDEX rawdata_image_output_source_id_idx ON rawdata_image_output(source_id);
CREATE INDEX rawdata_image_output_rawdata_metadata_id_idx ON rawdata_image_output(rawdata_metadata_id);
CREATE INDEX rawdata_image_output_image_output_id_idx ON rawdata_image_output(image_output_id);

-- VIEW
CREATE OR REPLACE VIEW rawdata_image_output_view AS
  SELECT
    r.rawdata_image_output_id AS rawdata_image_output_id,
    f.flight_date AS flight_date,
    f.pilot AS pilot,
    f.operator AS operator,
    f.liftoff_time AS liftoff_time,
    s.session_name AS session_name,
    s.line_count AS line_count,
    fl.line_number AS line_number,
    rd.capture_time AS capture_time,
    rd.quality AS rawdata_quality,
    rd.cold_storage AS rd_cold_storage,
    rd.hot_storage AS rd_hot_storage,
    rd.hot_storage_expiration AS rd_hot_storage_expiration,
    io.image_dir AS image_dir,
    io.image_dir_owner AS image_dir_owner,
    io.image_exists AS image_exists,
    io.processing_date AS processing_date,
    io.expiration_date AS expiration_date,
    io.expiration_date AS expiration_type,

    sc.name AS source_name
  FROM
    rawdata_image_output r
LEFT JOIN source sc ON r.source_id = sc.source_id
LEFT JOIN rawdata_metadata rd ON r.rawdata_metadata_id = rd.rawdata_metadata_id
LEFT JOIN flightlines fl ON fl.flightlines_id = rd.flightlines_id
LEFT JOIN sessions s ON s.sessions_id = fl.sessions_id
LEFT JOIN flights f ON s.flights_id = f.flights_id
LEFT JOIN image_output io ON r.image_output_id = io.image_output_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rawdata_image_output (
  rawdata_image_output_id UUID,
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
  image_dir TEXT,
  image_dir_owner TEXT,
  image_exists BOOL,
  processing_date DATE,
  expiration_date DATE,
  expiration_type IMAGE_OUTPUT_EXPIRATION_TYPE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  rdid UUID;
  ioid UUID;
BEGIN
  SELECT get_rawdata_metadata_id(flight_date, pilot, operator, liftoff_time, session_name, line_count, line_number, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration) INTO rdid;
  SELECT get_image_output_id(image_dir, image_dir_owner, image_exists, processing_date, expiration_date, expiration_type) INTO ioid;

  IF( rawdata_image_output_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_image_output_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata_image_output (
    rawdata_image_output_id, rawdata_metadata_id, image_output_id, source_id
  ) VALUES (
    rawdata_image_output_id, rdid, ioid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_rawdata_image_output (
  rawdata_image_output_id UUID,
  flightlines_id UUID,
  capture_time TIME,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  image_dir TEXT,
  image_dir_owner TEXT,
  image_exists BOOL,
  processing_date DATE,
  expiration_date DATE,
  expiration_type IMAGE_OUTPUT_EXPIRATION_TYPE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  rdid UUID;
  ioid UUID;
BEGIN
  SELECT get_epi_rawdata_metadata_id(flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration) INTO rdid;
  SELECT get_image_output_id(image_dir, image_dir_owner, image_exists, processing_date, expiration_date, expiration_type) INTO ioid;

  IF( rawdata_image_output_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_image_output_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata_image_output (
    rawdata_image_output_id, rawdata_metadata_id, image_output_id, source_id
  ) VALUES (
    rawdata_image_output_id, rdid, ioid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rawdata_image_output (
  rawdata_image_output_id_in UUID,
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
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE) RETURNS void AS $$
DECLARE
rdid UUID;
ioid UUID;

BEGIN
  SELECT get_rawdata_metadata_id(flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, line_count_in, line_number_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in) INTO rdid;
  SELECT get_image_output_id(image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in) INTO ioid;

  UPDATE rawdata_image_output SET (
    rawdata_metadata_id, image_output_id
  ) = (
    rdid, ioid
  ) WHERE
    rawdata_image_output_id = rawdata_image_output_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_rawdata_image_output (
  rawdata_image_output_id_in UUID,
  flightlines_id_in UUID,
  capture_time_in TIME,
  quality_in RAWDATA_QUALITY,
  cold_storage_in TEXT,
  hot_storage_in TEXT,
  hot_storage_expiration_in DATE,
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE) RETURNS void AS $$
DECLARE
rdid UUID;
ioid UUID;

BEGIN
  SELECT get_epi_rawdata_metadata_id(flightlines_id_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in) INTO rdid;
  SELECT get_image_output_id(image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in) INTO ioid;

  UPDATE rawdata_image_output SET (
    rawdata_metadata_id, image_output_id
  ) = (
    rdid, ioid
  ) WHERE
    rawdata_image_output_id = rawdata_image_output_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_rawdata_image_output_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_rawdata_image_output(
    rawdata_image_output_id := NEW.rawdata_image_output_id,
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
    source_name := NEW.source_name,
    image_dir := NEW.image_dir,
    image_dir_owner := NEW.image_dir_owner,
    image_exists := NEW.image_exists,
    processing_date := NEW.processing_date,
    expiration_date := NEW.expiration_date,
    expiration_type := NEW.expiration_type,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_rawdata_image_output_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_rawdata_image_output(
    rawdata_image_output_id := NEW.rawdata_image_output_id,
    flightlines_id := NEW.flightlines_id,
    capture_time := NEW.capture_time,
    quality := NEW.quality,
    cold_storage := NEW.cold_storage,
    hot_storage := NEW.hot_storage,
    hot_storage_expiration := NEW.hot_storage_expiration,
    source_name := NEW.source_name,
    image_dir := NEW.image_dir,
    image_dir_owner := NEW.image_dir_owner,
    image_exists := NEW.image_exists,
    processing_date := NEW.processing_date,
    expiration_date := NEW.expiration_date,
    expiration_type := NEW.expiration_type,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rawdata_image_output_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_rawdata_image_output(
    rawdata_image_output_id_in := NEW.rawdata_image_output_id,
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
    image_dir_in := NEW.image_dir,
    image_dir_owner_in := NEW.image_dir_owner,
    image_exists_in := NEW.image_exists,
    processing_date_in := NEW.processing_date,
    expiration_date_in := NEW.expiration_date,
    expiration_type_in := NEW.expiration_type
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_rawdata_image_output_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_epi_rawdata_image_output(
    rawdata_image_output_id_in := NEW.rawdata_image_output_id,
    flightlines_id_in := NEW.flightlines_id,
    capture_time_in := NEW.capture_time,
    quality_in := NEW.quality,
    cold_storage_in := NEW.cold_storage,
    hot_storage_in := NEW.hot_storage,
    hot_storage_expiration_in := NEW.hot_storage_expiration,
    image_dir_in := NEW.image_dir,
    image_dir_owner_in := NEW.image_dir_owner,
    image_exists_in := NEW.image_exists,
    processing_date_in := NEW.processing_date,
    expiration_date_in := NEW.expiration_date,
    expiration_type_in := NEW.expiration_type
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_rawdata_image_output_id(
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
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  rdid UUID;
  ioid UUID;
BEGIN
  SELECT get_rawdata_metadata_id(quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in) INTO rdid;
  SELECT get_image_output_id(image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in) INTO ioid;

  SELECT
    rawdata_image_output_id INTO rid
  FROM
    rawdata_image_output r
  WHERE
    rawdata_metadata_id = rdid AND
    image_output_id = ioid;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_image_output: rawdata_quality="%" rd_cold_storage="%" rd_hot_storage="%" rd_hot_storage_expiration="%" image_dir="%"
    image_dir_owner="%" image_exists="%" processing_date="%" expiration_date="%" expiration_type="%"',
    quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in, image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in,
    expiration_date_in, expiration_type_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_epi_rawdata_image_output_id(
  flightlines_id_in UUID,
  capture_time_in TIME,
  quality_in RAWDATA_QUALITY,
  cold_storage_in TEXT,
  hot_storage_in TEXT,
  hot_storage_expiration_in DATE,
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  rdid UUID;
  ioid UUID;
BEGIN
  SELECT get_lightweight_rawdata_metadata_id(flightlines_id_in, capture_time_in)  INTO rdid;
  SELECT get_image_output_id(image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in) INTO ioid;

  SELECT
    rawdata_image_output_id INTO rid
  FROM
    rawdata_image_output r
  WHERE
    rawdata_metadata_id = rdid AND
    image_output_id = ioid;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_image_output: rawdata_quality="%" rd_cold_storage="%" rd_hot_storage="%" rd_hot_storage_expiration="%" image_dir="%"
    image_dir_owner="%" image_exists="%" processing_date="%" expiration_date="%" expiration_type="%"',
    quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in, image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in,
    expiration_date_in, expiration_type_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER rawdata_image_output_insert_trig
  INSTEAD OF INSERT ON
  rawdata_image_output_view FOR EACH ROW
  EXECUTE PROCEDURE insert_rawdata_image_output_from_trig();

CREATE TRIGGER rawdata_image_output_insert_epi_trig
  INSTEAD OF INSERT ON
  rawdata_image_output_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_rawdata_image_output_from_trig();


CREATE TRIGGER rawdata_image_output_update_trig
  INSTEAD OF UPDATE ON
  rawdata_image_output_view FOR EACH ROW
  EXECUTE PROCEDURE update_rawdata_image_output_from_trig();

CREATE TRIGGER rawdata_image_output_update_epi_trig
  INSTEAD OF UPDATE ON
  rawdata_image_output_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_rawdata_image_output_from_trig();
