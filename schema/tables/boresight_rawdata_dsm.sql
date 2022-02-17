-- TABLE
DROP TABLE IF EXISTS boresight_rawdata_dsm CASCADE;
CREATE TABLE boresight_rawdata_dsm (
  boresight_rawdata_dsm_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  boresight_offsets_id UUID REFERENCES boresight_offsets NOT NULL,
  rawdata_metadata_id UUID REFERENCES rawdata_metadata NOT NULL,
  dsm_id UUID REFERENCES dsm NOT NULL,
  UNIQUE(boresight_offsets_id, rawdata_metadata_id)
);
CREATE INDEX boresight_rawdata_dsm_source_id_idx ON boresight_rawdata_dsm(source_id);
CREATE INDEX boresight_rawdata_dsm_boresight_id_idx ON boresight_rawdata_dsm(boresight_offsets_id);
CREATE INDEX boresight_rawdata_dsm_rawdata_metadata_id_idx ON boresight_rawdata_dsm(rawdata_metadata_id);
CREATE INDEX boresight_rawdata_dsm_dsm_id_idx ON boresight_rawdata_dsm(dsm_id);

-- VIEW
CREATE OR REPLACE VIEW boresight_rawdata_dsm_view AS
  SELECT
    b.boresight_rawdata_dsm_id AS boresight_rawdata_dsm_id,
    bo.calculation_method  as calculation_method,
    bo.roll_offset  as roll_offset,
    bo.pitch_offset  as pitch_offset,
    bo.heading_offset  as heading_offset,
    bo.rmse  as rmse,
    bo.gcp_file  as gcp_file,
    f.flight_date AS flight_date,
    f.pilot AS pilot,
    f.operator AS operator,
    f.liftoff_time AS liftoff_time,
    s.session_name AS session_name,
    s.line_count AS line_count,
    fl.line_number AS line_number,
    r.capture_time AS capture_time,
    r.quality  as rawdata_quality,
    r.cold_storage as rd_cold_storage,
    r.hot_storage as rd_hot_storage,
    r.hot_storage_expiration as rd_hot_storage_expiration,
    d.dsm_name  as dsm_name,
    ST_AsKML(d.extent_poly)  as extent_poly_kml,
    d.epsg  as epsg,
    d.vdatum  as vdatum,
    d.dsm_file  as dsm_file,
    d.dsm_metadata  as dsm_metadata,
    sc.name AS source_name
  FROM
    boresight_rawdata_dsm b
LEFT JOIN source sc ON b.source_id = sc.source_id
LEFT JOIN boresight_offsets bo ON b.boresight_offsets_id = bo.boresight_offsets_id
LEFT JOIN rawdata_metadata r ON b.rawdata_metadata_id = r.rawdata_metadata_id
LEFT JOIN flightlines fl ON fl.flightlines_id = r.flightlines_id
LEFT JOIN sessions s ON s.sessions_id = fl.sessions_id
LEFT JOIN flights f ON s.flights_id = f.flights_id
LEFT JOIN dsm d ON b.dsm_id = d.dsm_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_boresight_rawdata_dsm (
  boresight_rawdata_dsm_id UUID,
  calculation_method TEXT,
  roll_offset FLOAT,
  pitch_offset FLOAT,
  heading_offset FLOAT,
  rmse FLOAT,
  gcp_file TEXT,
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
  dsm_name TEXT,
  extent_poly_kml TEXT,
  epsg FLOAT,
  vdatum TEXT,
  dsm_file TEXT,
  dsm_metadata TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  boid UUID;
  rid UUID;
  dsmid UUID;
BEGIN

  IF( boresight_rawdata_dsm_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO boresight_rawdata_dsm_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_boresight_offsets_id(calculation_method, roll_offset, pitch_offset, heading_offset, rmse, gcp_file) INTO boid;
  SELECT get_rawdata_metadata_id(flight_date, pilot, operator,liftoff_time, session_name, line_count,line_number, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration) INTO rid;
  SELECT get_dsm_id(dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata) INTO dsmid;

  INSERT INTO boresight_rawdata_dsm (
    boresight_rawdata_dsm_id, boresight_offsets_id, rawdata_metadata_id, dsm_id, source_id
  ) VALUES (
    boresight_rawdata_dsm_id, boid, rid, dsmid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_boresight_rawdata_dsm (
  boresight_rawdata_dsm_id UUID,
  calculation_method TEXT,
  roll_offset FLOAT,
  pitch_offset FLOAT,
  heading_offset FLOAT,
  rmse FLOAT,
  gcp_file TEXT,
  flightlines_id UUID,
  capture_time TIME,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  dsm_name TEXT,
  extent_poly_kml TEXT,
  epsg FLOAT,
  vdatum TEXT,
  dsm_file TEXT,
  dsm_metadata TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  boid UUID;
  rid UUID;
  dsmid UUID;
BEGIN

  IF( boresight_rawdata_dsm_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO boresight_rawdata_dsm_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_boresight_offsets_id(calculation_method, roll_offset, pitch_offset, heading_offset, rmse, gcp_file) INTO boid;
  SELECT get_epi_rawdata_metadata_id(flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration) INTO rid;
  SELECT get_dsm_id(dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata) INTO dsmid;

  INSERT INTO boresight_rawdata_dsm (
    boresight_rawdata_dsm_id, boresight_offsets_id, rawdata_metadata_id, dsm_id, source_id
  ) VALUES (
    boresight_rawdata_dsm_id, boid, rid, dsmid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_boresight_rawdata_dsm (
  boresight_rawdata_dsm_id_in UUID,
  calculation_method_in TEXT,
  roll_offset_in FLOAT,
  pitch_offset_in FLOAT,
  heading_offset_in FLOAT,
  rmse_in FLOAT,
  gcp_file_in TEXT,
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
  dsm_name_in TEXT,
  extent_poly_kml_in TEXT,
  epsg_in FLOAT,
  vdatum_in TEXT,
  dsm_file_in TEXT,
  dsm_metadata_in TEXT) RETURNS void AS $$
DECLARE
boid UUID;
rid UUID;
dsmid UUID;

BEGIN
  SELECT get_boresight_offsets_id(calculation_method_in, roll_offset_in, pitch_offset_in, heading_offset_in, rmse_in, gcp_file_in) INTO boid;
  SELECT get_rawdata_metadata_id(flight_date_in, pilot_in, operator_in,liftoff_time_in, session_name_in, line_count_in,line_number_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in) INTO rid;
  SELECT get_dsm_id(dsm_name_in, extent_poly_kml_in, epsg_in, vdatum_in, dsm_file_in, dsm_metadata_in) INTO dsmid;
  UPDATE boresight_rawdata_dsm SET (
    boresight_offsets_id, rawdata_metadata_id, dsm_id
  ) = (
    boid, rid, dsmid
  ) WHERE
    boresight_rawdata_dsm_id = boresight_rawdata_dsm_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_boresight_rawdata_dsm (
  boresight_rawdata_dsm_id_in UUID,
  calculation_method_in TEXT,
  roll_offset_in FLOAT,
  pitch_offset_in FLOAT,
  heading_offset_in FLOAT,
  rmse_in FLOAT,
  gcp_file_in TEXT,
  flightlines_id_in UUID,
  capture_time_in TIME,
  quality_in RAWDATA_QUALITY,
  cold_storage_in TEXT,
  hot_storage_in TEXT,
  hot_storage_expiration_in DATE,
  dsm_name_in TEXT,
  extent_poly_kml_in TEXT,
  epsg_in FLOAT,
  vdatum_in TEXT,
  dsm_file_in TEXT,
  dsm_metadata_in TEXT) RETURNS void AS $$
DECLARE
boid UUID;
rid UUID;
dsmid UUID;

BEGIN
  SELECT get_boresight_offsets_id(calculation_method_in, roll_offset_in, pitch_offset_in, heading_offset_in, rmse_in, gcp_file_in) INTO boid;
  SELECT get_epi_rawdata_metadata_id(flightlines_id_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in) INTO rid;
  SELECT get_dsm_id(dsm_name_in, extent_poly_kml_in, epsg_in, vdatum_in, dsm_file_in, dsm_metadata_in) INTO dsmid;
  UPDATE boresight_rawdata_dsm SET (
    boresight_offsets_id, rawdata_metadata_id, dsm_id
  ) = (
    boid, rid, dsmid
  ) WHERE
    boresight_rawdata_dsm_id = boresight_rawdata_dsm_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_boresight_rawdata_dsm_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_boresight_rawdata_dsm(
    boresight_rawdata_dsm_id := NEW.boresight_rawdata_dsm_id,
    calculation_method_in := NEW.calculation_method,
    roll_offset_in := NEW.roll_offset,
    pitch_offset_in := NEW.pitch_offset,
    heading_offset_in := NEW.heading_offset,
    rmse_in := NEW.rmse,
    gcp_file_in := NEW.gcp_file,
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
    dsm_name_in := NEW.dsm_name,
    extent_poly_kml_in := NEW.extent_poly_kml,
    epsg_in := NEW.epsg,
    vdatum_in := NEW.vdatum,
    dsm_file_in := NEW.dsm_file,
    dsm_metadata_in := NEW.dsm_metadata,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_boresight_rawdata_dsm_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_boresight_rawdata_dsm(
    boresight_rawdata_dsm_id := NEW.boresight_rawdata_dsm_id,
    calculation_method_in := NEW.calculation_method,
    roll_offset_in := NEW.roll_offset,
    pitch_offset_in := NEW.pitch_offset,
    heading_offset_in := NEW.heading_offset,
    rmse_in := NEW.rmse,
    gcp_file_in := NEW.gcp_file,
    flightlines_id := NEW.flightlines_id,
    capture_time := NEW.capture_time,
    quality_in := NEW.quality,
    cold_storage_in := NEW.cold_storage,
    hot_storage_in := NEW.hot_storage,
    hot_storage_expiration_in := NEW.hot_storage_expiration,
    dsm_name_in := NEW.dsm_name,
    extent_poly_kml_in := NEW.extent_poly_kml,
    epsg_in := NEW.epsg,
    vdatum_in := NEW.vdatum,
    dsm_file_in := NEW.dsm_file,
    dsm_metadata_in := NEW.dsm_metadata,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_boresight_rawdata_dsm_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_boresight_rawdata_dsm(
    boresight_rawdata_dsm_id_in := NEW.boresight_rawdata_dsm_id,
    calculation_method_in := NEW.calculation_method,
    roll_offset_in := NEW.roll_offset,
    pitch_offset_in := NEW.pitch_offset,
    heading_offset_in := NEW.heading_offset,
    rmse_in := NEW.rmse,
    gcp_file_in := NEW.gcp_file,
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
    dsm_name_in := NEW.dsm_name,
    extent_poly_kml_in := NEW.extent_poly_kml,
    epsg_in := NEW.epsg,
    vdatum_in := NEW.vdatum,
    dsm_file_in := NEW.dsm_file,
    dsm_metadata_in := NEW.dsm_metadata
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_boresight_rawdata_dsm_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_get_boresight_rawdata_dsm(
    boresight_rawdata_dsm_id_in := NEW.boresight_rawdata_dsm_id,
    calculation_method_in := NEW.calculation_method,
    roll_offset_in := NEW.roll_offset,
    pitch_offset_in := NEW.pitch_offset,
    heading_offset_in := NEW.heading_offset,
    rmse_in := NEW.rmse,
    gcp_file_in := NEW.gcp_file,
    flightlines_id_in := NEW.flightlines_id,
    capture_time_in := NEW.capture_time,
    quality_in := NEW.quality,
    cold_storage_in := NEW.cold_storage,
    hot_storage_in := NEW.hot_storage,
    hot_storage_expiration_in := NEW.hot_storage_expiration,
    dsm_name_in := NEW.dsm_name,
    extent_poly_kml_in := NEW.extent_poly_kml,
    epsg_in := NEW.epsg,
    vdatum_in := NEW.vdatum,
    dsm_file_in := NEW.dsm_file,
    dsm_metadata_in := NEW.dsm_metadata
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_boresight_rawdata_dsm_id(
  calculation_method TEXT,
  roll_offset FLOAT,
  pitch_offset FLOAT,
  heading_offset FLOAT,
  rmse FLOAT,
  gcp_file TEXT,
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
  dsm_name TEXT,
  extent_poly_kml TEXT,
  epsg FLOAT,
  vdatum TEXT,
  dsm_file TEXT,
  dsm_metadata TEXT
) RETURNS UUID AS $$
DECLARE
bid UUID;
boid UUID;
rid UUID;
dsmid UUID;
BEGIN
  SELECT get_boresight_offsets_id(calculation_method, roll_offset, pitch_offset, heading_offset, rmse, gcp_file) INTO boid;
  SELECT get_rawdata_metadata_id(flight_date_in, pilot_in, operator_in,liftoff_time_in, session_name_in, line_count_in,line_number_in, capture_time_in, quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in) INTO rid;
  SELECT get_dsm_id(dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata) INTO dsmid;
  SELECT
    boresight_rawdata_dsm_id INTO bid
  FROM
    boresight_rawdata_dsm b
  WHERE
    boresight_offsets_id = boid AND
    rawdata_metadata_id = rid AND
    dsm_id = dsmid;

  IF (bid IS NULL) THEN
    RAISE EXCEPTION 'Unknown boresight_rawdata_dsm: calculation_method="%" roll_offset="%" pitch_offset="%" heading_offset="%" rmse="%" gcp_file="%"
    quality="%" cold_storage="%" hot_storage="%" hot_storage_expiration="%" dsm_name="%" extent_poly_kml="%" epsg="%" vdatum="%" dsm_file="%" dsm_metadata="%"', calculation_method, roll_offset,
    pitch_offset, heading_offset, rmse, gcp_file, quality, cold_storage, hot_storage, hot_storage_expiration, dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata;
  END IF;

  RETURN bid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_epi_boresight_rawdata_dsm_id(
  calculation_method TEXT,
  roll_offset FLOAT,
  pitch_offset FLOAT,
  heading_offset FLOAT,
  rmse FLOAT,
  gcp_file TEXT,
  flightlines_id UUID,
  capture_time TIME,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  dsm_name TEXT,
  extent_poly_kml TEXT,
  epsg FLOAT,
  vdatum TEXT,
  dsm_file TEXT,
  dsm_metadata TEXT
) RETURNS UUID AS $$
DECLARE
bid UUID;
boid UUID;
rid UUID;
dsmid UUID;
BEGIN
  SELECT get_boresight_offsets_id(calculation_method, roll_offset, pitch_offset, heading_offset, rmse, gcp_file) INTO boid;
  SELECT get_epi_rawdata_metadata_id(flightlines_id, capture_time, quality, cold_storage, hot_storage, hot_storage_expiration) INTO rid;
  SELECT get_dsm_id(dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata) INTO dsmid;
  SELECT
    boresight_rawdata_dsm_id INTO bid
  FROM
    boresight_rawdata_dsm b
  WHERE
    boresight_offsets_id = boid AND
    rawdata_metadata_id = rid AND
    dsm_id = dsmid;

  IF (bid IS NULL) THEN
    RAISE EXCEPTION 'Unknown boresight_rawdata_dsm: calculation_method="%" roll_offset="%" pitch_offset="%" heading_offset="%" rmse="%" gcp_file="%"
    quality="%" cold_storage="%" hot_storage="%" hot_storage_expiration="%" dsm_name="%" extent_poly_kml="%" epsg="%" vdatum="%" dsm_file="%" dsm_metadata="%"', calculation_method, roll_offset,
    pitch_offset, heading_offset, rmse, gcp_file, quality, cold_storage, hot_storage, hot_storage_expiration, dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata;
  END IF;

  RETURN bid;
END ;
$$ LANGUAGE plpgsql;


-- RULES
CREATE TRIGGER boresight_rawdata_dsm_insert_trig
  INSTEAD OF INSERT ON
  boresight_rawdata_dsm_view FOR EACH ROW
  EXECUTE PROCEDURE insert_boresight_rawdata_dsm_from_trig();

CREATE TRIGGER boresight_rawdata_dsm_insert_epi_trig
  INSTEAD OF INSERT ON
  boresight_rawdata_dsm_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_boresight_rawdata_dsm_from_trig();

CREATE TRIGGER boresight_rawdata_dsm_update_trig
  INSTEAD OF UPDATE ON
  boresight_rawdata_dsm_view FOR EACH ROW
  EXECUTE PROCEDURE update_boresight_rawdata_dsm_from_trig();

CREATE TRIGGER boresight_rawdata_dsm_update_epi_trig
  INSTEAD OF UPDATE ON
  boresight_rawdata_dsm_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_boresight_rawdata_dsm_from_trig();
