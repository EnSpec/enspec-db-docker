-- TABLE
DROP TABLE IF EXISTS radiometric_calibration_target CASCADE;
CREATE TABLE radiometric_calibration_target (
  radiometric_calibration_target_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  calibration_date DATE NOT NULL,
  collection_start_time TIME NOT NULL,
  collection_stop_time TIME NOT NULL,
  radiometric_target_center GEOMETRY NOT NULL,
  radiometric_target_poly GEOMETRY NOT NULL,
  raw_file_loc TEXT NOT NULL,
  processed_spectra_loc TEXT NOT NULL
);
CREATE INDEX radiometric_calibration_target_source_id_idx ON radiometric_calibration_target(source_id);

-- VIEW
CREATE OR REPLACE VIEW radiometric_calibration_target_view AS
  SELECT
    r.radiometric_calibration_target_id AS radiometric_calibration_target_id,
    r.calibration_date  as calibration_date,
    r.collection_start_time  as collection_start_time,
    r.collection_stop_time  as collection_stop_time,
    ST_AsKML(r.radiometric_target_center)  as radiometric_target_center_kml,
    ST_AsKML(r.radiometric_target_poly)  as radiometric_target_poly_kml,
    r.raw_file_loc  as raw_file_loc,
    r.processed_spectra_loc  as processed_spectra_loc,

    sc.name AS source_name
  FROM
    radiometric_calibration_target r
LEFT JOIN source sc ON r.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_radiometric_calibration_target (
  radiometric_calibration_target_id UUID,
  calibration_date DATE,
  collection_start_time TIME,
  collection_stop_time TIME,
  radiometric_target_center_kml TEXT,
  radiometric_target_poly_kml TEXT,
  raw_file_loc TEXT,
  processed_spectra_loc TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  radiometric_target_center_geom GEOMETRY;
  radiometric_target_poly_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(radiometric_target_center_kml) INTO radiometric_target_center_geom;
  SELECT ST_GeomFromKML(radiometric_target_poly_kml) INTO radiometric_target_poly_geom;
  IF( radiometric_calibration_target_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO radiometric_calibration_target_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO radiometric_calibration_target (
    radiometric_calibration_target_id, calibration_date, collection_start_time, collection_stop_time, radiometric_target_center, radiometric_target_poly, raw_file_loc, processed_spectra_loc, source_id
  ) VALUES (
    radiometric_calibration_target_id, calibration_date, collection_start_time, collection_stop_time, radiometric_target_center_geom, radiometric_target_poly_geom, raw_file_loc, processed_spectra_loc, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_radiometric_calibration_target (
  radiometric_calibration_target_id_in UUID,
  calibration_date_in DATE,
  collection_start_time_in TIME,
  collection_stop_time_in TIME,
  radiometric_target_center_kml_in TEXT,
  radiometric_target_poly_kml_in TEXT,
  raw_file_loc_in TEXT,
  processed_spectra_loc_in TEXT) RETURNS void AS $$
DECLARE
radiometric_target_center_geom GEOMETRY;
radiometric_target_poly_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(radiometric_target_center_kml_in) INTO radiometric_target_center_geom;
  SELECT ST_GeomFromKML(radiometric_target_poly_kml_in) INTO radiometric_target_poly_geom;
  UPDATE radiometric_calibration_target SET (
    calibration_date, collection_start_time, collection_stop_time, radiometric_target_center, radiometric_target_poly, raw_file_loc, processed_spectra_loc
  ) = (
    calibration_date_in, collection_start_time_in, collection_stop_time_in, radiometric_target_center_geom, radiometric_target_poly_geom, raw_file_loc_in, processed_spectra_loc_in
  ) WHERE
    radiometric_calibration_target_id = radiometric_calibration_target_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_radiometric_calibration_target_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_radiometric_calibration_target(
    radiometric_calibration_target_id := NEW.radiometric_calibration_target_id,
    calibration_date := NEW.calibration_date,
    collection_start_time := NEW.collection_start_time,
    collection_stop_time := NEW.collection_stop_time,
    radiometric_target_center_kml := NEW.radiometric_target_center_kml,
    radiometric_target_poly_kml := NEW.radiometric_target_poly_kml,
    raw_file_loc := NEW.raw_file_loc,
    processed_spectra_loc := NEW.processed_spectra_loc,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_radiometric_calibration_target_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_radiometric_calibration_target(
    radiometric_calibration_target_id_in := NEW.radiometric_calibration_target_id,
    calibration_date_in := NEW.calibration_date,
    collection_start_time_in := NEW.collection_start_time,
    collection_stop_time_in := NEW.collection_stop_time,
    radiometric_target_center_kml_in := NEW.radiometric_target_center_kml,
    radiometric_target_poly_kml_in := NEW.radiometric_target_poly_kml,
    raw_file_loc_in := NEW.raw_file_loc,
    processed_spectra_loc_in := NEW.processed_spectra_loc
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_radiometric_calibration_target_id(
  calibration_date_in DATE,
  collection_start_time_in TIME,
  collection_stop_time_in TIME,
  radiometric_target_center_kml_in TEXT,
  radiometric_target_poly_kml_in TEXT,
  raw_file_loc_in TEXT,
  processed_spectra_loc_in TEXT
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  radiometric_target_center_geom GEOMETRY;
  radiometric_target_poly_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(radiometric_target_center_kml_in) INTO radiometric_target_center_geom;
  SELECT ST_GeomFromKML(radiometric_target_poly_kml_in) INTO radiometric_target_poly_geom;
  SELECT
    radiometric_calibration_target_id INTO rid
  FROM
    radiometric_calibration_target r
  WHERE
    calibration_date = calibration_date_in AND
    collection_start_time = collection_start_time_in AND
    collection_stop_time = collection_stop_time_in AND
    radiometric_target_center = radiometric_target_center_geom AND
    radiometric_target_poly = radiometric_target_poly_geom AND
    raw_file_loc = raw_file_loc_in AND
    processed_spectra_loc = processed_spectra_loc_in;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown radiometric_calibration_target: calibration_date="%" collection_start_time="%" collection_stop_time="%" radiometric_target_center_kml="%" radiometric_target_poly_kml="%"
    raw_file_loc="%" processed_spectra_loc="%"', calibration_date_in, collection_start_time_in, collection_stop_time_in, radiometric_target_center_kml_in, radiometric_target_poly_kml_in, raw_file_loc_in, processed_spectra_loc_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER radiometric_calibration_target_insert_trig
  INSTEAD OF INSERT ON
  radiometric_calibration_target_view FOR EACH ROW
  EXECUTE PROCEDURE insert_radiometric_calibration_target_from_trig();

CREATE TRIGGER radiometric_calibration_target_update_trig
  INSTEAD OF UPDATE ON
  radiometric_calibration_target_view FOR EACH ROW
  EXECUTE PROCEDURE update_radiometric_calibration_target_from_trig();
