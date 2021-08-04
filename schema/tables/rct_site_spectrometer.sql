-- TABLE
DROP TABLE IF EXISTS rct_site_spectrometer CASCADE;
CREATE TABLE rct_site_spectrometer (
  rct_site_spectrometer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  rct_id UUID REFERENCES radiometric_calibration_target NOT NULL,
  study_sites_id UUID REFERENCES study_sites NOT NULL,
  spectrometer_id UUID REFERENCES instruments NOT NULL
);
CREATE INDEX rct_site_spectrometer_source_id_idx ON rct_site_spectrometer(source_id);
CREATE INDEX rct_site_spectrometer_rct_id_idx ON rct_site_spectrometer(rct_id);
CREATE INDEX rct_site_spectrometer_study_sites_id_idx ON rct_site_spectrometer(study_sites_id);
CREATE INDEX rct_site_spectrometer_spectrometer_id_idx ON rct_site_spectrometer(spectrometer_id);

-- VIEW
CREATE OR REPLACE VIEW rct_site_spectrometer_view AS
  SELECT
    r.rct_site_spectrometer_id AS rct_site_spectrometer_id,
    rct.calibration_date  as calibration_date,
    rct.collection_start_time  as collection_start_time,
    rct.collection_stop_time  as collection_stop_time,
    ST_AsKML(rct.radiometric_target_center)  as radiometric_target_center_kml,
    ST_AsKML(rct.radiometric_target_poly)  as radiometric_target_poly_kml,
    rct.raw_file_loc  as raw_file_loc,
    rct.processed_spectra_loc  as processed_spectra_loc,
    s.site_name  as site_name,
    s.region  as region,
    ST_AsKML(s.site_poly)  as site_poly_kml,
    i.make as make,
    i.model as model,
    i.serial_number as serial_number,
    i.type as type,

    sc.name AS source_name
  FROM
    rct_site_spectrometer r
LEFT JOIN source sc ON r.source_id = sc.source_id
LEFT JOIN radiometric_calibration_target rct ON r.rct_id = rct.radiometric_calibration_target_id
LEFT JOIN study_sites s ON r.study_sites_id = s.study_sites_id
LEFT JOIN instruments i ON r.spectrometer_id = i.instruments_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rct_site_spectrometer (
  rct_site_spectrometer_id UUID,
  calibration_date DATE,
  collection_start_time TIME,
  collection_stop_time TIME,
  radiometric_target_center_kml TEXT,
  radiometric_target_poly_kml TEXT,
  raw_file_loc TEXT,
  processed_spectra_loc TEXT,
  site_name TEXT,
  region TEXT,
  site_poly_kml TEXT,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  serial_number TEXT,
  type INSTRUMENT_TYPE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  rctid UUID;
  sid UUID;
  iid UUID;
BEGIN
  SELECT get_radiometric_calibration_target_id(calibration_date, collection_start_time, collection_stop_time,
    radiometric_target_center_kml, radiometric_target_poly_kml, raw_file_loc, processed_spectra_loc
  ) INTO rctid;
  SELECT get_study_sites_id(site_name, region, site_poly_kml) INTO sid;
  SELECT get_instruments_id(make, model, serial_number, type) INTO iid;

  IF( rct_site_spectrometer_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rct_site_spectrometer_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rct_site_spectrometer (
    rct_site_spectrometer_id, rct_id, study_sites_id, spectrometer_id, source_id
  ) VALUES (
    rct_site_spectrometer_id, rctid, sid, iid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rct_site_spectrometer (
  rct_site_spectrometer_id_in UUID,
  calibration_date_in DATE,
  collection_start_time_in TIME,
  collection_stop_time_in TIME,
  radiometric_target_center_kml_in TEXT,
  radiometric_target_poly_kml_in TEXT,
  raw_file_loc_in TEXT,
  processed_spectra_loc_in TEXT,
  site_name_in TEXT,
  region_in TEXT,
  site_poly_kml_in TEXT,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE
) RETURNS void AS $$
DECLARE
rctid UUID;
sid UUID;
iid UUID;
BEGIN
  SELECT get_radiometric_calibration_target_id(calibration_date_in, collection_start_time_in, collection_stop_time_in,
    radiometric_target_center_kml_in, radiometric_target_poly_kml_in, raw_file_loc_in, processed_spectra_loc_in
  ) INTO rctid;
  SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO sid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;
  UPDATE rct_site_spectrometer SET (
    rct_id, study_sites_id, spectrometer_id
  ) = (
    rctid, sid, iid
  ) WHERE
    rct_site_spectrometer_id = rct_site_spectrometer_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_rct_site_spectrometer_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_rct_site_spectrometer(
    rct_site_spectrometer_id := NEW.rct_site_spectrometer_id,
    calibration_date := NEW.calibration_date,
    collection_start_time := NEW.collection_start_time,
    collection_stop_time := NEW.collection_stop_time,
    radiometric_target_center_kml := NEW.radiometric_target_center_kml,
    radiometric_target_poly_kml := NEW.radiometric_target_poly_kml,
    raw_file_loc := NEW.raw_file_loc,
    processed_spectra_loc := NEW.processed_spectra_loc,
    site_name := NEW.site_name,
    region := NEW.region,
    site_poly_kml := NEW.site_poly_kml,
    make := NEW.make,
    model := NEW.model,
    serial_number := NEW.serial_number,
    type := NEW.type,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rct_site_spectrometer_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_rct_site_spectrometer(
    rct_site_spectrometer_id_in := NEW.rct_site_spectrometer_id,
    calibration_date_in := NEW.calibration_date,
    collection_start_time_in := NEW.collection_start_time,
    collection_stop_time_in := NEW.collection_stop_time,
    radiometric_target_center_kml_in := NEW.radiometric_target_center_kml,
    radiometric_target_poly_kml_in := NEW.radiometric_target_poly_kml,
    raw_file_loc_in := NEW.raw_file_loc,
    processed_spectra_loc_in := NEW.processed_spectra_loc,
    site_name_in := NEW.site_name,
    region_in := NEW.region,
    site_poly_kml_in := NEW.site_poly_kml,
    make_in := NEW.make,
    model_in := NEW.model,
    serial_number_in := NEW.serial_number,
    type_in := NEW.type
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_rct_site_spectrometer_id(
  calibration_date_in DATE,
  collection_start_time_in TIME,
  collection_stop_time_in TIME,
  radiometric_target_center_kml_in TEXT,
  radiometric_target_poly_kml_in TEXT,
  raw_file_loc_in TEXT,
  processed_spectra_loc_in TEXT,
  site_name_in text, region_in text,
  site_poly_kml_in TEXT,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  rctid UUID;
  sid UUID;
  iid UUID;
BEGIN
  SELECT get_radiometric_calibration_target_id(calibration_date_in, collection_start_time_in, collection_stop_time_in,
    radiometric_target_center_kml_in, radiometric_target_poly_kml_in, raw_file_loc_in, processed_spectra_loc_in
  ) INTO rctid;
  SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO sid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;
  SELECT
    rct_site_spectrometer_id INTO rid
  FROM
    rct_site_spectrometer r
  WHERE
    radiometric_calibration_target_id = rctid AND
    study_sites_id = sid AND
    spectrometer_id = iid;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rct_site_spectrometer: calibration_date="%" collection_start_time="%" collection_stop_time="%" radiometric_target_center_kml="%" radiometric_target_poly_kml="%"
    raw_file_loc="%" processed_spectra_loc="%" study_sites: site_name="%" region="%" site_poly_kml="%" serial_number="%"', calibration_date_in, collection_start_time_in, collection_stop_time_in,
    radiometric_target_center_kml_in, radiometric_target_poly_kml_in, raw_file_loc_in, processed_spectra_loc_in, site_name_in, region_in, site_poly_kml_in, serial_number_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER rct_site_spectrometer_insert_trig
  INSTEAD OF INSERT ON
  rct_site_spectrometer_view FOR EACH ROW
  EXECUTE PROCEDURE insert_rct_site_spectrometer_from_trig();

CREATE TRIGGER rct_site_spectrometer_update_trig
  INSTEAD OF UPDATE ON
  rct_site_spectrometer_view FOR EACH ROW
  EXECUTE PROCEDURE update_rct_site_spectrometer_from_trig();
