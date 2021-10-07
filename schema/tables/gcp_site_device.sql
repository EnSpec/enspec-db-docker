-- TABLE
DROP TABLE IF EXISTS gcp_site_device CASCADE;
CREATE TABLE gcp_site_device (
  gcp_site_device_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  gcp_id UUID REFERENCES geometric_calibration_points NOT NULL,
  study_sites_id UUID REFERENCES study_sites NOT NULL,
  device_id UUID REFERENCES instruments NOT NULL
);
CREATE INDEX gcp_site_device_source_id_idx ON gcp_site_device(source_id);
CREATE INDEX gcp_site_device_gcp_id_idx ON gcp_site_device(gcp_id);
CREATE INDEX gcp_site_device_site_id_idx ON gcp_site_device(study_sites_id);
CREATE INDEX gcp_site_device_device_id_idx ON gcp_site_device(device_id);

-- VIEW
CREATE OR REPLACE VIEW gcp_site_device_view AS
  SELECT
    g.gcp_site_device_id AS gcp_site_device_id,
    ST_X(gcp.gcp_geom_loc)  as gcp_long,
    ST_Y(gcp.gcp_geom_loc)  as gcp_lat,
    gcp.gcp_csv_loc  as gcp_csv_loc,
    gcp.gcp_coordinate_system  as gcp_coordinate_system,
    gcp.gcp_date  as gcp_date,
    s.site_name  as site_name,
    s.region  as region,
    ST_AsKML(s.site_poly)  as site_poly_kml,
    i.make as make,
    i.model as model,
    i.serial_number as serial_number,
    i.type as type,

    sc.name AS source_name
  FROM
    gcp_site_device g
LEFT JOIN source sc ON g.source_id = sc.source_id
LEFT JOIN geometric_calibration_points gcp ON g.gcp_id = gcp.geometric_calibration_points_id
LEFT JOIN study_sites s ON g.study_sites_id = s.study_sites_id
LEFT JOIN instruments i ON g.device_id = i.instruments_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_gcp_site_device (
  gcp_site_device_id UUID,
  gcp_long FLOAT,
  gcp_lat FLOAT,
  gcp_csv_loc TEXT,
  gcp_coordinate_system INT,
  gcp_date DATE,
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
  gcpid UUID;
  sid UUID;
  iid UUID;
BEGIN
  SELECT get_geometric_calibration_points_id(gcp_long, gcp_lat, gcp_csv_loc, gcp_coordinate_system, gcp_date) INTO gcpid;
  SELECT get_study_sites_id(site_name, region, site_poly_kml) INTO sid;
  SELECT get_instruments_id(make, model, serial_number, type) INTO iid;
  IF( gcp_site_device_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO gcp_site_device_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO gcp_site_device (
    gcp_site_device_id, gcp_id, study_sites_id, device_id, source_id
  ) VALUES (
    gcp_site_device_id, gcpid, sid, iid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_gcp_site_device (
  gcp_site_device_id_in UUID,
  gcp_long_in FLOAT,
  gcp_lat_in FLOAT,
  gcp_csv_loc_in TEXT,
  gcp_coordinate_system_in INT,
  gcp_date_in DATE,
  site_name_in TEXT,
  region_in TEXT,
  site_poly_kml_in TEXT,
  source_name_in TEXT,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE
) RETURNS void AS $$
DECLARE
gcpid UUID;
sid UUID;
iid UUID;
BEGIN
  SELECT get_geometric_calibration_points_id(gcp_long_in, gcp_lat_in, gcp_csv_loc_in, gcp_coordinate_system_in, gcp_date_in) INTO gcpid;
  SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO sid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;

  UPDATE gcp_site_device SET (
    gcp_id, study_sites_id, device_id
  ) = (
    gcpid, sid, iid
  ) WHERE
    gcp_site_device_id = gcp_site_device_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_gcp_site_device_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_gcp_site_device(
    gcp_site_device_id := NEW.gcp_site_device_id,
    gcp_long := NEW.gcp_long,
    gcp_lat := NEW.gcp_lat,
    gcp_csv_loc := NEW.gcp_csv_loc,
    gcp_coordinate_system := NEW.gcp_coordinate_system,
    gcp_date := NEW.gcp_date,
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

CREATE OR REPLACE FUNCTION update_gcp_site_device_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_gcp_site_device(
    gcp_site_device_id_in := NEW.gcp_site_device_id,
    gcp_long_in := NEW.gcp_long,
    gcp_lat_in := NEW.gcp_lat,
    gcp_csv_loc_in := NEW.gcp_csv_loc,
    gcp_coordinate_system_in := NEW.gcp_coordinate_system,
    gcp_date_in := NEW.gcp_date,
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
CREATE OR REPLACE FUNCTION get_gcp_site_device_id(
  gcp_long_in FLOAT,
  gcp_lat_in FLOAT,
  gcp_csv_loc_in TEXT,
  gcp_coordinate_system_in INT,
  gcp_date_in DATE,
  site_name_in text,
  region_in text,
  site_poly_kml_in TEXT,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE
) RETURNS UUID AS $$
DECLARE
  gid UUID;
  gcpid UUID;
  sid UUID;
  iid UUID;
BEGIN
  SELECT get_geometric_calibration_points_id(gcp_long_in, gcp_lat_in, gcp_csv_loc_in, gcp_coordinate_system_in, gcp_date_in) INTO gcpid;
  SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO sid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;
  SELECT
    gcp_site_device_id INTO gid
  FROM
    gcp_site_device g
  WHERE
    geometric_calibration_points_id = gcpid AND
    study_sites_id = sid AND
    device_id = iid;

  IF (gid IS NULL) THEN
    RAISE EXCEPTION 'Unknown gcp_site_device: gcp_long="%" gcp_lat="%" gcp_csv_loc="%" gcp_coordinate_system="%" gcp_date="%" site_name="%" region="%" site_poly_kml="%"
    serial_number="%"', gcp_long_in, gcp_lat_in, gcp_csv_loc_in, gcp_coordinate_system_in, gcp_date_in, site_name_in, region_in, site_poly_kml_in, serial_number_in;
  END IF;

  RETURN gid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER gcp_site_device_insert_trig
  INSTEAD OF INSERT ON
  gcp_site_device_view FOR EACH ROW
  EXECUTE PROCEDURE insert_gcp_site_device_from_trig();

CREATE TRIGGER gcp_site_device_update_trig
  INSTEAD OF UPDATE ON
  gcp_site_device_view FOR EACH ROW
  EXECUTE PROCEDURE update_gcp_site_device_from_trig();
