-- TABLE
DROP TABLE IF EXISTS study_sites CASCADE;
CREATE TABLE study_sites (
  study_sites_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  site_name TEXT NOT NULL,
  region TEXT,
  site_poly GEOMETRY NOT NULL,
  UNIQUE(site_name, region)
);
CREATE INDEX study_sites_source_id_idx ON study_sites(source_id);
-- Add unique constraint

-- VIEW
CREATE OR REPLACE VIEW study_sites_view AS
  SELECT
    s.study_sites_id AS study_sites_id,
    s.site_name  as site_name,
    s.region  as region,
    ST_AsKML(s.site_poly)  as site_poly_kml,
    sc.name AS source_name
  FROM
    study_sites s
LEFT JOIN source sc ON s.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_study_sites (
  study_sites_id UUID,
  site_name TEXT,
  region TEXT,
  site_poly_kml TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  site_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(site_poly_kml) INTO site_kml_to_geom;
  IF( study_sites_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO study_sites_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO study_sites (
    study_sites_id, site_name, region, site_poly, source_id
  ) VALUES (
    study_sites_id, site_name, region, site_kml_to_geom, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_study_sites (
  study_sites_id_in UUID,
  site_name_in TEXT,
  region_in TEXT,
  site_poly_kml_in TEXT) RETURNS void AS $$
DECLARE
site_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(site_poly_kml_in) INTO site_kml_to_geom;
  UPDATE study_sites SET (
    site_name, region, site_poly
  ) = (
    site_name_in, region_in, site_kml_to_geom
  ) WHERE
    study_sites_id = study_sites_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_study_sites_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_study_sites(
    study_sites_id := NEW.study_sites_id,
    site_name := NEW.site_name,
    region := NEW.region,
    site_poly_kml := NEW.site_poly_kml,
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_study_sites_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_study_sites(
    study_sites_id_in := NEW.study_sites_id,
    site_name_in := NEW.site_name,
    region_in := NEW.region,
    site_poly_kml_in := NEW.site_poly_kml
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_study_sites_id(site_name_in text, region_in text, site_poly_kml_in text) RETURNS UUID AS $$
DECLARE
  sid UUID;
  site_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(site_poly_kml_in) INTO site_kml_to_geom;
  IF region is NULL THEN
    SELECT
      study_sites_id INTO sid
    FROM
      study_sites s
    WHERE
      site_name = site_name_in AND
      region is NULL AND
      site_poly = site_kml_to_geom;
  ELSE
    SELECT
      study_sites_id INTO sid
    FROM
      study_sites s
    WHERE
      site_name = site_name_in AND
      region = region_in AND
      site_poly = site_kml_to_geom;
  END IF;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown study_sites: site_name="%" region="%" site_poly_kml="%"', site_name_in, region_in, site_poly_kml_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER study_sites_insert_trig
  INSTEAD OF INSERT ON
  study_sites_view FOR EACH ROW
  EXECUTE PROCEDURE insert_study_sites_from_trig();

CREATE TRIGGER study_sites_update_trig
  INSTEAD OF UPDATE ON
  study_sites_view FOR EACH ROW
  EXECUTE PROCEDURE update_study_sites_from_trig();
