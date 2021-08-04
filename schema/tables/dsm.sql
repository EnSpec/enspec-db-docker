-- TABLE
DROP TABLE IF EXISTS dsm CASCADE;
CREATE TABLE dsm (
  dsm_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  dsm_name TEXT UNIQUE NOT NULL,
  extent_poly GEOMETRY NOT NULL,
  epsg FLOAT NOT NULL,
  vdatum TEXT NOT NULL,
  dsm_file TEXT NOT NULL,
  dsm_metadata TEXT
);
CREATE INDEX dsm_source_id_idx ON dsm(source_id);

-- VIEW
CREATE OR REPLACE VIEW dsm_view AS
  SELECT
    d.dsm_id AS dsm_id,
    d.dsm_name  as dsm_name,
    ST_AsKML(d.extent_poly)  as extent_poly_kml,
    d.epsg  as epsg,
    d.vdatum  as vdatum,
    d.dsm_file  as dsm_file,
    d.dsm_metadata  as dsm_metadata,
    sc.name AS source_name
  FROM
    dsm d
LEFT JOIN source sc ON d.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_dsm (
  dsm_id UUID,
  dsm_name TEXT,
  extent_poly_kml TEXT,
  epsg FLOAT,
  vdatum TEXT,
  dsm_file TEXT,
  dsm_metadata TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  extent_poly_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(extent_poly_kml) INTO extent_poly_geom;
  IF( dsm_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO dsm_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO dsm (
    dsm_id, dsm_name, extent_poly, epsg, vdatum, dsm_file, dsm_metadata, source_id
  ) VALUES (
    dsm_id, dsm_name, extent_poly_geom, epsg, vdatum, dsm_file, dsm_metadata, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_dsm (
  dsm_id_in UUID,
  dsm_name_in TEXT,
  extent_poly_kml_in TEXT,
  epsg_in FLOAT,
  vdatum_in TEXT,
  dsm_file_in TEXT,
  dsm_metadata_in TEXT) RETURNS void AS $$
DECLARE
extent_poly_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(extent_poly_kml_in) INTO extent_poly_geom;
  UPDATE dsm SET (
    dsm_name, extent_poly, epsg, vdatum, dsm_file, dsm_metadata
  ) = (
    dsm_name_in, extent_poly_geom, epsg_in, vdatum_in, dsm_file_in, dsm_metadata_in
  ) WHERE
    dsm_id = dsm_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_dsm_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_dsm(
    dsm_id := NEW.dsm_id,
    dsm_name := NEW.dsm_name,
    extent_poly_kml := NEW.extent_poly_kml,
    epsg := NEW.epsg,
    vdatum := NEW.vdatum,
    dsm_file := NEW.dsm_file,
    dsm_metadata := NEW.dsm_metadata,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_dsm_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_dsm(
    dsm_id_in := NEW.dsm_id,
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
CREATE OR REPLACE FUNCTION get_dsm_id(dsm_name_in text, extent_poly_kml_in text, epsg_in float, vdatum_in text, dsm_file_in text, dsm_metadata_in text) RETURNS UUID AS $$
DECLARE
  did UUID;
  extent_poly_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(extent_poly_kml_in) INTO extent_poly_geom;
  IF dsm_metadata_in IS NULL THEN
    SELECT
      dsm_id INTO did
    FROM
      dsm d
    WHERE
      dsm_name = dsm_name_in AND
      extent_poly = extent_poly_geom AND
      epsg = epsg_in AND
      vdatum = vdatum_in AND
      dsm_file = dsm_file_in AND
      dsm_metadata is NULL;
  ELSE
    SELECT
      dsm_id INTO did
    FROM
      dsm d
    WHERE
      dsm_name = dsm_name_in AND
      extent_poly = extent_poly_geom AND
      epsg = epsg_in AND
      vdatum = vdatum_in AND
      dsm_file = dsm_file_in AND
      dsm_metadata = dsm_metadata_in;
  END IF;

  IF (did IS NULL) THEN
    RAISE EXCEPTION 'Unknown dsm: dsm_name="%" extent_poly_kml="%" epsg="%" vdatum="%" dsm_file="%" dsm_metadata="%"', dsm_name_in, extent_poly_kml_in, epsg_in, vdatum_in, dsm_file_in, dsm_metadata_in;
  END IF;

  RETURN did;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER dsm_insert_trig
  INSTEAD OF INSERT ON
  dsm_view FOR EACH ROW
  EXECUTE PROCEDURE insert_dsm_from_trig();

CREATE TRIGGER dsm_update_trig
  INSTEAD OF UPDATE ON
  dsm_view FOR EACH ROW
  EXECUTE PROCEDURE update_dsm_from_trig();
