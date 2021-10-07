-- TABLE
DROP FUNCTION update_geometric_calibration_points(uuid,text,text,integer,date);
DROP FUNCTION get_geometric_calibration_points_id(text,text,integer,date);
DROP TABLE IF EXISTS geometric_calibration_points CASCADE;

CREATE TABLE geometric_calibration_points (
  geometric_calibration_points_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  gcp_csv_loc TEXT NOT NULL,
  gcp_coordinate_system INT NOT NULL,
  gcp_date DATE NOT NULL,
  gcp_geom_loc GEOMETRY(POINT,4326)
);
CREATE INDEX geometric_calibration_points_source_id_idx ON geometric_calibration_points(source_id);
-- SELECT AddGeometryColumn('geometric_calibration_points', 'gcp_geom_loc', '4326', 'POINT', 2);

-- VIEW
CREATE OR REPLACE VIEW geometric_calibration_points_view AS
  SELECT
    g.geometric_calibration_points_id AS geometric_calibration_points_id,
    ST_X(g.gcp_geom_loc)  as gcp_long,
    ST_Y(g.gcp_geom_loc)  as gcp_lat,
    g.gcp_csv_loc  as gcp_csv_loc,
    g.gcp_coordinate_system  as gcp_coordinate_system,
    g.gcp_date  as gcp_date,

    sc.name AS source_name
  FROM
    geometric_calibration_points g
LEFT JOIN source sc ON g.source_id = sc.source_id;
-- END;
-- $$ LANGUAGE plpgsql;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_geometric_calibration_points (
  geometric_calibration_points_id UUID,
  gcp_long FLOAT,
  gcp_lat FLOAT,
  gcp_csv_loc TEXT,
  gcp_coordinate_system INT,
  gcp_date DATE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  gcp_latlong_to_geom GEOMETRY;
BEGIN
  SELECT ST_SetSRID(ST_MakePoint(gcp_long, gcp_lat), 4326) INTO gcp_latlong_to_geom;

  IF( geometric_calibration_points_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO geometric_calibration_points_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO geometric_calibration_points (
    geometric_calibration_points_id, gcp_geom_loc, gcp_csv_loc, gcp_coordinate_system, gcp_date, source_id
  ) VALUES (
    geometric_calibration_points_id, gcp_latlong_to_geom, gcp_csv_loc, gcp_coordinate_system, gcp_date, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_geometric_calibration_points (
  geometric_calibration_points_id_in UUID,
  gcp_long_in FLOAT,
  gcp_lat_in FLOAT,
  gcp_csv_loc_in TEXT,
  gcp_coordinate_system_in INT,
  gcp_date_in DATE) RETURNS void AS $$
DECLARE
gcp_latlong_to_geom GEOMETRY;
BEGIN
  SELECT ST_SetSRID(ST_MakePoint(gcp_long_in, gcp_lat_in),4326) INTO gcp_latlong_to_geom;
  UPDATE geometric_calibration_points SET (
    gcp_geom_loc, gcp_csv_loc, gcp_coordinate_system, gcp_date
  ) = (
    gcp_latlong_to_geom, gcp_csv_loc_in, gcp_coordinate_system_in, gcp_date_in
  ) WHERE
    geometric_calibration_points_id = geometric_calibration_points_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_geometric_calibration_points_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_geometric_calibration_points(
    geometric_calibration_points_id := NEW.geometric_calibration_points_id,
    gcp_long := NEW.gcp_long,
    gcp_lat := NEW.gcp_lat,
    gcp_csv_loc := NEW.gcp_csv_loc,
    gcp_coordinate_system := NEW.gcp_coordinate_system,
    gcp_date := NEW.gcp_date,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_geometric_calibration_points_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_geometric_calibration_points(
    geometric_calibration_points_id_in := NEW.geometric_calibration_points_id,
    gcp_long_in := NEW.gcp_long,
    gcp_lat_in := NEW.gcp_lat,
    gcp_csv_loc_in := NEW.gcp_csv_loc,
    gcp_coordinate_system_in := NEW.gcp_coordinate_system,
    gcp_date_in := NEW.gcp_date
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_geometric_calibration_points_id(
  gcp_long_in FLOAT,
  gcp_lat_in FLOAT,
  gcp_csv_loc_in TEXT,
  gcp_coordinate_system_in INT,
  gcp_date_in DATE
) RETURNS UUID AS $$
DECLARE
  gid UUID;
  gcp_latlong_to_geom GEOMETRY;
BEGIN
  SELECT ST_SetSRID(ST_MakePoint(gcp_long_in, gcp_lat_in), 4326) INTO gcp_latlong_to_geom;
  SELECT
    geometric_calibration_points_id INTO gid
  FROM
    geometric_calibration_points g
  WHERE
    gcp_geom_loc = gcp_latlong_to_geom AND
    gcp_csv_loc = gcp_csv_loc_in AND
    gcp_coordinate_system = gcp_coordinate_system_in AND
    gcp_date = gcp_date_in;

  IF (gid IS NULL) THEN
    RAISE EXCEPTION 'Unknown geometric_calibration_points: gcp_long="%" gcp_lat="%" gcp_csv_loc="%" gcp_coordinate_system="%" gcp_date="%"',
    gcp_long_in, gcp_lat_in, gcp_csv_loc_in, gcp_coordinate_system_in, gcp_date_in;
  END IF;

  RETURN gid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER geometric_calibration_points_insert_trig
  INSTEAD OF INSERT ON
  geometric_calibration_points_view FOR EACH ROW
  EXECUTE PROCEDURE insert_geometric_calibration_points_from_trig();

CREATE TRIGGER geometric_calibration_points_update_trig
  INSTEAD OF UPDATE ON
  geometric_calibration_points_view FOR EACH ROW
  EXECUTE PROCEDURE update_geometric_calibration_points_from_trig();
