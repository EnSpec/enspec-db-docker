-- TABLE
DROP TABLE IF EXISTS tree_data CASCADE;
CREATE TABLE tree_data (
  tree_data_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  canopy_level TREE_CANOPY_LEVEL NOT NULL,
  crown_poly GEOMETRY NOT NULL,
  tree_location GEOMETRY NOT NULL
);
CREATE INDEX tree_data_source_id_idx ON tree_data(source_id);

-- VIEW
CREATE OR REPLACE VIEW tree_data_view AS
  SELECT
    t.tree_data_id AS tree_data_id,
    t.canopy_level  as canopy_level,
    ST_AsKML(t.crown_poly)  as crown_poly_kml,
    ST_AsKML(t.tree_location)  as tree_location_kml,

    sc.name AS source_name
  FROM
    tree_data t
LEFT JOIN source sc ON t.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_tree_data (
  tree_data_id UUID,
  canopy_level TREE_CANOPY_LEVEL,
  crown_poly_kml TEXT,
  tree_location_kml TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  crown_poly_geom GEOMETRY;
  tree_location_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(crown_poly_kml) INTO crown_poly_geom;
  SELECT ST_GeomFromKML(tree_location_kml) INTO tree_location_geom;

  IF( tree_data_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO tree_data_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO tree_data (
    tree_data_id, canopy_level, crown_poly, tree_location, source_id
  ) VALUES (
    tree_data_id, canopy_level, crown_poly_geom, tree_location_geom, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_tree_data (
  tree_data_id_in UUID,
  canopy_level_in TREE_CANOPY_LEVEL,
  crown_poly_kml_in TEXT,
  tree_location_kml_in TEXT) RETURNS void AS $$
DECLARE
crown_poly_geom GEOMETRY;
tree_location_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(crown_poly_kml_in) INTO crown_poly_geom;
  SELECT ST_GeomFromKML(tree_location_kml_in) INTO tree_location_geom;

  UPDATE tree_data SET (
    canopy_level, crown_poly, tree_location
  ) = (
    canopy_level_in, crown_poly_geom, tree_location_geom
  ) WHERE
    tree_data_id = tree_data_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_tree_data_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_tree_data(
    tree_data_id := NEW.tree_data_id,
    canopy_level := NEW.canopy_level,
    crown_poly_kml := NEW.crown_poly_kml,
    tree_location_kml := NEW.tree_location_kml,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_tree_data_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_tree_data(
    tree_data_id_in := NEW.tree_data_id,
    canopy_level_in := NEW.canopy_level,
    crown_poly_kml_in := NEW.crown_poly_kml,
    tree_location_kml_in := NEW.tree_location_kml
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_tree_data_id(canopy_level_in TREE_CANOPY_LEVEL, crown_poly_kml_in text, tree_location_kml_in text) RETURNS UUID AS $$
DECLARE
  tid UUID;
  crown_poly_geom GEOMETRY;
  tree_location_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(crown_poly_kml_in) INTO crown_poly_geom;
  SELECT ST_GeomFromKML(tree_location_kml_in) INTO tree_location_geom;
  SELECT
    tree_data_id INTO tid
  FROM
    tree_data t
  WHERE
    canopy_level = canopy_level_in AND
    crown_poly = crown_poly_geom AND
    tree_location = tree_location_geom;

  IF (tid IS NULL) THEN
    RAISE EXCEPTION 'Unknown tree_data: canopy_level="%" crown_poly_kml="%" tree_location_kml="%"', canopy_level_in, crown_poly_kml_in, tree_location_kml_in;
  END IF;

  RETURN tid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER tree_data_insert_trig
  INSTEAD OF INSERT ON
  tree_data_view FOR EACH ROW
  EXECUTE PROCEDURE insert_tree_data_from_trig();

CREATE TRIGGER tree_data_update_trig
  INSTEAD OF UPDATE ON
  tree_data_view FOR EACH ROW
  EXECUTE PROCEDURE update_tree_data_from_trig();
