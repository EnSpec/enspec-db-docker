-- TABLE
DROP TABLE IF EXISTS tree_observations CASCADE;
CREATE TABLE tree_observations (
  tree_observations_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  tree_data_id UUID REFERENCES tree_data NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX tree_observations_source_id_idx ON tree_observations(source_id);
CREATE INDEX tree_observations_tree_id_idx ON tree_observations(tree_data_id);
CREATE INDEX tree_observations_variables_id_idx ON tree_observations(variables_id);
CREATE INDEX tree_observations_units_id_idx ON tree_observations(units_id);

-- VIEW
CREATE OR REPLACE VIEW tree_observations_view AS
  SELECT
    t.tree_observations_id AS tree_observations_id,
    td.canopy_level  as canopy_level,
    ST_AsKML(td.crown_poly)  as crown_poly_kml,
    ST_X(td.tree_location)  as tree_loc_long,
    ST_Y(td.tree_location)  as tree_loc_lat,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,
    t.value  as value,

    sc.name AS source_name
  FROM
    tree_observations t
LEFT JOIN source sc ON t.source_id = sc.source_id
LEFT JOIN tree_data td ON t.tree_data_id = td.tree_data_id
LEFT JOIN variables v ON t.variables_id = v.variables_id
LEFT JOIN units u ON t.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_tree_observations (
  tree_observations_id UUID,
  canopy_level TREE_CANOPY_LEVEL,
  crown_poly_kml TEXT,
  tree_loc_long FLOAT,
  tree_loc_lat FLOAT,
  variable_name TEXT,
  variable_type TEXT,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,
  value FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  tdid UUID;
  vid UUID;
  uid UUID;
BEGIN

  SELECT get_tree_data_id(canopy_level, crown_poly_kml, tree_loc_long, tree_loc_lat) INTO tdid;
  SELECT get_variables_id(variable_name, variable_type) INTO vid;
  SELECT get_units_id(units_name, units_type, units_description) INTO uid;
  IF( tree_observations_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO tree_observations_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO tree_observations (
    tree_observations_id, tree_data_id, variables_id, units_id, value, source_id
  ) VALUES (
    tree_observations_id, tdid, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_tree_observations (
  tree_observations_id_in UUID,
  canopy_level_in TREE_CANOPY_LEVEL,
  crown_poly_kml_in TEXT,
  tree_loc_long_in FLOAT,
  tree_loc_lat_in FLOAT,
  variable_name_in TEXT,
  variable_type_in TEXT,
  units_name_in TEXT,
  units_type_in TEXT,
  units_description_in TEXT,
  value_in FLOAT) RETURNS void AS $$
DECLARE
tdid UUID;
vid UUID;
uid UUID;

BEGIN
  SELECT get_tree_data_id(canopy_level_in, crown_poly_kml_in, tree_loc_long_in, tree_loc_lat_in) INTO tdid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  UPDATE tree_observations SET (
    tree_data_id, variables_id, units_id, value
  ) = (
    tdid, vid, uid, value_in
  ) WHERE
    tree_observations_id = tree_observations_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_tree_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_tree_observations(
    tree_observations_id := NEW.tree_observations_id,
    canopy_level := NEW.canopy_level,
    crown_poly_kml := NEW.crown_poly_kml,
    tree_loc_long := NEW.tree_loc_long,
    tree_loc_lat := NEW.tree_loc_lat,
    variable_name := NEW.variable_name,
    variable_type := NEW.variable_type,
    units_name := NEW.units_name,
    units_type := NEW.units_type,
    units_description := NEW.units_description,
    value := NEW.value,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_tree_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_tree_observations(
    tree_observations_id_in := NEW.tree_observations_id,
    canopy_level_in := NEW.canopy_level,
    crown_poly_kml_in := NEW.crown_poly_kml,
    tree_loc_long_in := NEW.tree_loc_long,
    tree_loc_lat_in := NEW.tree_loc_lat,
    variable_name_in := NEW.variable_name,
    variable_type_in := NEW.variable_type,
    units_name_in := NEW.units_name,
    units_type_in := NEW.units_type,
    units_description_in := NEW.units_description,
    value_in := NEW.value
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_tree_observations_id(
  canopy_level_in TREE_CANOPY_LEVEL,
  crown_poly_kml_in TEXT,
  tree_loc_long_in FLOAT,
  tree_loc_lat_in FLOAT,
  variable_name_in text,
  variable_type_in text,
  units_name_in text,
  units_type_in text,
  units_description_in text,
  value_in FLOAT
) RETURNS UUID AS $$
DECLARE
  tid UUID;
  tdid UUID;
  vid UUID;
  uid UUID;
BEGIN

  SELECT get_tree_data_id(canopy_level_in, crown_poly_kml_in, tree_loc_long_in, tree_loc_lat_in) INTO tdid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  SELECT
    tree_observations_id INTO tid
  FROM
    tree_observations t
  WHERE
    tree_data_id = tdid AND
    variables_id = vid AND
    units_id = uid AND
    value = value_in;

  IF (tid IS NULL) THEN
    RAISE EXCEPTION 'Unknown tree_observations: canopy_level="%" crown_poly_kml="%" tree_loc_long="%" tree_loc_lat="%"
    variable_name="%" variable_type="%" units_name="%" units_type="%" units_description="%" value="%"',
    canopy_level_in, crown_poly_kml_in, tree_loc_long_in, tree_loc_lat_in, variable_name_in, variable_type_in,
    units_name_in, units_type_in, units_description_in, value_in;
  END IF;

  RETURN tid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER tree_observations_insert_trig
  INSTEAD OF INSERT ON
  tree_observations_view FOR EACH ROW
  EXECUTE PROCEDURE insert_tree_observations_from_trig();

CREATE TRIGGER tree_observations_update_trig
  INSTEAD OF UPDATE ON
  tree_observations_view FOR EACH ROW
  EXECUTE PROCEDURE update_tree_observations_from_trig();
