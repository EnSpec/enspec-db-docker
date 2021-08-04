-- TABLE
DROP TABLE IF EXISTS tree_branch_data CASCADE;
CREATE TABLE tree_branch_data (
  tree_branch_data_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  tree_data_id UUID REFERENCES tree_data NOT NULL,
  branch_data_id UUID REFERENCES branch_data NOT NULL
);
CREATE INDEX tree_branch_data_source_id_idx ON tree_branch_data(source_id);
CREATE INDEX tree_branch_data_tree_data_id_idx ON tree_branch_data(tree_data_id);
CREATE INDEX tree_branch_data_branch_id_idx ON tree_branch_data(branch_data_id);

-- VIEW
CREATE OR REPLACE VIEW tree_branch_data_view AS
  SELECT
    t.tree_branch_data_id AS tree_branch_data_id,
    td.canopy_level  as canopy_level,
    td.crown_poly  as crown_poly,
    td.tree_location  as tree_location,
    b.branch_position  as branch_position,
    b.branch_exposure  as branch_exposure,

    sc.name AS source_name
  FROM
    tree_branch_data t
LEFT JOIN source sc ON t.source_id = sc.source_id
LEFT JOIN tree_data td ON t.tree_data_id = td.tree_data_id
LEFT JOIN branch_data b ON t.branch_data_id = b.branch_data_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_tree_branch_data (
  tree_branch_data_id UUID,
  canopy_level TREE_CANOPY_LEVEL,
  crown_poly GEOMETRY,
  tree_location GEOMETRY,
  branch_position BRANCHPOSITION,
  branch_exposure BRANCHEXPOSURE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  tdid UUID;
  bid UUID;
BEGIN

  IF( tree_branch_data_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO tree_branch_data_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_tree_data_id(canopy_level, crown_poly, tree_location) INTO tdid;
  SELECT get_branch_data_id(branch_position, branch_exposure) INTO bid;

  INSERT INTO tree_branch_data (
    tree_branch_data_id, tree_data_id, branch_data_id, source_id
  ) VALUES (
    tree_branch_data_id, tdid, bid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_tree_branch_data (
  tree_branch_data_id_in UUID,
  canopy_level_in TREE_CANOPY_LEVEL,
  crown_poly_in GEOMETRY,
  tree_location_in GEOMETRY,
  branch_position_in BRANCHPOSITION,
  branch_exposure_in BRANCHEXPOSURE
  ) RETURNS void AS $$
DECLARE
tdid UUID;
bid UUID;

BEGIN
  SELECT get_tree_data_id(canopy_level_in, crown_poly_in, tree_location_in) INTO tdid;
  SELECT get_branch_data_id(branch_position_in, branch_exposure_in) INTO bid;

  UPDATE tree_branch_data SET (
    tree_data_id, branch_data_id
  ) = (
    tdid, bid
  ) WHERE
    tree_branch_data_id = tree_branch_data_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_tree_branch_data_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_tree_branch_data(
    tree_branch_data_id := NEW.tree_branch_data_id,
    canopy_level := NEW.canopy_level,
    crown_poly := NEW.crown_poly,
    tree_location := NEW.tree_location,
    branch_position := NEW.branch_position,
    branch_exposure := NEW.branch_exposure,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_tree_branch_data_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_tree_branch_data(
    tree_branch_data_id_in := NEW.tree_branch_data_id,
    canopy_level_in := NEW.canopy_level,
    crown_poly_in := NEW.crown_poly,
    tree_location_in := NEW.tree_location,
    branch_position_in := NEW.branch_position,
    branch_exposure_in := NEW.branch_exposure
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_tree_branch_data_id(
  canopy_level_in TREE_CANOPY_LEVEL, crown_poly_in geometry, tree_location_in geometry,
  branch_position_in branchposition, branch_exposure_in branchexposure
) RETURNS UUID AS $$
DECLARE
  tid UUID;
  tdid UUID;
  bid UUID;
BEGIN
  SELECT get_tree_data_id(canopy_level_in, crown_poly_in, tree_location_in) INTO tdid;
  SELECT get_branch_data_id(branch_position_in, branch_exposure_in) INTO bid;

  SELECT
    tree_branch_data_id INTO tid
  FROM
    tree_branch_data t
  WHERE
    tree_data_id = tdid AND
    branch_data_id = bid;

  IF (tid IS NULL) THEN
    RAISE EXCEPTION 'Unknown tree_branch_data: canopy_level="%" crown_poly="%" tree_location="%" branch_position="%" branch_exposure="%"',
    canopy_level_in, crown_poly_in, tree_location_in, branch_position_in, branch_exposure_in;
  END IF;

  RETURN tid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER tree_branch_data_insert_trig
  INSTEAD OF INSERT ON
  tree_branch_data_view FOR EACH ROW
  EXECUTE PROCEDURE insert_tree_branch_data_from_trig();

CREATE TRIGGER tree_branch_data_update_trig
  INSTEAD OF UPDATE ON
  tree_branch_data_view FOR EACH ROW
  EXECUTE PROCEDURE update_tree_branch_data_from_trig();
