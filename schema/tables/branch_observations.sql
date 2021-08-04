-- TABLE
DROP TABLE IF EXISTS branch_observations CASCADE;
CREATE TABLE branch_observations (
  branch_observations_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  branch_data_id UUID REFERENCES branch_data NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX branch_observations_source_id_idx ON branch_observations(source_id);
CREATE INDEX branch_observations_branch_data_id_idx ON branch_observations(branch_data_id);
CREATE INDEX branch_observations_variables_id_idx ON branch_observations(variables_id);
CREATE INDEX branch_observations_units_id_idx ON branch_observations(units_id);

-- VIEW
CREATE OR REPLACE VIEW branch_observations_view AS
  SELECT
    b.branch_observations_id AS branch_observations_id,
    bd.branch_position  as branch_position,
    bd.branch_exposure  as branch_exposure,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,
    b.value  as value,

    sc.name AS source_name
  FROM
    branch_observations b
LEFT JOIN source sc ON b.source_id = sc.source_id
LEFT JOIN branch_data bd ON b.branch_data_id = bd.branch_data_id
LEFT JOIN variables v ON b.variables_id = v.variables_id
LEFT JOIN units u ON b.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_branch_observations (
  branch_observations_id UUID,
  branch_position BRANCHPOSITION,
  branch_exposure BRANCHEXPOSURE,
  variable_name TEXT,
  variable_type TEXT,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,
  value FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  bdid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_branch_data_id(branch_position, branch_exposure) INTO bdid;
  SELECT get_variables_id(variable_name, variable_type) INTO vid;
  SELECT get_units_id(units_name, units_type, units_description) INTO uid;
  IF( branch_observations_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO branch_observations_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO branch_observations (
    branch_observations_id, branch_data_id, variables_id, units_id, value, source_id
  ) VALUES (
    branch_observations_id, bdid, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_branch_observations (
  branch_observations_id_in UUID,
  branch_position BRANCHPOSITION,
  branch_exposure BRANCHEXPOSURE,
  variable_name_in TEXT,
  variable_type_in TEXT,
  units_name_in TEXT,
  units_type_in TEXT,
  units_description_in TEXT,
  value_in FLOAT) RETURNS void AS $$
DECLARE
bdid UUID;
vid UUID;
uid UUID;
BEGIN
  SELECT get_branch_data_id(branch_position_in, branch_exposure_in) INTO bdid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;

  UPDATE branch_observations SET (
    branch_data_id, variables_id, units_id, value
  ) = (
    bdid, vid, uid, value_in
  ) WHERE
    branch_observations_id = branch_observations_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_branch_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_branch_observations(
    branch_observations_id := NEW.branch_observations_id,
    branch_position := NEW.branch_position,
    branch_exposure := NEW.branch_exposure,
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

CREATE OR REPLACE FUNCTION update_branch_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_branch_observations(
    branch_observations_id_in := NEW.branch_observations_id,
    branch_position_in := NEW.branch_position,
    branch_exposure_in := NEW.branch_exposure,
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
CREATE OR REPLACE FUNCTION get_branch_observations_id(
  branch_position_in branchposition,
  branch_exposure_in branchexposure,
  variable_name_in text,
  variable_type_in text,
  units_name_in text,
  units_type_in text,
  units_description_in text,
  value_in FLOAT
) RETURNS UUID AS $$
DECLARE
  bid UUID;
  bdid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_branch_data_id(branch_position_in, branch_exposure_in) INTO bdid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  SELECT
    branch_observations_id INTO bid
  FROM
    branch_observations b
  WHERE
    branch_data_id = bdid AND
    variables_id = vid AND
    units_id = uid AND
    value = value_in;

  IF (bid IS NULL) THEN
    RAISE EXCEPTION 'Unknown branch_observations: branch_position="%" branch_exposure="%"
    variable_name="%" variable_type="%" units_name="%" units_type="%" units_description="%" value="%"',
    branch_position_in, branch_exposure_in, variable_name_in, variable_type_in,
    units_name_in, units_type_in, units_description_in, value_in;
  END IF;

  RETURN bid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER branch_observations_insert_trig
  INSTEAD OF INSERT ON
  branch_observations_view FOR EACH ROW
  EXECUTE PROCEDURE insert_branch_observations_from_trig();

CREATE TRIGGER branch_observations_update_trig
  INSTEAD OF UPDATE ON
  branch_observations_view FOR EACH ROW
  EXECUTE PROCEDURE update_branch_observations_from_trig();
