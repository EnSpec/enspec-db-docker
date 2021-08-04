-- TABLE
DROP TABLE IF EXISTS branch_data CASCADE;
CREATE TABLE branch_data (
  branch_data_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  branch_position BRANCHPOSITION,
  branch_exposure BRANCHEXPOSURE
);
CREATE INDEX branch_data_source_id_idx ON branch_data(source_id);

-- VIEW
CREATE OR REPLACE VIEW branch_data_view AS
  SELECT
    b.branch_data_id AS branch_data_id,
    b.branch_position  as branch_position,
    b.branch_exposure  as branch_exposure,

    sc.name AS source_name
  FROM
    branch_data b
LEFT JOIN source sc ON b.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_branch_data (
  branch_data_id UUID,
  branch_position BRANCHPOSITION,
  branch_exposure BRANCHEXPOSURE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( branch_data_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO branch_data_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO branch_data (
    branch_data_id, branch_position, branch_exposure, source_id
  ) VALUES (
    branch_data_id, branch_position, branch_exposure, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_branch_data (
  branch_data_id_in UUID,
  branch_position_in BRANCHPOSITION,
  branch_exposure_in BRANCHEXPOSURE) RETURNS void AS $$
BEGIN

  UPDATE branch_data SET (
    branch_position, branch_exposure
  ) = (
    branch_position_in, branch_exposure_in
  ) WHERE
    branch_data_id = branch_data_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_branch_data_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_branch_data(
    branch_data_id := NEW.branch_data_id,
    branch_position := NEW.branch_position,
    branch_exposure := NEW.branch_exposure,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_branch_data_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_branch_data(
    branch_data_id_in := NEW.branch_data_id,
    branch_position_in := NEW.branch_position,
    branch_exposure_in := NEW.branch_exposure
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_branch_data_id(branch_position_in branchposition, branch_exposure_in branchexposure) RETURNS UUID AS $$
DECLARE
  bid UUID;
BEGIN

  SELECT
    branch_data_id INTO bid
  FROM
    branch_data b
  WHERE
    branch_position = branch_position_in AND
    branch_exposure = branch_exposure_in;

  IF (bid IS NULL) THEN
    RAISE EXCEPTION 'Unknown branch_data: branch_position="%" branch_exposure="%"', branch_position_in, branch_exposure_in;
  END IF;

  RETURN bid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER branch_data_insert_trig
  INSTEAD OF INSERT ON
  branch_data_view FOR EACH ROW
  EXECUTE PROCEDURE insert_branch_data_from_trig();

CREATE TRIGGER branch_data_update_trig
  INSTEAD OF UPDATE ON
  branch_data_view FOR EACH ROW
  EXECUTE PROCEDURE update_branch_data_from_trig();
