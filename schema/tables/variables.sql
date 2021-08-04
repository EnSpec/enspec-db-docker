-- TABLE
DROP TABLE IF EXISTS variables CASCADE;
CREATE TABLE variables (
  variables_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  variable_name TEXT UNIQUE NOT NULL,
  variable_type TEXT
);
CREATE INDEX variables_source_id_idx ON variables(source_id);

-- VIEW
CREATE OR REPLACE VIEW variables_view AS
  SELECT
    v.variables_id AS variables_id,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    sc.name AS source_name
  FROM
    variables v
LEFT JOIN source sc ON v.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_variables (
  variables_id UUID,
  variable_name TEXT,
  variable_type TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( variables_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO variables_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO variables (
    variables_id, variable_name, variable_type, source_id
  ) VALUES (
    variables_id, variable_name, variable_type, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_variables (
  variables_id_in UUID,
  variable_name_in TEXT,
  variable_type_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE variables SET (
    variable_name, variable_type
  ) = (
    variable_name_in, variable_type_in
  ) WHERE
    variables_id = variables_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_variables_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_variables(
    variables_id := NEW.variables_id,
    variable_name := NEW.variable_name,
    variable_type := NEW.variable_type,
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_variables_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_variables(
    variables_id_in := NEW.variables_id,
    variable_name_in := NEW.variable_name,
    variable_type_in := NEW.variable_type
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_variables_id(variable_name_in text, variable_type_in text) RETURNS UUID AS $$
DECLARE
  vid UUID;
BEGIN
  IF variable_type_in IS NULL THEN
    SELECT
      variables_id INTO vid
    FROM
      variables v
    WHERE
      variable_name = variable_name_in AND
      variable_type IS NULL;
  ELSE
    SELECT
      variables_id INTO vid
    FROM
      variables v
    WHERE
      variable_name = variable_name_in AND
      variable_type = variable_type_in;
  END IF;

  IF (vid IS NULL) THEN
    RAISE EXCEPTION 'Unknown variables: variable_name="%" variable_type="%"', variable_name_in, variable_type_in;
  END IF;

  RETURN vid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER variables_insert_trig
  INSTEAD OF INSERT ON
  variables_view FOR EACH ROW
  EXECUTE PROCEDURE insert_variables_from_trig();

CREATE TRIGGER variables_update_trig
  INSTEAD OF UPDATE ON
  variables_view FOR EACH ROW
  EXECUTE PROCEDURE update_variables_from_trig();
