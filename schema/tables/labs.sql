-- TABLE
DROP TABLE IF EXISTS labs CASCADE;
CREATE TABLE labs (
  labs_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  lab_name TEXT UNIQUE NOT NULL
);
CREATE INDEX labs_source_id_idx ON labs(source_id);

-- VIEW
CREATE OR REPLACE VIEW labs_view AS
  SELECT
    l.labs_id AS labs_id,
    l.lab_name  as lab_name,

    sc.name AS source_name
  FROM
    labs l
LEFT JOIN source sc ON l.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_labs (
  labs_id UUID,
  lab_name TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( labs_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO labs_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO labs (
    labs_id, lab_name, source_id
  ) VALUES (
    labs_id, lab_name, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_labs (
  labs_id_in UUID,
  lab_name_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE labs SET (
    lab_name
  ) = (
    lab_name_in
  ) WHERE
    labs_id = labs_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_labs_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_labs(
    labs_id := NEW.labs_id,
    lab_name := NEW.lab_name,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_labs_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_labs(
    labs_id_in := NEW.labs_id,
    lab_name_in := NEW.lab_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_labs_id(lab_name_in text) RETURNS UUID AS $$
DECLARE
  lid UUID;
BEGIN

  SELECT
    labs_id INTO lid
  FROM
    labs l
  WHERE
    lab_name = lab_name_in;

  IF (lid IS NULL) THEN
    RAISE EXCEPTION 'Unknown labs: lab_name="%"', lab_name_in;
  END IF;

  RETURN lid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER labs_insert_trig
  INSTEAD OF INSERT ON
  labs_view FOR EACH ROW
  EXECUTE PROCEDURE insert_labs_from_trig();

CREATE TRIGGER labs_update_trig
  INSTEAD OF UPDATE ON
  labs_view FOR EACH ROW
  EXECUTE PROCEDURE update_labs_from_trig();
