-- TABLE
DROP TABLE IF EXISTS specifications CASCADE;
CREATE TABLE specifications (
  specifications_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  name TEXT NOT NULL,
  value_ranges TEXT,
  other_details TEXT
);
CREATE INDEX specifications_source_id_idx ON specifications(source_id);

-- VIEW
CREATE OR REPLACE VIEW specifications_view AS
  SELECT
    s.specifications_id AS specifications_id,
    s.name as name,
    s.value_ranges as value_ranges,
    s.other_details as other_details,

    sc.name AS source_name
  FROM
    specifications s
LEFT JOIN source sc ON s.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_specifications (
  specifications_id UUID,
  name TEXT,
  value_ranges TEXT,
  other_details TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( specifications_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO specifications_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO specifications (
    specifications_id, name, value_ranges, other_details, source_id
  ) VALUES (
    specifications_id, name, value_ranges, other_details, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_specifications (
  specifications_id_in UUID,
  name_in TEXT,
  value_ranges_in TEXT,
  other_details_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE specifications SET (
    name, value_ranges, other_details
  ) = (
    name_in, value_ranges_in, other_details_in
  ) WHERE
    specifications_id = specifications_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_specifications_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_specifications(
    specifications_id := NEW.specifications_id,
    name := NEW.name,
    value_ranges := NEW.value_ranges,
    other_details := NEW.other_details,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_specifications_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_specifications(
    specifications_id_in := NEW.specifications_id,
    name_in := NEW.name,
    value_ranges_in := NEW.value_ranges,
    other_details_in := NEW.other_details
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_specifications_id(name_in text, value_ranges_in text) RETURNS UUID AS $$
DECLARE
  sid UUID;
BEGIN

  IF value_ranges_in IS NULL THEN
    SELECT
      specifications_id INTO sid
    FROM
      specifications s
    WHERE
      name = name_in;
  ELSE
    SELECT
      specifications_id INTO sid
    FROM
      specifications s
    WHERE
      name = name_in AND
      value_ranges = value_ranges_in;
  END IF;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown specifications: name="%"', name_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER specifications_insert_trig
  INSTEAD OF INSERT ON
  specifications_view FOR EACH ROW
  EXECUTE PROCEDURE insert_specifications_from_trig();

CREATE TRIGGER specifications_update_trig
  INSTEAD OF UPDATE ON
  specifications_view FOR EACH ROW
  EXECUTE PROCEDURE update_specifications_from_trig();
