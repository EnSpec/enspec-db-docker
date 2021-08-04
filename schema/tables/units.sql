-- TABLE
DROP TABLE IF EXISTS units CASCADE;
DROP FUNCTION insert_units(uuid,text,text,text,text);
DROP FUNCTION update_units(uuid,text,text,text);
DROP FUNCTION get_units_id(text,text,text);

CREATE TABLE units (
  units_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  units_name TEXT NOT NULL,
  units_type TEXT NOT NULL,
  units_description TEXT,
  UNIQUE(units_name, units_type)
);
CREATE INDEX units_source_id_idx ON units(source_id);

-- VIEW
CREATE OR REPLACE VIEW units_view AS
  SELECT
    u.units_id AS units_id,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,
    sc.name AS source_name
  FROM
    units u
LEFT JOIN source sc ON u.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_units (
  units_id UUID,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( units_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO units_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO units (
    units_id, units_name, units_type, units_description, source_id
  ) VALUES (
    units_id, units_name, units_type, units_description, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_units (
  units_id_in UUID,
  units_name_in TEXT,
  units_type_in TEXT,
  units_description_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE units SET (
    units_name, units_type, units_description
  ) = (
    units_name_in, units_type_in, units_description_in
  ) WHERE
    units_id = units_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_units_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_units(
    units_id := NEW.units_id,
    units_name := NEW.units_name,
    units_type := NEW.units_type,
    units_description := NEW.units_description,
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_units_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_units(
    units_id_in := NEW.units_id,
    units_name_in := NEW.units_name,
    units_type_in := NEW.units_type,
    units_description_in := NEW.units_description
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_units_id(units_name_in text, units_type_in text, units_description_in text) RETURNS UUID AS $$
DECLARE
  uid UUID;
BEGIN
  IF units_description_in IS NULL THEN
    SELECT
      units_id INTO uid
    FROM
      units u
    WHERE
      units_name = units_name_in AND
      units_type = units_type_in AND
      units_description IS NULL;
  ELSE
    SELECT
      units_id INTO uid
    FROM
      units u
    WHERE
      units_name = units_name_in AND
      units_type = units_type_in AND
      units_description = units_description_in;
  END IF;

  IF (uid IS NULL) THEN
    RAISE EXCEPTION 'Unknown units: units_name="%" units_type="%" units_description="%"', units_name_in, units_type_in, units_description_in;
  END IF;

  RETURN uid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER units_insert_trig
  INSTEAD OF INSERT ON
  units_view FOR EACH ROW
  EXECUTE PROCEDURE insert_units_from_trig();

CREATE TRIGGER units_update_trig
  INSTEAD OF UPDATE ON
  units_view FOR EACH ROW
  EXECUTE PROCEDURE update_units_from_trig();
