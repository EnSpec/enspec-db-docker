-- TABLE
DROP TABLE IF EXISTS installations CASCADE;
CREATE TABLE installations (
  installations_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  install_date DATE NOT NULL,
  removal_date DATE,
  dir_location TEXT NOT NULL
);
CREATE INDEX installations_source_id_idx ON installations(source_id);

-- VIEW
CREATE OR REPLACE VIEW installations_view AS
  SELECT
    i.installations_id AS installations_id,
    i.install_date  as install_date,
    i.removal_date  as removal_date,
    i.dir_location  as dir_location,

    sc.name AS source_name
  FROM
    installations i
LEFT JOIN source sc ON i.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_installations (
  installations_id UUID,
  install_date DATE,
  removal_date DATE,
  dir_location TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( installations_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO installations_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO installations (
    installations_id, install_date, removal_date, dir_location, source_id
  ) VALUES (
    installations_id, install_date, removal_date, dir_location, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_installations (
  installations_id_in UUID,
  install_date_in DATE,
  removal_date_in DATE,
  dir_location_in TEXT) RETURNS void AS $$

BEGIN

  UPDATE installations SET (
    install_date, removal_date, dir_location
  ) = (
    install_date_in, removal_date_in, dir_location_in
  ) WHERE
    installations_id = installations_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_installations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_installations(
    installations_id := NEW.installations_id,
    install_date := NEW.install_date,
    removal_date := NEW.removal_date,
    dir_location := NEW.dir_location,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_installations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_installations(
    installations_id_in := NEW.installations_id,
    install_date_in := NEW.install_date,
    removal_date_in := NEW.removal_date,
    dir_location_in := NEW.dir_location
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_installations_id(install_date_in date, removal_date_in date, dir_location_in text) RETURNS UUID AS $$
DECLARE
  iid UUID;
BEGIN

  IF removal_date_in IS NULL THEN
    SELECT
      installations_id INTO iid
    FROM
      installations i
    WHERE
      install_date = install_date_in AND
      removal_date is NULL AND
      dir_location = dir_location_in;
  ELSE
    SELECT
      installations_id INTO iid
    FROM
      installations i
    WHERE
      install_date = install_date_in AND
      removal_date = removal_date_in AND
      dir_location = dir_location_in;
  END IF;

  IF (iid IS NULL) THEN
    RAISE EXCEPTION 'Unknown installations: install_date="%" removal_date="%" dir_location="%"', install_date_in, removal_date_in, dir_location_in;
  END IF;

  RETURN iid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER installations_insert_trig
  INSTEAD OF INSERT ON
  installations_view FOR EACH ROW
  EXECUTE PROCEDURE insert_installations_from_trig();

CREATE TRIGGER installations_update_trig
  INSTEAD OF UPDATE ON
  installations_view FOR EACH ROW
  EXECUTE PROCEDURE update_installations_from_trig();
