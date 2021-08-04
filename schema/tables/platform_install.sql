-- TABLE
DROP TABLE IF EXISTS platform_install CASCADE;
CREATE TABLE platform_install (
  platform_install_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  platform_id UUID REFERENCES platform NOT NULL,
  installations_id UUID REFERENCES installations NOT NULL,
  UNIQUE(platform_id, installations_id)
);
CREATE INDEX platform_install_source_id_idx ON platform_install(source_id);
CREATE INDEX platform_install_platform_id_idx ON platform_install(platform_id);
CREATE INDEX platform_install_installations_id ON platform_install(installations_id);

-- VIEW
CREATE OR REPLACE VIEW platform_install_view AS
  SELECT
    p.platform_install_id AS platform_install_id,
    pf.aircraft_sign AS aircraft_sign,
    pf.platform_type AS platform_type,
    i.install_date AS install_date,
    i.removal_date AS removal_date,
    i.dir_location AS dir_location,
    sc.name AS source_name
  FROM
    platform_install p
LEFT JOIN source sc ON p.source_id = sc.source_id
LEFT JOIN platform pf ON p.platform_id = pf.platform_id
LEFT JOIN installations i ON p.installations_id = i.installations_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_platform_install (
  platform_install_id UUID,
  aircraft_sign TEXT,
  platform_type TEXT,
  install_date DATE,
  removal_date DATE,
  dir_location TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  pf UUID;
  i UUID;
BEGIN

  IF( platform_install_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO platform_install_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_platform_id(aircraft_sign, platform_type) INTO pf;
  SELECT get_installations_id(install_date, removal_date, removal_date, dir_location) INTO i;

  INSERT INTO platform_install (
    platform_install_id, platform_id, installations_id, source_id
  ) VALUES (
    platform_install_id, platform_id, installations_id, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_platform_install (
  platform_install_id_in UUID,
  aircraft_sign TEXT,
  platform_type TEXT,
  install_date DATE,
  removal_date DATE,
  dir_location TEXT) RETURNS void AS $$
DECLARE
  i UUID;
  pf UUID;

BEGIN
  SELECT get_platform_id(aircraft_sign, platform_type) INTO pf;
  SELECT get_installations_id(install_date, removal_date, removal_date, dir_location) INTO i;

  UPDATE platform_install SET (
    platform_id, installations_id
  ) = (
    pf, i
  ) WHERE
    platform_install_id = platform_install_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_platform_install_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_platform_install(
    platform_install_id := NEW.platform_install_id,
    aircraft_sign := NEW.aircraft_sign,
    platform_type := NEW.platform_type,
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

CREATE OR REPLACE FUNCTION update_platform_install_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_platform_install(
    platform_install_id_in := NEW.platform_install_id,
    aircraft_sign := NEW.aircraft_sign,
    platform_type := NEW.platform_type,
    install_date := NEW.install_date,
    removal_date := NEW.removal_date,
    dir_location := NEW.dir_location
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_platform_install_id(
  aircraft_sign text, platform_type text, install_date date, removal_date date, dir_location text) RETURNS UUID AS $$
DECLARE
  pid UUID;
  pf UUID;
  i UUID;
BEGIN
  SELECT get_platform_id(aircraft_sign, platform_type) INTO pf;
  SELECT get_installations_id(install_date, removal_date, removal_date, dir_location) INTO i;
  SELECT
    platform_install_id INTO pid
  FROM
    platform_install p
  WHERE
    platform_id = pf AND
    install_id = i;
  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown platform_install: aircraft_sign="%" platform_type="%" install_date="%" removal_date="%" removal_date="%" dir_location="%"', aircraft_sign, platform_type, install_date, removal_date, removal_date, dir_location;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER platform_install_insert_trig
  INSTEAD OF INSERT ON
  platform_install_view FOR EACH ROW
  EXECUTE PROCEDURE insert_platform_install_from_trig();

CREATE TRIGGER platform_install_update_trig
  INSTEAD OF UPDATE ON
  platform_install_view FOR EACH ROW
  EXECUTE PROCEDURE update_platform_install_from_trig();
