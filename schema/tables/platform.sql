-- TABLE
DROP TABLE IF EXISTS platform CASCADE;
CREATE TABLE platform (
  platform_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  aircraft_sign TEXT NOT NULL,
  platform_type TEXT NOT NULL
);
CREATE INDEX platform_source_id_idx ON platform(source_id);

-- VIEW
CREATE OR REPLACE VIEW platform_view AS
  SELECT
    p.platform_id AS platform_id,
    p.aircraft_sign  as aircraft_sign,
    p.platform_type  as platform_type,

    sc.name AS source_name
  FROM
    platform p
LEFT JOIN source sc ON p.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_platform (
  platform_id UUID,
  aircraft_sign TEXT,
  platform_type TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( platform_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO platform_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO platform (
    platform_id, aircraft_sign, platform_type, source_id
  ) VALUES (
    platform_id, aircraft_sign, platform_type, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_platform (
  platform_id_in UUID,
  aircraft_sign_in TEXT,
  platform_type_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE platform SET (
    platform_id, aircraft_sign, platform_type
  ) = (
    platform_id_in, aircraft_sign_in, platform_type_in
  ) WHERE
    platform_id = platform_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_platform_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_platform(
    platform_id := NEW.platform_id,
    aircraft_sign := NEW.aircraft_sign,
    platform_type := NEW.platform_type,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_platform_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_platform(
    platform_id_in := NEW.platform_id,
    aircraft_sign_in := NEW.aircraft_sign,
    platform_type_in := NEW.platform_type
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_platform_id(aircraft_sign_in text, platform_type_in text) RETURNS UUID AS $$
DECLARE
  pid UUID;
BEGIN

  SELECT
    platform_id INTO pid
  FROM
    platform p
  WHERE
    aircraft_sign = aircraft_sign_in AND
    platform_type = platform_type_in;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown platform: aircraft_sign="%" platform_type="%"', aircraft_sign_in, platform_type_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER platform_insert_trig
  INSTEAD OF INSERT ON
  platform_view FOR EACH ROW
  EXECUTE PROCEDURE insert_platform_from_trig();

CREATE TRIGGER platform_update_trig
  INSTEAD OF UPDATE ON
  platform_view FOR EACH ROW
  EXECUTE PROCEDURE update_platform_from_trig();
