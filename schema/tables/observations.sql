-- TABLE
DROP TABLE IF EXISTS observations CASCADE;
DROP FUNCTION update_observations(uuid,text,text,integer);
DROP FUNCTION get_observations_id(text,text,integer);
DROP FUNCTION insert_observations(uuid,text,text,integer,text);

CREATE TABLE observations (
  observations_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  observation_type TEXT NOT NULL,
  observation_subtype TEXT,
  value_precision INTEGER NOT NULL
);
CREATE INDEX observations_source_id_idx ON observations(source_id);

-- VIEW
CREATE OR REPLACE VIEW observations_view AS
  SELECT
    o.observations_id AS observations_id,
    o.observation_type  as observation_type,
    o.observation_subtype  as observation_subtype,
    o.value_precision  as value_precision,

    sc.name AS source_name
  FROM
    observations o
LEFT JOIN source sc ON o.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_observations (
  observations_id UUID,
  observation_type TEXT,
  observation_subtype TEXT,
  value_precision INTEGER,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( observations_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO observations_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO observations (
    observations_id, observation_type, observation_subtype, value_precision, source_id
  ) VALUES (
    observations_id, observation_type, observation_subtype, value_precision, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_observations (
  observations_id_in UUID,
  observation_type_in TEXT,
  observation_subtype_in TEXT,
  value_precision_in INTEGER) RETURNS void AS $$
BEGIN

  UPDATE observations SET (
    observation_type, observation_subtype, value_precision
  ) = (
    observation_type_in, observation_subtype_in, value_precision_in
  ) WHERE
    observations_id = observations_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_observations(
    observations_id := NEW.observations_id,
    observation_type := NEW.observation_type,
    observation_subtype := NEW.observation_subtype,
    value_precision := NEW.value_precision,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_observations(
    observations_id_in := NEW.observations_id,
    observation_type_in := NEW.observation_type,
    observation_subtype_in := NEW.observation_subtype,
    value_precision_in := NEW.value_precision
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_observations_id(
  observation_type_in TEXT,
  observation_subtype_in TEXT,
  value_precision_in INTEGER
) RETURNS UUID AS $$
DECLARE
  oid UUID;
BEGIN

  IF observation_subtype is NULL THEN
    SELECT
      observations_id INTO oid
    FROM
      observations o
    WHERE
      observation_type = observation_type_in AND
      observation_subtype is NULL AND
      value_precision = value_precision_in;
  ELSE
    SELECT
      observations_id INTO oid
    FROM
      observations o
    WHERE
      observation_type = observation_type_in AND
      observation_subtype = observation_subtype_in AND
      value_precision = value_precision_in;
  END IF;

  IF (oid IS NULL) THEN
    RAISE EXCEPTION 'Unknown observations: observation_type="%" observation_subtype="%" value_precision="%"', observation_type_in, observation_subtype_in, value_precision_in;
  END IF;

  RETURN oid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER observations_insert_trig
  INSTEAD OF INSERT ON
  observations_view FOR EACH ROW
  EXECUTE PROCEDURE insert_observations_from_trig();

CREATE TRIGGER observations_update_trig
  INSTEAD OF UPDATE ON
  observations_view FOR EACH ROW
  EXECUTE PROCEDURE update_observations_from_trig();
