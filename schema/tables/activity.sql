-- TABLE
DROP TABLE IF EXISTS activity CASCADE;
DROP FUNCTION insert_activity(uuid,text,text,text);
DROP FUNCTION update_activity(uuid,text,text);
DROP FUNCTION get_activity_id(text,text);

CREATE TABLE activity (
  activity_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  activity TEXT UNIQUE NOT NULL,
  activity_description TEXT NOT NULL
);
CREATE INDEX activity_source_id_idx ON activity(source_id);

-- VIEW
CREATE OR REPLACE VIEW activity_view AS
  SELECT
    a.activity_id AS activity_id,
    a.activity  as activity,
    a.activity_description  as activity_description,

    sc.name AS source_name
  FROM
    activity a
LEFT JOIN source sc ON a.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_activity (
  activity_id UUID,
  activity TEXT,
  activity_description TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( activity_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO activity_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO activity (
    activity_id, activity, activity_description, source_id
  ) VALUES (
    activity_id, activity, activity_description, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_activity (
  activity_id_in UUID,
  activity_in TEXT,
  activity_description_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE activity SET (
    activity, activity_description
  ) = (
    activity_in, activity_description_in
  ) WHERE
    activity_id = activity_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_activity_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_activity(
    activity_id := NEW.activity_id,
    activity := NEW.activity,
    activity_description := NEW.activity_description,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_activity_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_activity(
    activity_id_in := NEW.activity_id,
    activity_in := NEW.activity,
    activity_description_in := NEW.activity_description
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_activity_id(
  activity_in TEXT,
  activity_description_in TEXT
) RETURNS UUID AS $$
DECLARE
  aid UUID;
BEGIN

  SELECT
    activity_id INTO aid
  FROM
    activity a
  WHERE
    activity = activity_in AND
    activity_description = activity_description_in;

  IF (aid IS NULL) THEN
    RAISE EXCEPTION 'Unknown activity: activity="%" activity_description="%"', activity_in, activity_description_in;
  END IF;

  RETURN aid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER activity_insert_trig
  INSTEAD OF INSERT ON
  activity_view FOR EACH ROW
  EXECUTE PROCEDURE insert_activity_from_trig();

CREATE TRIGGER activity_update_trig
  INSTEAD OF UPDATE ON
  activity_view FOR EACH ROW
  EXECUTE PROCEDURE update_activity_from_trig();
