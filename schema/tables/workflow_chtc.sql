-- TABLE
DROP TABLE IF EXISTS workflow_chtc CASCADE;
CREATE TABLE workflow_chtc (
  workflow_chtc_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  chtc_batch_name TEXT NOT NULL,
  chtc_process TEXT NOT NULL,
  hypro_version TEXT NOT NULL,
  workflow_version TEXT NOT NULL
);
CREATE INDEX workflow_chtc_source_id_idx ON workflow_chtc(source_id);

-- VIEW
CREATE OR REPLACE VIEW workflow_chtc_view AS
  SELECT
    w.workflow_chtc_id AS workflow_chtc_id,
    w.chtc_batch_name  as chtc_batch_name,
    w.chtc_process  as chtc_process,
    w.hypro_version  as hypro_version,
    w.workflow_version  as workflow_version,

    sc.name AS source_name
  FROM
    workflow_chtc w
LEFT JOIN source sc ON w.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_workflow_chtc (
  workflow_chtc_id UUID,
  chtc_batch_name TEXT,
  chtc_process TEXT,
  hypro_version TEXT,
  workflow_version TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( workflow_chtc_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO workflow_chtc_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO workflow_chtc (
    workflow_chtc_id, chtc_batch_name, chtc_process, hypro_version, workflow_version, source_id
  ) VALUES (
    workflow_chtc_id, chtc_batch_name, chtc_process, hypro_version, workflow_version, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_workflow_chtc (
  workflow_chtc_id_in UUID,
  chtc_batch_name_in TEXT,
  chtc_process_in TEXT,
  hypro_version_in TEXT,
  workflow_version_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE workflow_chtc SET (
    chtc_batch_name, chtc_process, hypro_version, workflow_version
  ) = (
    chtc_batch_name_in, chtc_process_in, hypro_version_in, workflow_version_in
  ) WHERE
    workflow_chtc_id = workflow_chtc_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_workflow_chtc_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_workflow_chtc(
    workflow_chtc_id := NEW.workflow_chtc_id,
    chtc_batch_name := NEW.chtc_batch_name,
    chtc_process := NEW.chtc_process,
    hypro_version := NEW.hypro_version,
    workflow_version := NEW.workflow_version,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_workflow_chtc_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_workflow_chtc(
    workflow_chtc_id_in := NEW.workflow_chtc_id,
    chtc_batch_name_in := NEW.chtc_batch_name,
    chtc_process_in := NEW.chtc_process,
    hypro_version_in := NEW.hypro_version,
    workflow_version_in := NEW.workflow_version
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_workflow_chtc_id(
  chtc_batch_name_in TEXT,
  chtc_process_in TEXT,
  hypro_version_in TEXT,
  workflow_version_in TEXT
) RETURNS UUID AS $$
DECLARE
  wid UUID;
BEGIN

  SELECT
    workflow_chtc_id INTO wid
  FROM
    workflow_chtc w
  WHERE
    chtc_batch_name = chtc_batch_name_in AND
    chtc_process = chtc_process_in AND
    hypro_version = hypro_version_in AND
    workflow_version = workflow_version_in;

  IF (wid IS NULL) THEN
    RAISE EXCEPTION 'Unknown workflow_chtc: chtc_batch_name="%" chtc_process="%" hypro_version="%" workflow_version="%"',
    chtc_batch_name_in, chtc_process_in, hypro_version_in, workflow_version_in;
  END IF;

  RETURN wid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER workflow_chtc_insert_trig
  INSTEAD OF INSERT ON
  workflow_chtc_view FOR EACH ROW
  EXECUTE PROCEDURE insert_workflow_chtc_from_trig();

CREATE TRIGGER workflow_chtc_update_trig
  INSTEAD OF UPDATE ON
  workflow_chtc_view FOR EACH ROW
  EXECUTE PROCEDURE update_workflow_chtc_from_trig();
