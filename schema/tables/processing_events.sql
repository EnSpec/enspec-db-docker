-- TABLE
DROP TABLE IF EXISTS processing_events CASCADE;
CREATE TABLE processing_events (
  processing_events_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  system TEXT NOT NULL,
  software_version TEXT NOT NULL,
  job_type TEXT NOT NULL,
  input_dir TEXT NOT NULL,
  proc_params TEXT NOT NULL
);
CREATE INDEX processing_events_source_id_idx ON processing_events(source_id);

-- VIEW
CREATE OR REPLACE VIEW processing_events_view AS
  SELECT
    p.processing_events_id AS processing_events_id,
    p.system  as system,
    p.software_version  as software_version,
    p.job_type  as job_type,
    p.input_dir  as input_dir,
    p.proc_params  as proc_params,

    sc.name AS source_name
  FROM
    processing_events p
LEFT JOIN source sc ON p.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_processing_events (
  processing_events_id UUID,
  system TEXT,
  software_version TEXT,
  job_type TEXT,
  input_dir TEXT,
  proc_params TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( processing_events_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO processing_events_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO processing_events (
    processing_events_id, system, software_version, job_type, input_dir, proc_params, source_id
  ) VALUES (
    processing_events_id, system, software_version, job_type, input_dir, proc_params, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_processing_events (
  processing_events_id_in UUID,
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE processing_events SET (
    system, software_version, job_type, input_dir, proc_params
  ) = (
    system_in, software_version_in, job_type_in, input_dir_in, proc_params_in
  ) WHERE
    processing_events_id = processing_events_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_processing_events_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_processing_events(
    processing_events_id := NEW.processing_events_id,
    system := NEW.system,
    software_version := NEW.software_version,
    job_type := NEW.job_type,
    input_dir := NEW.input_dir,
    proc_params := NEW.proc_params,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_processing_events_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_processing_events(
    processing_events_id_in := NEW.processing_events_id,
    system_in := NEW.system,
    software_version_in := NEW.software_version,
    job_type_in := NEW.job_type,
    input_dir_in := NEW.input_dir,
    proc_params_in := NEW.proc_params
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_processing_events_id(
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT
) RETURNS UUID AS $$
DECLARE
  pid UUID;
BEGIN

  SELECT
    processing_events_id INTO pid
  FROM
    processing_events p
  WHERE
    system = system_in AND
    software_version = software_version_in AND
    job_type = job_type_in AND
    input_dir = input_dir_in AND
    proc_params = proc_params_in;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown processing_events: system="%" software_version="%" job_type="%" input_dir="%" proc_params="%"',
    system_in, software_version_in, job_type_in, input_dir_in, proc_params_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER processing_events_insert_trig
  INSTEAD OF INSERT ON
  processing_events_view FOR EACH ROW
  EXECUTE PROCEDURE insert_processing_events_from_trig();

CREATE TRIGGER processing_events_update_trig
  INSTEAD OF UPDATE ON
  processing_events_view FOR EACH ROW
  EXECUTE PROCEDURE update_processing_events_from_trig();
