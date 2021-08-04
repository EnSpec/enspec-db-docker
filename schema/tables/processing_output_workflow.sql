-- TABLE
DROP TABLE IF EXISTS processing_output_workflow CASCADE;
CREATE TABLE processing_output_workflow (
  processing_output_workflow_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  processing_events_id UUID REFERENCES processing_events NOT NULL,
  image_output_id UUID REFERENCES image_output NOT NULL,
  workflow_chtc_id UUID REFERENCES workflow_chtc NOT NULL
);
CREATE INDEX processing_output_workflow_source_id_idx ON processing_output_workflow(source_id);
CREATE INDEX processing_output_workflow_processing_events_id_idx ON processing_output_workflow(processing_events_id);
CREATE INDEX processing_output_workflow_image_output_id_idx ON processing_output_workflow(image_output_id);
CREATE INDEX processing_output_workflow_workflow_chtc_id_idx ON processing_output_workflow(workflow_chtc_id);

-- VIEW
CREATE OR REPLACE VIEW processing_output_workflow_view AS
  SELECT
    p.processing_output_workflow_id AS processing_output_workflow_id,
    pe.system  as system,
    pe.software_version  as software_version,
    pe.job_type  as job_type,
    pe.input_dir  as input_dir,
    pe.proc_params  as proc_params,
    i.image_dir  as image_dir,
    i.image_dir_owner  as image_dir_owner,
    i.image_exists  as image_exists,
    i.processing_date  as processing_date,
    i.expiration_date  as expiration_date,
    i.expiration_type  as expiration_type,
    w.chtc_batch_name  as chtc_batch_name,
    w.chtc_process  as chtc_process,
    w.hypro_version  as hypro_version,
    w.workflow_version  as workflow_version,

    sc.name AS source_name
  FROM
    processing_output_workflow p
LEFT JOIN source sc ON p.source_id = sc.source_id
LEFT JOIN processing_events pe ON p.processing_events_id = pe.processing_events_id
LEFT JOIN image_output i ON p.image_output_id = i.image_output_id
LEFT JOIN workflow_chtc w ON p.workflow_chtc_id = w.workflow_chtc_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_processing_output_workflow (
  processing_output_workflow_id UUID,
  system TEXT,
  software_version TEXT,
  job_type TEXT,
  input_dir TEXT,
  proc_params TEXT,
  image_dir TEXT,
  image_dir_owner TEXT,
  image_exists BOOL,
  processing_date DATE,
  expiration_date DATE,
  expiration_type IMAGE_OUTPUT_EXPIRATION_TYPE,
  chtc_batch_name TEXT,
  chtc_process TEXT,
  hypro_version TEXT,
  workflow_version TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  peid UUID;
  ioid UUID;
  wid UUID;
BEGIN
 SELECT get_processing_events_id(system, software_version, job_type, input_dir, proc_params) INTO peid;
 SELECT get_image_output_id(image_dir, image_dir_owner, image_exists, processing_date, expiration_date, expiration_type) INTO ioid;
 SELECT get_workflow_chtc_id(chtc_batch_name, chtc_process, hypro_version, workflow_version) INTO wid;
  IF( processing_output_workflow_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO processing_output_workflow_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO processing_output_workflow (
    processing_output_workflow_id, processing_events_id, image_output_id, workflow_chtc_id, source_id
  ) VALUES (
    processing_output_workflow_id, peid, ioid, wid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_processing_output_workflow (
  processing_output_workflow_id_in UUID,
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT,
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE,
  chtc_batch_name_in TEXT,
  chtc_process_in TEXT,
  hypro_version_in TEXT,
  workflow_version_in TEXT
) RETURNS void AS $$
DECLARE
  peid_in UUID;
  ioid_in UUID;
  wid_in UUID;
BEGIN
  SELECT get_processing_events_id(system_in, software_version_in, job_type_in, input_dir_in, proc_params_in) INTO peid_in;
  SELECT get_image_output_id(image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in) INTO ioid_in;
  SELECT get_workflow_chtc_id(chtc_batch_name_in, chtc_process_in, hypro_version_in, workflow_version_in) INTO wid_in;

  UPDATE processing_output_workflow SET (
    processing_events_id, image_output_id, workflow_chtc_id
  ) = (
    peid_in, ioid_in, wid_in
  ) WHERE
    processing_output_workflow_id = processing_output_workflow_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_processing_output_workflow_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_processing_output_workflow(
    processing_output_workflow_id := NEW.processing_output_workflow_id,
    system := NEW.system,
    software_version := NEW.software_version,
    job_type := NEW.job_type,
    input_dir := NEW.input_dir,
    proc_params := NEW.proc_params,
    image_dir := NEW.image_dir,
    image_dir_owner := NEW.image_dir_owner,
    image_exists := NEW.image_exists,
    processing_date := NEW.processing_date,
    expiration_date := NEW.expiration_date,
    expiration_type := NEW.expiration_type,
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

CREATE OR REPLACE FUNCTION update_processing_output_workflow_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_processing_output_workflow(
    processing_output_workflow_id_in := NEW.processing_output_workflow_id,
    system_in := NEW.system,
    software_version_in := NEW.software_version,
    job_type_in := NEW.job_type,
    input_dir_in := NEW.input_dir,
    proc_params_in := NEW.proc_params,
    image_dir_in := NEW.image_dir,
    image_dir_owner_in := NEW.image_dir_owner,
    image_exists_in := NEW.image_exists,
    processing_date_in := NEW.processing_date,
    expiration_date_in := NEW.expiration_date,
    expiration_type_in := NEW.expiration_type,
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
CREATE OR REPLACE FUNCTION get_processing_output_workflow_id(
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT,
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE,
  chtc_batch_name_in TEXT,
  chtc_process_in TEXT,
  hypro_version_in TEXT,
  workflow_version_in TEXT
) RETURNS UUID AS $$
DECLARE
  pid UUID;
  peid_in UUID;
  ioid_in UUID;
  wid_in UUID;
BEGIN
  SELECT get_processing_events_id(system_in, software_version_in, job_type_in, input_dir_in, proc_params_in) INTO peid_in;
  SELECT get_image_output_id(image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in) INTO ioid_in;
  SELECT get_workflow_chtc_id(chtc_batch_name_in, chtc_process_in, hypro_version_in, workflow_version_in) INTO wid_in;

  SELECT
    processing_output_workflow_id INTO pid
  FROM
    processing_output_workflow p
  WHERE
    processing_events_id = peid_in AND
    image_output_id = ioid_in AND
    workflow_chtc_id = wid_in;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown processing_output_workflow: system="%" software_version="%" job_type="%" input_dir="%" proc_params="%"
    image_dir="%" image_dir_owner="%" image_exists="%" processing_date="%" expiration_date="%" expiration_type="%"
    chtc_batch_name="%" chtc_process="%" hypro_version="%" workflow_version="%"', system_in, software_version_in, job_type_in, input_dir_in, proc_params_in,
    image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in, chtc_batch_name_in, chtc_process_in, hypro_version_in, workflow_version_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER processing_output_workflow_insert_trig
  INSTEAD OF INSERT ON
  processing_output_workflow_view FOR EACH ROW
  EXECUTE PROCEDURE insert_processing_output_workflow_from_trig();

CREATE TRIGGER processing_output_workflow_update_trig
  INSTEAD OF UPDATE ON
  processing_output_workflow_view FOR EACH ROW
  EXECUTE PROCEDURE update_processing_output_workflow_from_trig();
