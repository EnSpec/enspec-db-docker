-- TABLE
DROP TABLE IF EXISTS processing_metadata CASCADE;
-- DROP FUNCTION insert_processing_metadata(uuid,text,text,text,text,text,text,text,text,text,text,double precision,text);
-- DROP FUNCTION update_processing_metadata(uuid,text,text,text,text,text,text,text,text,text,text,double precision);
DROP FUNCTION get_processing_metadata_id(text,text,text,text,text,text,text,text,text,text,double precision);

CREATE TABLE processing_metadata (
  processing_metadata_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  processing_events_id UUID REFERENCES processing_events NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX processing_metadata_source_id_idx ON processing_metadata(source_id);
CREATE INDEX processing_metadata_processing_events_id_idx ON processing_metadata(processing_events_id);
CREATE INDEX processing_metadata_variables_id_idx ON processing_metadata(variables_id);
CREATE INDEX processing_metadata_units_id_idx ON processing_metadata(units_id);

-- VIEW
CREATE OR REPLACE VIEW processing_metadata_view AS
  SELECT
    p.processing_metadata_id AS processing_metadata_id,
    pe.system  as system,
    pe.software_version  as software_version,
    pe.job_type  as job_type,
    pe.input_dir  as input_dir,
    pe.proc_params  as proc_params,
    p.value  as value,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,

    sc.name AS source_name
  FROM
    processing_metadata p
LEFT JOIN source sc ON p.source_id = sc.source_id
LEFT JOIN processing_events pe ON p.processing_events_id = pe.processing_events_id
LEFT JOIN variables v ON p.variables_id = v.variables_id
LEFT JOIN units u ON p.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_processing_metadata (
  processing_metadata_id UUID,
  system TEXT,
  software_version TEXT,
  job_type TEXT,
  input_dir TEXT,
  proc_params TEXT,
  variable_name TEXT,
  variable_type TEXT,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,
  value FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  peid UUID;
  vid UUID;
  uid UUID;
BEGIN

  SELECT get_processing_events_id(system, software_version, job_type, input_dir, proc_params) INTO peid;
  SELECT get_variables_id(variable_name, variable_type) INTO vid;
  SELECT get_units_id(units_name, units_type, units_description) INTO uid;

  IF( processing_metadata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO processing_metadata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO processing_metadata (
    processing_metadata_id, processing_events_id, variable_id, unit_id, value, source_id
  ) VALUES (
    processing_metadata_id, peid, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_processing_metadata (
  processing_metadata_id_in UUID,
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT,
  variable_name_in TEXT,
  variable_type_in TEXT,
  units_name_in TEXT,
  units_type_in TEXT,
  units_description_in TEXT,
  value_in FLOAT) RETURNS void AS $$
DECLARE
peid UUID;
vid UUID;
uid UUID;

BEGIN
  SELECT get_processing_events_id(system_in, software_version_in, job_type_in, input_dir_in, proc_params_in) INTO peid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  UPDATE processing_metadata SET (
    processing_events_id, variable_id, unit_id, value
  ) = (
    processing_events_id_in, variable_id_in, unit_id_in, value_in
  ) WHERE
    processing_metadata_id = processing_metadata_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_processing_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_processing_metadata(
    processing_metadata_id := NEW.processing_metadata_id,
    system := NEW.system,
    software_version := NEW.software_version,
    job_type := NEW.job_type,
    input_dir := NEW.input_dir,
    proc_params := NEW.proc_params,
    variable_name := NEW.variable_name,
    variable_type := NEW.variable_type,
    units_name := NEW.units_name,
    units_type := NEW.units_type,
    units_description := NEW.units_description,
    value := NEW.value,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_processing_metadata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_processing_metadata(
    processing_metadata_id_in := NEW.processing_metadata_id,
    system_in := NEW.system,
    software_version_in := NEW.software_version,
    job_type_in := NEW.job_type,
    input_dir_in := NEW.input_dir,
    proc_params_in := NEW.proc_params,
    variable_name_in := NEW.variable_name,
    variable_type_in := NEW.variable_type,
    units_name_in := NEW.units_name,
    units_type_in := NEW.units_type,
    units_description_in := NEW.units_description,
    value_in := NEW.value
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_processing_metadata_id(
  system_in TEXT,
  software_version_in TEXT,
  job_type_in TEXT,
  input_dir_in TEXT,
  proc_params_in TEXT,
  variable_name_in text,
  variable_type_in text,
  units_name_in text,
  units_type_in text,
  units_description_in text,
  value_in float
) RETURNS UUID AS $$
DECLARE
  pid UUID;
  peid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_processing_events_id(system_in, software_version_in, job_type_in, input_dir_in, proc_params_in) INTO peid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  SELECT
    processing_metadata_id INTO pid
  FROM
    processing_metadata p
  WHERE
    processing_events_id = peid AND
    variables_id = vid AND
    units_id = uid;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown processing_metadata: system="%" software_version="%" job_type="%" input_dir="%" proc_params="%"
    variable_name="%" variable_type="%" units_name="%" units_type="%" units_description="%" value="%"', system_in, software_version_in, job_type_in, input_dir_in,
    proc_params_in, variable_name_in, variable_type_in, units_name_in, units_type_in, units_description_in, value_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER processing_metadata_insert_trig
  INSTEAD OF INSERT ON
  processing_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE insert_processing_metadata_from_trig();

CREATE TRIGGER processing_metadata_update_trig
  INSTEAD OF UPDATE ON
  processing_metadata_view FOR EACH ROW
  EXECUTE PROCEDURE update_processing_metadata_from_trig();
