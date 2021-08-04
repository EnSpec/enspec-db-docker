-- TABLE
DROP TABLE IF EXISTS samples_measurements CASCADE;
CREATE TABLE samples_measurements (
  samples_measurements_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  samples_id UUID REFERENCES samples NOT NULL,
  labs_id UUID REFERENCES labs NOT NULL,
  measurements_id UUID REFERENCES measurements NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX samples_measurements_source_id_idx ON samples_measurements(source_id);
CREATE INDEX samples_measurements_samples_id_idx ON samples_measurements(samples_id);
CREATE INDEX samples_measurements_labs_id_idx ON samples_measurements(labs_id);
CREATE INDEX samples_measurements_measurements_id_idx ON samples_measurements(measurements_id);
CREATE INDEX samples_measurements_variables_id_idx ON samples_measurements(variables_id);
CREATE INDEX samples_measurements_units_id_idx ON samples_measurements(units_id);

-- VIEW
CREATE OR REPLACE VIEW samples_measurements_view AS
  SELECT
    s.samples_measurements_id AS samples_measurements_id,
    sam.sample_alive  as sample_alive,
    sam.physical_storage  as physical_storage,
    sam.sample_notes  as sample_notes,
    l.lab_name  as lab_name,
    m.measurement_type  as measurement_type,
    m.file_location  as file_location,
    m.assay  as assay,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,
    s.value  as value,

    sc.name AS source_name
  FROM
    samples_measurements s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN samples sam ON s.samples_id = sam.samples_id
LEFT JOIN labs l ON s.labs_id = l.labs_id
LEFT JOIN measurements m ON s.measurements_id = m.measurements_id
LEFT JOIN variables v ON s.variables_id = v.variables_id
LEFT JOIN units u ON s.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_samples_measurements (
  samples_measurements_id UUID,
  sample_alive BOOL,
  physical_storage SAMPLE_STORAGE,
  sample_notes TEXT,
  lab_name TEXT,
  measurement_type PHYSICAL_OR_CHEMICAL,
  file_location TEXT,
  assay TEXT,
  variable_name TEXT,
  variable_type TEXT,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,
  value FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  samid UUID;
  lid UUID;
  mid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_samples_id(sample_alive, physical_storage, sample_notes) INTO samid;
  SELECT get_labs_id(lab_name) INTO lid;
  SELECT get_measurements_id(measurement_type, file_location, assay) INTO mid;
  SELECT get_variables_id(variable_name, variable_type) INTO vid;
  SELECT get_units_id(units_name, units_type, units_description) INTO uid;

  IF( samples_measurements_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO samples_measurements_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO samples_measurements (
    samples_measurements_id, samples_id, labs_id, measurements_id, variables_id, units_id, value, source_id
  ) VALUES (
    samples_measurements_id, samid, lid, mid, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_samples_measurements (
  samples_measurements_id_in UUID,
  samples_id_in UUID,
  labs_id_in UUID,
  measurements_id_in UUID,
  variables_id_in UUID,
  units_id_in UUID,
  value_in FLOAT) RETURNS void AS $$
DECLARE
samid UUID;
lid UUID;
mid UUID;
vid UUID;
uid UUID;

BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_labs_id(lab_name_in) INTO lid;
  SELECT get_measurements_id(measurement_type_in, file_location_in, assay_in) INTO mid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;

  UPDATE samples_measurements SET (
    samples_id, labs_id, measurements_id, variables_id, units_id, value
  ) = (
    samid, lid, mid, vid, uid, value_in
  ) WHERE
    samples_measurements_id = samples_measurements_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_samples_measurements_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_samples_measurements(
    samples_measurements_id := NEW.samples_measurements_id,
    sample_alive := NEW.sample_alive,
    physical_storage := NEW.physical_storage,
    sample_notes := NEW.sample_notes,
    lab_name := NEW.lab_name,
    measurement_type := NEW.measurement_type,
    file_location := NEW.file_location,
    assay := NEW.assay,
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

CREATE OR REPLACE FUNCTION update_samples_measurements_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_samples_measurements(
    samples_measurements_id_in := NEW.samples_measurements_id,
    sample_alive_in := NEW.sample_alive,
    physical_storage_in := NEW.physical_storage,
    sample_notes_in := NEW.sample_notes,
    lab_name_in := NEW.lab_name,
    measurement_type_in := NEW.measurement_type,
    file_location_in := NEW.file_location,
    assay_in := NEW.assay,
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
CREATE OR REPLACE FUNCTION get_samples_measurements_id(
  sample_alive_in bool,
  physical_storage_in sample_storage,
  sample_notes_in text,
  lab_name_in text,
  measurement_type_in PHYSICAL_OR_CHEMICAL,
  file_location_in TEXT,
  assay_in TEXT,
  variable_name_in text,
  variable_type_in text,
  units_name_in text,
  units_type_in text,
  units_description_in text,
  value_in FLOAT
) RETURNS UUID AS $$
DECLARE
  sid UUID;
  samid UUID;
  lid UUID;
  mid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_labs_id(lab_name_in) INTO lid;
  SELECT get_measurements_id(measurement_type_in, file_location_in, assay_in) INTO mid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;

  SELECT
    samples_measurements_id INTO sid
  FROM
    samples_measurements s
  WHERE
    samples_id = samid AND
    labs_id = lid AND
    measurements_id = mid AND
    variables_id = vid AND
    units_id = uid AND
    value = value_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown samples_measurements: sample_alive="%" physical_storage="%" sample_notes="%" lab_name="%"
    measurement_type="%" file_location="%" assay="%" variable_name="%" variable_type="%" units_name="%" units_type="%"
    units_description="%" value="%"', sample_alive_in, physical_storage_in, sample_notes_in, lab_name_in,
    measurement_type_in, file_location_in, assay_in, variable_name_in, variable_type_in, units_name_in, units_type_in,
    units_description_in, value_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER samples_measurements_insert_trig
  INSTEAD OF INSERT ON
  samples_measurements_view FOR EACH ROW
  EXECUTE PROCEDURE insert_samples_measurements_from_trig();

CREATE TRIGGER samples_measurements_update_trig
  INSTEAD OF UPDATE ON
  samples_measurements_view FOR EACH ROW
  EXECUTE PROCEDURE update_samples_measurements_from_trig();
