-- TABLE
DROP TABLE IF EXISTS branch_samples CASCADE;
CREATE TABLE branch_samples (
  branch_samples_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  branch_data_id UUID REFERENCES branch_data NOT NULL,
  samples_id UUID REFERENCES samples NOT NULL
);
CREATE INDEX branch_samples_source_id_idx ON branch_samples(source_id);
CREATE INDEX branch_samples_branch_data_id_idx ON branch_samples(branch_data_id);
CREATE INDEX branch_samples_samples_id_idx ON branch_samples(samples_id);

-- VIEW
CREATE OR REPLACE VIEW branch_samples_view AS
  SELECT
    b.branch_samples_id AS branch_samples_id,
    bd.branch_position  as branch_position,
    bd.branch_exposure  as branch_exposure,
    s.sample_alive  as sample_alive,
    s.physical_storage  as physical_storage,
    s.sample_notes  as sample_notes,

    sc.name AS source_name
  FROM
    branch_samples b
LEFT JOIN source sc ON b.source_id = sc.source_id
LEFT JOIN branch_data bd ON b.branch_data_id = bd.branch_data_id
LEFT JOIN samples s ON b.samples_id = s.samples_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_branch_samples (
  branch_samples_id UUID,
  branch_position BRANCHPOSITION,
  branch_exposure BRANCHEXPOSURE,
  sample_alive BOOL,
  physical_storage SAMPLE_STORAGE,
  sample_notes TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  bdid UUID;
  sid UUID;
BEGIN
  SELECT get_branch_data_id(branch_position, branch_exposure) INTO bdid;
  SELECT get_samples_id(sample_alive, physical_storage, sample_notes) INTO sid;

  IF( branch_samples_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO branch_samples_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO branch_samples (
    branch_samples_id, branch_data_id, samples_id, source_id
  ) VALUES (
    branch_samples_id, bdid, sid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_branch_samples (
  branch_samples_id_in UUID,
  branch_position_in BRANCHPOSITION,
  branch_exposure_in BRANCHEXPOSURE,
  sample_alive_in BOOL,
  physical_storage_in SAMPLE_STORAGE,
  sample_notes_in TEXT
  ) RETURNS void AS $$
DECLARE
bdid UUID;
sid UUID;
BEGIN
  SELECT get_branch_data_id(branch_position_in, branch_exposure_in) INTO bdid;
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO sid;
  UPDATE branch_samples SET (
    branch_data_id, samples_id
  ) = (
    bdid, sid
  ) WHERE
    branch_samples_id = branch_samples_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_branch_samples_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_branch_samples(
    branch_samples_id := NEW.branch_samples_id,
    branch_position := NEW.branch_position,
    branch_exposure := NEW.branch_exposure,
    sample_alive := NEW.sample_alive,
    physical_storage := NEW.physical_storage,
    sample_notes := NEW.sample_notes,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_branch_samples_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_branch_samples(
    branch_samples_id_in := NEW.branch_samples_id,
    branch_position_in := NEW.branch_position,
    branch_exposure_in := NEW.branch_exposure,
    sample_alive_in := NEW.sample_alive,
    physical_storage_in := NEW.physical_storage,
    sample_notes_in := NEW.sample_notes
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_branch_samples_id(
  branch_position_in branchposition,
  branch_exposure_in branchexposure,
  sample_alive_in bool,
  physical_storage_in sample_storage,
  sample_notes_in text
) RETURNS UUID AS $$
DECLARE
  bid UUID;
  bdid UUID;
  sid UUID;
BEGIN
  SELECT get_branch_data_id(branch_position_in, branch_exposure_in) INTO bdid;
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO sid;
  SELECT
    branch_samples_id INTO bid
  FROM
    branch_samples b
  WHERE
    branch_data_id = bdid AND
    samples_id = sid;

  IF (bid IS NULL) THEN
    RAISE EXCEPTION 'Unknown branch_samples: branch_position="%" branch_exposure="%" sample_alive="%" physical_storage="%" sample_notes="%"',
    branch_position_in, branch_exposure_in, sample_alive_in, physical_storage_in, sample_notes_in;
  END IF;

  RETURN bid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER branch_samples_insert_trig
  INSTEAD OF INSERT ON
  branch_samples_view FOR EACH ROW
  EXECUTE PROCEDURE insert_branch_samples_from_trig();

CREATE TRIGGER branch_samples_update_trig
  INSTEAD OF UPDATE ON
  branch_samples_view FOR EACH ROW
  EXECUTE PROCEDURE update_branch_samples_from_trig();
