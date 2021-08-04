-- TABLE
DROP TABLE IF EXISTS samples CASCADE;
CREATE TABLE samples (
  samples_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  sample_alive BOOL NOT NULL,
  physical_storage SAMPLE_STORAGE NOT NULL,
  sample_notes TEXT NOT NULL
);
CREATE INDEX samples_source_id_idx ON samples(source_id);

-- VIEW
CREATE OR REPLACE VIEW samples_view AS
  SELECT
    s.samples_id AS samples_id,
    s.sample_alive  as sample_alive,
    s.physical_storage  as physical_storage,
    s.sample_notes  as sample_notes,

    sc.name AS source_name
  FROM
    samples s
LEFT JOIN source sc ON s.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_samples (
  samples_id UUID,
  sample_alive BOOL,
  physical_storage SAMPLE_STORAGE,
  sample_notes TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( samples_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO samples_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO samples (
    samples_id, sample_alive, physical_storage, sample_notes, source_id
  ) VALUES (
    samples_id, sample_alive, physical_storage, sample_notes, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_samples (
  samples_id_in UUID,
  sample_alive_in BOOL,
  physical_storage_in SAMPLE_STORAGE,
  sample_notes_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE samples SET (
    sample_alive, physical_storage, sample_notes
  ) = (
    sample_alive_in, physical_storage_in, sample_notes_in
  ) WHERE
    samples_id = samples_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_samples_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_samples(
    samples_id := NEW.samples_id,
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

CREATE OR REPLACE FUNCTION update_samples_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_samples(
    samples_id_in := NEW.samples_id,
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
CREATE OR REPLACE FUNCTION get_samples_id(sample_alive_in bool, physical_storage_in sample_storage, sample_notes_in text) RETURNS UUID AS $$
DECLARE
  sid UUID;
BEGIN

  SELECT
    samples_id INTO sid
  FROM
    samples s
  WHERE
    sample_alive = sample_alive_in AND
    physical_storage = physical_storage_in AND
    sample_notes = sample_notes_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown samples: sample_alive="%" physical_storage="%" sample_notes="%"', sample_alive_in, physical_storage_in, sample_notes_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER samples_insert_trig
  INSTEAD OF INSERT ON
  samples_view FOR EACH ROW
  EXECUTE PROCEDURE insert_samples_from_trig();

CREATE TRIGGER samples_update_trig
  INSTEAD OF UPDATE ON
  samples_view FOR EACH ROW
  EXECUTE PROCEDURE update_samples_from_trig();
