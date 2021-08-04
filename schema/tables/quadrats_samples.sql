-- TABLE
DROP TABLE IF EXISTS quadrats_samples CASCADE;
CREATE TABLE quadrats_samples (
  quadrats_samples_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  quadrats_id UUID REFERENCES quadrats NOT NULL,
  samples_id UUID REFERENCES samples NOT NULL
);
CREATE INDEX quadrats_samples_source_id_idx ON quadrats_samples(source_id);
CREATE INDEX quadrats_samples_quadrats_id_idx ON quadrats_samples(quadrats_id);
CREATE INDEX quadrats_samples_samples_id_idx ON quadrats_samples(samples_id);

-- VIEW
CREATE OR REPLACE VIEW quadrats_samples_view AS
  SELECT
    q.quadrats_samples_id AS quadrats_samples_id,
    qua.quadrat_name  as quadrat_name,
    qua.sampled  as sampled,
    qua.sampling_method  as sampling_method,
    ST_AsKML(qua.quadrat_geom)  as quadrat_geom_kml,
    s.sample_alive  as sample_alive,
    s.physical_storage  as physical_storage,
    s.sample_notes  as sample_notes,

    sc.name AS source_name
  FROM
    quadrats_samples q
LEFT JOIN source sc ON q.source_id = sc.source_id
LEFT JOIN quadrats qua ON q.quadrats_id = qua.quadrats_id
LEFT JOIN samples s ON q.samples_id = s.samples_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_quadrats_samples (
  quadrats_samples_id UUID,
  quadrat_name TEXT,
  sampled BOOL,
  sampling_method TEXT,
  quadrat_geom_kml TEXT,
  sample_alive BOOL,
  physical_storage SAMPLE_STORAGE,
  sample_notes TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  quaid UUID;
  sid UUID;
BEGIN

  IF( quadrats_samples_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO quadrats_samples_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_quadrats_id(quadrat_name, sampled, sampling_method, quadrat_geom_kml) INTO quaid;
  SELECT get_samples_id(sample_alive, physical_storage, sample_notes) INTO sid;

  INSERT INTO quadrats_samples (
    quadrats_samples_id, quadrats_id, samples_id, source_id
  ) VALUES (
    quadrats_samples_id, quaid, sid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_quadrats_samples (
  quadrats_samples_id_in UUID,
  quadrat_name_in TEXT,
  sampled_in BOOL,
  sampling_method_in TEXT,
  quadrat_geom_kml_in TEXT,
  sample_alive_in BOOL,
  physical_storage_in SAMPLE_STORAGE,
  sample_notes_in TEXT
  ) RETURNS void AS $$
DECLARE
quaid UUID;
sid UUID;

BEGIN

  SELECT get_quadrats_id(quadrat_name_in, sampled_in, sampling_method_in, quadrat_geom_kml_in) INTO quaid;
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO sid;
  UPDATE quadrats_samples SET (
    quadrats_id, samples_id
  ) = (
    quaid, sid
  ) WHERE
    quadrats_samples_id = quadrats_samples_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_quadrats_samples_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_quadrats_samples(
    quadrats_samples_id := NEW.quadrats_samples_id,
    quadrat_name := NEW.quadrat_name,
    sampled := NEW.sampled,
    sampling_method := NEW.sampling_method,
    quadrat_geom_kml := NEW.quadrat_geom_kml,
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

CREATE OR REPLACE FUNCTION update_quadrats_samples_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_quadrats_samples(
    quadrats_samples_id_in := NEW.quadrats_samples_id,
    quadrat_name_in := NEW.quadrat_name,
    sampled_in := NEW.sampled,
    sampling_method_in := NEW.sampling_method,
    quadrat_geom_kml_in := NEW.quadrat_geom_kml,
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
CREATE OR REPLACE FUNCTION get_quadrats_samples_id(
  quadrat_name_in TEXT,
  sampled_in BOOL,
  sampling_method_in TEXT,
  quadrat_geom_kml_in TEXT,
  sample_alive_in bool,
  physical_storage_in sample_storage,
  sample_notes_in text
) RETURNS UUID AS $$
DECLARE
  qid UUID;
  quaid UUID;
  sid UUID;
BEGIN
  SELECT get_quadrats_id(quadrat_name_in, sampled_in, sampling_method_in, quadrat_geom_kml_in) INTO quaid;
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO sid;
  SELECT
    quadrats_samples_id INTO qid
  FROM
    quadrats_samples q
  WHERE
    quadrats_id = quaid AND
    samples_id = sid;

  IF (qid IS NULL) THEN
    RAISE EXCEPTION 'Unknown quadrats_samples: quadrat_name="%" sampled="%" sampling_method="%" quadrat_geom_kml="%"
    sample_alive="%" physical_storage="%" sample_notes="%"', quadrat_name_in, sampled_in, sampling_method_in, quadrat_geom_kml_in,
    sample_alive_in, physical_storage_in, sample_notes_in;
  END IF;

  RETURN qid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER quadrats_samples_insert_trig
  INSTEAD OF INSERT ON
  quadrats_samples_view FOR EACH ROW
  EXECUTE PROCEDURE insert_quadrats_samples_from_trig();

CREATE TRIGGER quadrats_samples_update_trig
  INSTEAD OF UPDATE ON
  quadrats_samples_view FOR EACH ROW
  EXECUTE PROCEDURE update_quadrats_samples_from_trig();
