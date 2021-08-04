-- TABLE
DROP TABLE IF EXISTS sample_personnel CASCADE;
CREATE TABLE sample_personnel (
  sample_personnel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  samples_id UUID REFERENCES samples NOT NULL,
  personnel_id UUID REFERENCES personnel NOT NULL,
  activity_id UUID REFERENCES activity NOT NULL,
  activity_date_started DATE NOT NULL,
  activity_date_completed DATE,
  activity_lead TEXT NOT NULL
);
CREATE INDEX sample_personnel_source_id_idx ON sample_personnel(source_id);
CREATE INDEX sample_personnel_samples_id_idx ON sample_personnel(samples_id);
CREATE INDEX sample_personnel_personnel_id_idx ON sample_personnel(personnel_id);
CREATE INDEX sample_personnel_activity_id_idx ON sample_personnel(activity_id);

-- VIEW
CREATE OR REPLACE VIEW sample_personnel_view AS
  SELECT
    s.sample_personnel_id AS sample_personnel_id,
    sam.sample_alive  as sample_alive,
    sam.physical_storage  as physical_storage,
    sam.sample_notes  as sample_notes,
    per.personnel_name  as personnel_name,
    per.personnel_role  as personnel_role,
    per.organization  as organization,
    per.office_phone  as office_phone,
    per.cell_phone  as cell_phone,
    per.email  as email,
    per.personnel_notes  as personnel_notes,
    ac.activity  as activity,
    ac.activity_description  as activity_description,
    s.activity_date_started  as activity_date_started,
    s.activity_date_completed  as activity_date_completed,
    s.activity_lead  as activity_lead,

    sc.name AS source_name
  FROM
    sample_personnel s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN samples sam ON s.samples_id = sam.samples_id
LEFT JOIN personnel per ON s.personnel_id = per.personnel_id
LEFT JOIN activity ac ON s.activity_id = ac.activity_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_sample_personnel (
  sample_personnel_id UUID,
  sample_alive BOOL,
  physical_storage SAMPLE_STORAGE,
  sample_notes TEXT,
  personnel_name TEXT,
  personnel_role TEXT,
  organization TEXT,
  office_phone TEXT,
  cell_phone TEXT,
  email TEXT,
  personnel_notes TEXT,
  activity TEXT,
  activity_description TEXT,
  activity_date_started DATE,
  activity_date_completed DATE,
  activity_lead TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  samid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_samples_id(sample_alive, physical_storage, sample_notes) INTO samid;
  SELECT get_personnel_id(personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes) INTO perid;
  SELECT get_activity_id(activity, activity_description) INTO acid;

  IF( sample_personnel_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sample_personnel_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO sample_personnel (
    sample_personnel_id, samples_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead, source_id
  ) VALUES (
    sample_personnel_id, samid, perid, acid, activity_date_started, activity_date_completed, activity_lead, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sample_personnel (
  sample_personnel_id_in UUID,
  samples_id_in UUID,
  personnel_id_in UUID,
  activity_id_in UUID,
  activity_date_started_in DATE,
  activity_date_completed_in DATE,
  activity_lead_in TEXT) RETURNS void AS $$
DECLARE
samid UUID;
perid UUID;
acid UUID;
BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;
  UPDATE sample_personnel SET (
    samples_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead
  ) = (
    samid, perid, acid, activity_date_started_in, activity_date_completed_in, activity_lead_in
  ) WHERE
    sample_personnel_id = sample_personnel_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_sample_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_sample_personnel(
    sample_personnel_id := NEW.sample_personnel_id,
    sample_alive := NEW.sample_alive,
    physical_storage := NEW.physical_storage,
    sample_notes := NEW.sample_notes,
    personnel_name := NEW.personnel_name,
    personnel_role := NEW.personnel_role,
    organization := NEW.organization,
    office_phone := NEW.office_phone,
    cell_phone := NEW.cell_phone,
    email := NEW.email,
    personnel_notes := NEW.personnel_notes,
    activity := NEW.activity,
    activity_description := NEW.activity_description,
    activity_date_started := NEW.activity_date_started,
    activity_date_completed := NEW.activity_date_completed,
    activity_lead := NEW.activity_lead,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sample_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_sample_personnel(
    sample_personnel_id_in := NEW.sample_personnel_id,
    sample_alive_in := NEW.sample_alive,
    physical_storage_in := NEW.physical_storage,
    sample_notes_in := NEW.sample_notes,
    personnel_name_in := NEW.personnel_name,
    personnel_role_in := NEW.personnel_role,
    organization_in := NEW.organization,
    office_phone_in := NEW.office_phone,
    cell_phone_in := NEW.cell_phone,
    email_in := NEW.email,
    personnel_notes_in := NEW.personnel_notes,
    activity_in := NEW.activity,
    activity_description_in := NEW.activity_description,
    activity_date_started_in := NEW.activity_date_started,
    activity_date_completed_in := NEW.activity_date_completed,
    activity_lead_in := NEW.activity_lead
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_sample_personnel_id(
  sample_alive_in bool, physical_storage_in sample_storage, sample_notes_in text,
  personnel_name_in TEXT,
  personnel_role_in TEXT,
  organization_in TEXT,
  office_phone_in TEXT,
  cell_phone_in TEXT,
  email_in TEXT,
  personnel_notes_in TEXT,
  activity_in TEXT,
  activity_description_in TEXT,
  activity_date_started_in DATE,
  activity_date_completed_in DATE,
  activity_lead_in TEXT
) RETURNS UUID AS $$
DECLARE
  sid UUID;
  samid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  IF (activity_date_completed_in IS NULL) THEN
    SELECT
      sample_personnel_id INTO sid
    FROM
      sample_personnel s
    WHERE
      samples_id = samid AND
      personnel_id = perid AND
      activity_id = acid AND
      activity_date_started = activity_date_started_in AND
      activity_date_completed is NULL AND
      activity_lead = activity_lead_in;
  ELSE
    SELECT
      sample_personnel_id INTO sid
    FROM
      sample_personnel s
    WHERE
      samples_id = samid AND
      personnel_id = perid AND
      activity_id = acid AND
      activity_date_started = activity_date_started_in AND
      activity_date_completed = activity_date_completed_in AND
      activity_lead = activity_lead_in;

  END IF;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sample_personnel: sample_alive="%" physical_storage="%" sample_notes="%"
    personnel_name="%" personnel_role="%" organization="%" office_phone="%" cell_phone="%" email="%" personnel_notes="%"
    activity="%" activity_description="%" activity_date_started ="%" activity_date_completed="%" activity_lead="%"',
    personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in,
    activity_in, activity_description_in, activity_date_started_in, activity_date_completed_in, activity_lead_in,
    sample_alive_in, physical_storage_in, sample_notes_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER sample_personnel_insert_trig
  INSTEAD OF INSERT ON
  sample_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE insert_sample_personnel_from_trig();

CREATE TRIGGER sample_personnel_update_trig
  INSTEAD OF UPDATE ON
  sample_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE update_sample_personnel_from_trig();
