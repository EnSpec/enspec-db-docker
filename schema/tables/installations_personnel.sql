-- TABLE
DROP TABLE IF EXISTS installations_personnel CASCADE;
CREATE TABLE installations_personnel (
  installations_personnel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  installations_id UUID REFERENCES installations NOT NULL,
  personnel_id UUID REFERENCES personnel NOT NULL,
  activity_id UUID REFERENCES activity NOT NULL,
  activity_date_started DATE NOT NULL,
  activity_date_completed DATE,
  activity_lead TEXT NOT NULL
);
CREATE INDEX installations_personnel_source_id_idx ON installations_personnel(source_id);
CREATE INDEX installations_personnel_installations_id_idx ON installations_personnel(installations_id);
CREATE INDEX installations_personnel_personnel_id_idx ON installations_personnel(personnel_id);
CREATE INDEX installations_personnel_activity_id_idx ON installations_personnel(activity_id);

-- VIEW
CREATE OR REPLACE VIEW installations_personnel_view AS
  SELECT
    i.installations_personnel_id AS installations_personnel_id,
    inst.install_date  as install_date,
    inst.removal_date  as removal_date,
    inst.dir_location  as dir_location,
    per.personnel_name  as personnel_name,
    per.personnel_role  as personnel_role,
    per.organization  as organization,
    per.office_phone  as office_phone,
    per.cell_phone  as cell_phone,
    per.email  as email,
    per.personnel_notes  as personnel_notes,
    ac.activity  as activity,
    ac.activity_description  as activity_description,
    i.activity_date_started  as activity_date_started,
    i.activity_date_completed  as activity_date_completed,
    i.activity_lead  as activity_lead,

    sc.name AS source_name
  FROM
    installations_personnel i
LEFT JOIN source sc ON i.source_id = sc.source_id
LEFT JOIN installations inst ON i.installations_id = inst.installations_id
LEFT JOIN personnel per ON i.personnel_id = per.personnel_id
LEFT JOIN activity ac ON i.activity_id = ac.activity_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_installations_personnel (
  installations_personnel_id UUID,
  install_date DATE,
  removal_date DATE,
  dir_location TEXT,
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
  instid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_installations_id(install_date, removal_date, dir_location) INTO instid;
  SELECT get_personnel_id(personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes) INTO perid;
  SELECT get_activity_id(activity, activity_description) INTO acid;

  IF( installations_personnel_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO installations_personnel_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO installations_personnel (
    installations_personnel_id, installations_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead, source_id
  ) VALUES (
    installations_personnel_id, instid, perid, acid, activity_date_started, activity_date_completed, activity_lead, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_installations_personnel (
  installations_personnel_id_in UUID,
  install_date_in date, removal_date_in date, dir_location_in text,
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
  activity_lead_in TEXT) RETURNS void AS $$
DECLARE
instid UUID;
perid UUID;
acid UUID;
BEGIN
  SELECT get_installations_id(install_date_in, removal_date_in, dir_location_in) INTO instid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  UPDATE installations_personnel SET (
    installations_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead
  ) = (
    instid, perid, acid, activity_date_started_in, activity_date_completed_in, activity_lead_in
  ) WHERE
    installations_personnel_id = installations_personnel_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_installations_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_installations_personnel(
    installations_personnel_id := NEW.installations_personnel_id,
    install_date := NEW.install_date,
    removal_date := NEW.removal_date,
    dir_location := NEW.dir_location,
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

CREATE OR REPLACE FUNCTION update_installations_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_installations_personnel(
    installations_personnel_id_in := NEW.installations_personnel_id,
    install_date_in := NEW.install_date,
    removal_date_in := NEW.removal_date,
    dir_location_in := NEW.dir_location,
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
CREATE OR REPLACE FUNCTION get_installations_personnel_id(
  install_date_in date, removal_date_in date, dir_location_in text,
  personnel_name_in TEXT, personnel_role_in TEXT, organization_in TEXT, office_phone_in TEXT,
  cell_phone_in TEXT, email_in TEXT, personnel_notes_in TEXT,
  activity_in TEXT, activity_description_in TEXT,
  activity_date_started_in DATE, activity_date_completed_in DATE, activity_lead_in TEXT
) RETURNS UUID AS $$
DECLARE
  iid UUID;
  instid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_installations_id(install_date_in, removal_date_in, dir_location_in) INTO instid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  IF activity_date_completed IS NULL THEN
    SELECT
      installations_personnel_id INTO iid
    FROM
      installations_personnel i
    WHERE
      installations_id = instid AND personnel_id = perid AND activity_id = acid AND
      activity_date_started = activity_date_started_in AND
      activity_date_completed is NULL AND
      activity_lead = activity_lead_in;
  ELSE
    SELECT
      installations_personnel_id INTO iid
    FROM
      installations_personnel i
    WHERE
      installations_id = instid AND personnel_id = perid AND activity_id = acid AND
      activity_date_started = activity_date_started_in AND
      activity_date_completed =activity_date_completed_in AND
      activity_lead = activity_lead_in;
  END IF;

  IF (iid IS NULL) THEN
    RAISE EXCEPTION 'Unknown installations_personnel: install_date="%" removal_date="%" dir_location="%"
    personnel_name="%" personnel_role="%" organization="%" office_phone="%" cell_phone="%" email="%" personnel_notes="%"
    activity="%" activity_description="%" activity_date_started ="%" activity_date_completed="%" activity_lead="%"',
    install_date_in, removal_date_in, dir_location_in,
    personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in,
    activity_in, activity_description_in, activity_date_started_in, activity_date_completed_in, activity_lead_in;
  END IF;

  RETURN iid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER installations_personnel_insert_trig
  INSTEAD OF INSERT ON
  installations_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE insert_installations_personnel_from_trig();

CREATE TRIGGER installations_personnel_update_trig
  INSTEAD OF UPDATE ON
  installations_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE update_installations_personnel_from_trig();
