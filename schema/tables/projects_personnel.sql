-- TABLE
DROP TABLE IF EXISTS projects_personnel CASCADE;
CREATE TABLE projects_personnel (
  projects_personnel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  projects_id UUID REFERENCES projects NOT NULL,
  personnel_id UUID REFERENCES personnel NOT NULL,
  activity_id UUID REFERENCES activity NOT NULL,
  activity_date_started DATE NOT NULL,
  activity_date_completed DATE,
  activity_lead TEXT NOT NULL
);
CREATE INDEX projects_personnel_source_id_idx ON projects_personnel(source_id);
CREATE INDEX projects_personnel_projects_id_idx ON projects_personnel(projects_id);
CREATE INDEX projects_personnel_personnel_id_idx ON projects_personnel(personnel_id);
CREATE INDEX projects_personnel_activity_id_idx ON projects_personnel(activity_id);

-- VIEW
CREATE OR REPLACE VIEW projects_personnel_view AS
  SELECT
    p.projects_personnel_id AS projects_personnel_id,
    pr.project_name  as project_name,
    pr.funding_source  as funding_source,
    pr.project_region  as project_region,
    ST_AsKML(pr.project_poly)  as project_poly_kml,
    per.personnel_role  as personnel_role,
    per.organization  as organization,
    per.office_phone  as office_phone,
    per.cell_phone  as cell_phone,
    per.email  as email,
    per.personnel_notes  as personnel_notes,
    ac.activity  as activity,
    ac.activity_description as activity_description,
    p.activity_date_started  as activity_date_started,
    p.activity_date_completed  as activity_date_completed,
    p.activity_lead  as activity_lead,

    sc.name AS source_name
  FROM
    projects_personnel p
LEFT JOIN source sc ON p.source_id = sc.source_id
LEFT JOIN projects pr ON p.projects_id = pr.projects_id
LEFT JOIN personnel per ON p.personnel_id = per.personnel_id
LEFT JOIN activity ac ON p.activity_id = ac.activity_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_projects_personnel (
  projects_personnel_id UUID,
  project_name TEXT,
  funding_source TEXT,
  project_region TEXT,
  project_poly_kml TEXT,
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
  prid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_projects_id(project_name, funding_source, project_region, project_poly_kml) INTO prid;
  SELECT get_personnel_id(personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes) INTO perid;
  SELECT get_activity_id(activity, activity_description) INTO acid;

  IF( projects_personnel_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO projects_personnel_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO projects_personnel (
    projects_personnel_id, projects_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead, source_id
  ) VALUES (
    projects_personnel_id, prid, perid, acid, activity_date_started, activity_date_completed, activity_lead, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_projects_personnel (
  projects_personnel_id_in UUID,
  project_name_in TEXT,
  funding_source_in TEXT,
  project_region_in TEXT,
  project_poly_kml_in TEXT,
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
prid UUID;
perid UUID;
acid UUID;
BEGIN
  SELECT get_projects_id(project_name_in, funding_source_in, project_region_in, project_poly_kml_in) INTO prid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;
  UPDATE projects_personnel SET (
    projects_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead
  ) = (
    prid, perid, acid, activity_date_started_in, activity_date_completed_in, activity_lead_in
  ) WHERE
    projects_personnel_id = projects_personnel_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_projects_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_projects_personnel(
    projects_personnel_id := NEW.projects_personnel_id,
    project_name := NEW.project_name,
    funding_source := NEW.funding_source,
    project_region := NEW.project_region,
    project_poly_kml := NEW.project_poly_kml,
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

CREATE OR REPLACE FUNCTION update_projects_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_projects_personnel(
    projects_personnel_id_in := NEW.projects_personnel_id,
    project_name_in := NEW.project_name,
    funding_source_in := NEW.funding_source,
    project_region_in := NEW.project_region,
    project_poly_kml_in := NEW.project_poly_kml,
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
CREATE OR REPLACE FUNCTION get_projects_personnel_id(
  project_name_in TEXT, funding_source_in TEXT, project_region_in TEXT, project_poly_kml_in TEXT,
  personnel_name_in TEXT, personnel_role_in TEXT, organization_in TEXT, office_phone_in TEXT,
  cell_phone_in TEXT, email_in TEXT, personnel_notes_in TEXT, activity_in TEXT, activity_description_in TEXT,
  activity_date_started_in DATE, activity_date_completed_in DATE, activity_lead_in TEXT
) RETURNS UUID AS $$
DECLARE
  pid UUID;
  prid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_projects_id(project_name_in, funding_source_in, project_region_in, project_poly_kml_in) INTO prid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  IF (activity_date_completed_in IS NULL) THEN
    SELECT
      projects_personnel_id INTO pid
    FROM
      projects_personnel p
    WHERE
      projects_id = prid AND
      personnel_id = perid AND
      activity_id = acid AND
      activity_date_started = activity_date_started_in AND
      activity_date_completed is NULL AND
      activity_lead = activity_lead_in;
  ELSE
    SELECT
      projects_personnel_id INTO pid
    FROM
      projects_personnel p
    WHERE
      projects_id = prid AND
      personnel_id = perid AND
      activity_id = acid AND
      activity_date_started = activity_date_started_in AND
      activity_date_completed = activity_date_completed_in AND
      activity_lead = activity_lead_in;

  END IF;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown projects_personnel: project_name="%" funding_source="%" project_region="%" project_poly_kml="%"
    personnel_name="%" personnel_role="%" organization="%" office_phone="%" cell_phone="%" email="%" personnel_notes="%"
    activity="%" activity_description="%" activity_date_started ="%" activity_date_completed="%" activity_lead="%"',
    project_name_in, funding_source_in, project_region_in, project_poly_kml_in, personnel_name_in, personnel_role_in,
    organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in, activity_in,
    activity_description_in, activity_date_started_in, activity_date_completed_in, activity_lead_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER projects_personnel_insert_trig
  INSTEAD OF INSERT ON
  projects_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE insert_projects_personnel_from_trig();

CREATE TRIGGER projects_personnel_update_trig
  INSTEAD OF UPDATE ON
  projects_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE update_projects_personnel_from_trig();
