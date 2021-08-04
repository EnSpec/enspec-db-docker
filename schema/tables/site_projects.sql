-- TABLE
DROP TABLE IF EXISTS site_projects CASCADE;
CREATE TABLE site_projects (
  site_projects_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  study_sites_id UUID REFERENCES study_sites NOT NULL,
  projects_id UUID REFERENCES projects NOT NULL
);
CREATE INDEX site_projects_source_id_idx ON site_projects(source_id);
CREATE INDEX site_projects_study_sites_id_idx ON site_projects(study_sites_id);
CREATE INDEX site_projects_projects_id_idx ON site_projects(projects_id);

-- VIEW
CREATE OR REPLACE VIEW site_projects_view AS
  SELECT
    s.site_projects_id AS site_projects_id,
    ss.site_name AS site_name,
    ss.region AS region,
    ST_AsKML(ss.site_poly) AS site_poly_kml,
    p.project_name AS project_name,
    p.funding_source AS funding_source,
    p.project_region AS project_region,
    ST_AsKML(p.project_poly) AS project_poly_kml,
    sc.name AS source_name
  FROM
    site_projects s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN study_sites ss ON s.study_sites_id = ss.study_sites_id
LEFT JOIN projects p ON s.projects_id = p.projects_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_site_projects (
  site_projects_id UUID,
  site_name TEXT,
  region TEXT,
  site_poly_kml TEXT,
  project_name TEXT,
  funding_source TEXT,
  project_region TEXT,
  project_poly_kml TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  ssid UUID;
  pid UUID;
BEGIN

  IF( site_projects_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO site_projects_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_study_sites_id(site_name, region, site_poly_kml) INTO ssid;
  SELECT get_projects_id(project_name, funding_source, project_region, project_poly_kml) INTO pid;

  INSERT INTO site_projects (
    site_projects_id, study_sites_id, projects_id, source_id
  ) VALUES (
    site_projects_id, ssid, pid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_site_projects (
  site_projects_id_in UUID,
  site_name_in TEXT,
  region_in TEXT,
  site_poly_kml_in TEXT,
  project_name_in TEXT,
  funding_source_in TEXT,
  project_region_in TEXT,
  project_poly_kml_in TEXT) RETURNS void AS $$
DECLARE
  ssid UUID;
  pid UUID;

BEGIN
  SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO ssid;
  SELECT get_projects_id(project_name_in, funding_source_in, project_region_in, project_poly_kml_in) INTO pid;
  UPDATE site_projects SET (
    study_sites_id, projects_id
  ) = (
    ssid, pid
  ) WHERE
    site_projects_id = site_projects_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_site_projects_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_site_projects(
    site_projects_id := NEW.site_projects_id,
    site_name := NEW.site_name,
    region := NEW.region,
    site_poly_kml := NEW.site_poly_kml,
    project_name :=  NEW.project_name,
    funding_source := NEW.funding_source,
    project_region := NEW.project_region,
    project_poly_kml := NEW.project_poly_kml,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_site_projects_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_site_projects(
    site_projects_id_in := NEW.site_projects_id,
    site_name_in := NEW.site_name,
    region_in := NEW.region,
    site_poly_kml_in := NEW.site_poly_kml,
    project_name_in :=  NEW.project_name,
    funding_source_in := NEW.funding_source,
    project_region_in := NEW.project_region,
    project_poly_kml_in := NEW.project_poly_kml
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_site_projects_id(
  site_name_in TEXT,
  region_in TEXT,
  site_poly_kml_in TEXT,
  project_name_in TEXT,
  funding_source_in TEXT,
  project_region_in TEXT,
  project_poly_kml_in TEXT
) RETURNS UUID AS $$
DECLARE
  sid UUID;
  ssid UUID;
  pid UUID;
BEGIN
SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO ssid;
SELECT get_projects_id(project_name_in, funding_source_in, project_region_in, project_poly_kml_in) INTO pid;
  SELECT
    site_projects_id INTO sid
  FROM
    site_projects s
  WHERE
    study_sites_id = ssid AND
    projects_id = pid;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown site_projects: site_name="%" region="%" site_poly_kml="%" project_name="%" funding_source="%" project_region="%" project_poly_kml="%"',
    site_name_in, region_in, site_poly_kml_in, project_name_in, funding_source_in, project_region_in, project_poly_kml_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER site_projects_insert_trig
  INSTEAD OF INSERT ON
  site_projects_view FOR EACH ROW
  EXECUTE PROCEDURE insert_site_projects_from_trig();

CREATE TRIGGER site_projects_update_trig
  INSTEAD OF UPDATE ON
  site_projects_view FOR EACH ROW
  EXECUTE PROCEDURE update_site_projects_from_trig();
