-- TABLE
DROP TABLE IF EXISTS projects CASCADE;
CREATE TABLE projects (
  projects_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  project_name TEXT NOT NULL,
  funding_source TEXT NOT NULL,
  project_region TEXT NOT NULL,
  project_poly GEOMETRY(POLYGON, 4326) NOT NULL
);
CREATE INDEX projects_source_id_idx ON projects(source_id);

-- VIEW
CREATE OR REPLACE VIEW projects_view AS
  SELECT
    p.projects_id AS projects_id,
    p.project_name  as project_name,
    p.funding_source  as funding_source,
    p.project_region  as project_region,
    ST_AsKML(p.project_poly)  as project_poly_kml,

    sc.name AS source_name
  FROM
    projects p
LEFT JOIN source sc ON p.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_projects (
  projects_id UUID,
  project_name TEXT,
  funding_source TEXT,
  project_region TEXT,
  project_poly_kml TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  proj_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(project_poly_kml) INTO proj_geom;
  IF( projects_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO projects_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO projects (
    projects_id, project_name, funding_source, project_region, project_poly, source_id
  ) VALUES (
    projects_id, project_name, funding_source, project_region, proj_geom, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_projects (
  projects_id_in UUID,
  project_name_in TEXT,
  funding_source_in TEXT,
  project_region_in TEXT,
  project_poly_kml_in TEXT) RETURNS void AS $$
DECLARE
proj_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(project_poly_kml_in) INTO proj_geom;
  UPDATE projects SET (
    project_name, funding_source, project_region, project_poly
  ) = (
    project_name_in, funding_source_in, project_region_in, proj_geom
  ) WHERE
    projects_id = projects_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_projects_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_projects(
    projects_id := NEW.projects_id,
    project_name := NEW.project_name,
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

CREATE OR REPLACE FUNCTION update_projects_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_projects(
    projects_id_in := NEW.projects_id,
    project_name_in := NEW.project_name,
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
CREATE OR REPLACE FUNCTION get_projects_id(project_name_in TEXT, funding_source_in TEXT, project_region_in TEXT, project_poly_kml_in TEXT) RETURNS UUID AS $$
DECLARE
  pid UUID;
  proj_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(project_poly_kml_in) INTO proj_geom;
  SELECT
    projects_id INTO pid
  FROM
    projects p
  WHERE
    project_name = project_name_in AND
    funding_source = funding_source_in AND
    project_region = project_region_in AND
    project_poly = proj_geom;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown projects: project_name="%" funding_source="%" project_region="%" project_poly_kml="%"', project_name_in, funding_source_in, project_region_in, project_poly_kml_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER projects_insert_trig
  INSTEAD OF INSERT ON
  projects_view FOR EACH ROW
  EXECUTE PROCEDURE insert_projects_from_trig();

CREATE TRIGGER projects_update_trig
  INSTEAD OF UPDATE ON
  projects_view FOR EACH ROW
  EXECUTE PROCEDURE update_projects_from_trig();
