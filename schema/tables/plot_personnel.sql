-- TABLE
DROP TABLE IF EXISTS plot_personnel CASCADE;
CREATE TABLE plot_personnel (
  plot_personnel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  analysis_plot_id UUID REFERENCES analysis_plot NOT NULL,
  personnel_id UUID REFERENCES personnel NOT NULL,
  activity_id UUID REFERENCES activity NOT NULL,
  activity_date_started DATE NOT NULL,
  activity_date_completed DATE,
  activity_lead TEXT NOT NULL
);
CREATE INDEX plot_personnel_source_id_idx ON plot_personnel(source_id);
CREATE INDEX plot_personnel_analysis_plot_id_idx ON plot_personnel(analysis_plot_id);
CREATE INDEX plot_personnel_personnel_id_idx ON plot_personnel(personnel_id);
CREATE INDEX plot_personnel_activity_id_idx ON plot_personnel(activity_id);

-- VIEW
CREATE OR REPLACE VIEW plot_personnel_view AS
  SELECT
    p.plot_personnel_id AS plot_personnel_id,
    ap.plot_name  as plot_name,
    ap.plot_type  as plot_type,
    ap.physiognomic_type  as physiognomic_type,
    ap.plot_description  as plot_description,
    ap.veg_coverage  as veg_coverage,
    ap.plot_sketch_file  as plot_sketch_file,
    ap.datasheet_scan_file  as datasheet_scan_file,
    ap.plot_geom  as plot_geom,
    ap.plot_notes  as plot_notes,
    per.personnel_name  as personnel_name,
    per.personnel_role  as personnel_role,
    per.organization  as organization,
    per.office_phone  as office_phone,
    per.cell_phone  as cell_phone,
    per.email  as email,
    per.personnel_notes  as personnel_notes,
    ac.activity  as activity,
    ac.activity_description  as activity_description,
    p.activity_date_started  as activity_date_started,
    p.activity_date_completed  as activity_date_completed,
    p.activity_lead  as activity_lead,

    sc.name AS source_name
  FROM
    plot_personnel p
LEFT JOIN source sc ON p.source_id = sc.source_id
LEFT JOIN analysis_plot ap ON p.analysis_plot_id = ap.analysis_plot_id
LEFT JOIN personnel per ON p.personnel_id = per.personnel_id
LEFT JOIN activity ac ON p.activity_id = ac.activity_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_plot_personnel (
  plot_personnel_id UUID,
  plot_name TEXT,
  plot_type ANALYSIS_PLOT_TYPE,
  physiognomic_type ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description TEXT,
  veg_coverage FLOAT,
  plot_sketch_file TEXT,
  datasheet_scan_file TEXT,
  plot_geom GEOMETRY,
  plot_notes TEXT,
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
  apid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name, plot_type, physiognomic_type, plot_description, veg_coverage, plot_sketch_file, datasheet_scan_file, plot_geom, plot_notes) INTO apid;
  SELECT get_personnel_id(personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes) INTO perid;
  SELECT get_activity_id(activity, activity_description) INTO acid;

  IF( plot_personnel_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO plot_personnel_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO plot_personnel (
    plot_personnel_id, analysis_plot_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead, source_id
  ) VALUES (
    plot_personnel_id, apid, perid, acid, activity_date_started, activity_date_completed, activity_lead, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_plot_personnel (
  plot_personnel_id_in UUID,
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_in GEOMETRY,
  plot_notes_in TEXT,
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
apid UUID;
perid UUID;
acid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in,
    plot_sketch_file_in, datasheet_scan_file_in, plot_geom_in, plot_notes_in) INTO apid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  UPDATE plot_personnel SET (
    analysis_plot_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead
  ) = (
    apid, perid, acid, activity_date_started_in, activity_date_completed_in, activity_lead_in
  ) WHERE
    plot_personnel_id = plot_personnel_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_plot_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_plot_personnel(
    plot_personnel_id := NEW.plot_personnel_id,
    plot_name := NEW.plot_name,
    plot_type := NEW.plot_type,
    physiognomic_type := NEW.physiognomic_type,
    plot_description := NEW.plot_description,
    veg_coverage := NEW.veg_coverage,
    plot_sketch_file := NEW.plot_sketch_file,
    datasheet_scan_file := NEW.datasheet_scan_file,
    plot_geom := NEW.plot_geom,
    plot_notes := NEW.plot_notes,
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

CREATE OR REPLACE FUNCTION update_plot_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_plot_personnel(
    plot_personnel_id_in := NEW.plot_personnel_id,
    plot_name_in := NEW.plot_name,
    plot_type_in := NEW.plot_type,
    physiognomic_type_in := NEW.physiognomic_type,
    plot_description_in := NEW.plot_description,
    veg_coverage_in := NEW.veg_coverage,
    plot_sketch_file_in := NEW.plot_sketch_file,
    datasheet_scan_file_in := NEW.datasheet_scan_file,
    plot_geom_in := NEW.plot_geom,
    plot_notes_in := NEW.plot_notes,
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
CREATE OR REPLACE FUNCTION get_plot_personnel_id(
    plot_name_in TEXT,
    plot_type_in ANALYSIS_PLOT_TYPE,
    physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
    plot_description_in TEXT,
    veg_coverage_in FLOAT,
    plot_sketch_file_in TEXT,
    datasheet_scan_file_in TEXT,
    plot_geom_in GEOMETRY,
    plot_notes_in TEXT,
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
  pid UUID;
  apid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in,
    plot_sketch_file_in, datasheet_scan_file_in, plot_geom_in, plot_notes_in) INTO apid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  IF (activity_date_completed_in IS NULL) THEN
    SELECT
      plot_personnel_id INTO pid
    FROM
      plot_personnel p
    WHERE
      analysis_plot_id = apid AND
      personnel_id = perid AND
      activity_id = acid AND
      activity_date_started = activity_date_started_in AND
      activity_date_completed is NULL AND
      activity_lead = activity_lead_in;
  ELSE
    SELECT
      plot_personnel_id INTO pid
    FROM
      plot_personnel p
    WHERE
      analysis_plot_id = apid AND
      personnel_id = perid AND
      activity_id = acid AND
      activity_date_started = activity_date_started_in AND
      activity_date_completed = activity_date_completed_in AND
      activity_lead = activity_lead_in;

  END IF;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown plot_personnel: plot_name="%" plot_type="%" physiognomic_type="%"
    plot_description="%" veg_coverage="%" plot_sketch_file="%" datasheet_scan_file="%" plot_geom="%" plot_notes="%"
    personnel_name="%" personnel_role="%" organization="%" office_phone="%" cell_phone="%" email="%" personnel_notes="%"
    activity="%" activity_description="%" activity_date_started ="%" activity_date_completed="%" activity_lead="%"', plot_name_in,
    plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in,
    datasheet_scan_file_in, plot_geom_in, plot_notes_in, personnel_name_in, personnel_role_in, organization_in,
    office_phone_in, cell_phone_in, email_in, personnel_notes_in, activity_in, activity_description_in, activity_date_started_in,
    activity_date_completed_in, activity_lead_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER plot_personnel_insert_trig
  INSTEAD OF INSERT ON
  plot_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE insert_plot_personnel_from_trig();

CREATE TRIGGER plot_personnel_update_trig
  INSTEAD OF UPDATE ON
  plot_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE update_plot_personnel_from_trig();
