-- TABLE
DROP TABLE IF EXISTS AnalysisPlot_StudySites CASCADE;
CREATE TABLE AnalysisPlot_StudySites (
  AnalysisPlot_StudySites_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  analysis_plot_id UUID REFERENCES analysis_plot NOT NULL,
  study_sites_id UUID REFERENCES study_sites NOT NULL
);
CREATE INDEX AnalysisPlot_StudySites_source_id_idx ON AnalysisPlot_StudySites(source_id);
CREATE INDEX AnalysisPlot_StudySites_analysis_plot_id_idx ON AnalysisPlot_StudySites(analysis_plot_id);
CREATE INDEX AnalysisPlot_StudySites_study_sites_id_idx ON AnalysisPlot_StudySites(study_sites_id);

ALTER TABLE AnalysisPlot_StudySites ADD CONSTRAINT uniq_ap_ss_row UNIQUE(analysis_plot_id, study_sites_id);

-- VIEW
CREATE OR REPLACE VIEW AnalysisPlot_StudySites_view AS
  SELECT
    A.AnalysisPlot_StudySites_id AS AnalysisPlot_StudySites_id,
    ap.plot_name  as plot_name,
    ap.plot_type  as plot_type,
    ap.physiognomic_type  as physiognomic_type,
    ap.plot_description  as plot_description,
    ap.veg_coverage  as veg_coverage,
    ap.plot_sketch_file  as plot_sketch_file,
    ap.datasheet_scan_file  as datasheet_scan_file,
    ST_AsKML(ap.plot_geom)  as plot_geom_kml,
    ap.plot_notes  as plot_notes,
    s.site_name  as site_name,
    s.region  as region,
    ST_AsKML(s.site_poly)  as site_poly_kml,

    sc.name AS source_name
  FROM
    AnalysisPlot_StudySites A
LEFT JOIN source sc ON A.source_id = sc.source_id
LEFT JOIN analysis_plot ap ON A.analysis_plot_id = ap.analysis_plot_id
LEFT JOIN study_sites s ON A.study_sites_id = s.study_sites_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_AnalysisPlot_StudySites (
  AnalysisPlot_StudySites_id UUID,
  plot_name TEXT,
  plot_type ANALYSIS_PLOT_TYPE,
  physiognomic_type ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description TEXT,
  veg_coverage FLOAT,
  plot_sketch_file TEXT,
  datasheet_scan_file TEXT,
  plot_geom_kml TEXT,
  plot_notes TEXT,
  site_name TEXT,
  region TEXT,
  site_poly_kml TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  apid UUID;
  sid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name, plot_type, physiognomic_type, plot_description, veg_coverage, plot_sketch_file, datasheet_scan_file, plot_geom_kml, plot_notes) INTO apid;
  SELECT get_study_sites_id(site_name, region, site_poly_kml) INTO sid;
  IF( AnalysisPlot_StudySites_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO AnalysisPlot_StudySites_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO AnalysisPlot_StudySites (
    AnalysisPlot_StudySites_id, analysis_plot_id, study_sites_id, source_id
  ) VALUES (
    AnalysisPlot_StudySites_id, apid, sid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_AnalysisPlot_StudySites (
  AnalysisPlot_StudySites_id_in UUID,
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT,
  site_name_in TEXT,
  region_in TEXT,
  site_poly_kml_in TEXT
) RETURNS void AS $$
DECLARE
apid UUID;
sid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in) INTO apid;
  SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO sid;
  UPDATE AnalysisPlot_StudySites SET (
    analysis_plot_id, study_sites_id
  ) = (
    apid, sid
  ) WHERE
    AnalysisPlot_StudySites_id = AnalysisPlot_StudySites_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_AnalysisPlot_StudySites_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_AnalysisPlot_StudySites(
    AnalysisPlot_StudySites_id := NEW.AnalysisPlot_StudySites_id,
    plot_name := NEW.plot_name,
    plot_type := NEW.plot_type,
    physiognomic_type := NEW.physiognomic_type,
    plot_description := NEW.plot_description,
    veg_coverage := NEW.veg_coverage,
    plot_sketch_file := NEW.plot_sketch_file,
    datasheet_scan_file := NEW.datasheet_scan_file,
    plot_geom_kml := NEW.plot_geom_kml,
    plot_notes := NEW.plot_notes,
    site_name := NEW.site_name,
    region := NEW.region,
    site_poly_kml := NEW.site_poly_kml,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_AnalysisPlot_StudySites_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_AnalysisPlot_StudySites(
    AnalysisPlot_StudySites_id_in := NEW.AnalysisPlot_StudySites_id,
    plot_name_in := NEW.plot_name,
    plot_type_in := NEW.plot_type,
    physiognomic_type_in := NEW.physiognomic_type,
    plot_description_in := NEW.plot_description,
    veg_coverage_in := NEW.veg_coverage,
    plot_sketch_file_in := NEW.plot_sketch_file,
    datasheet_scan_file_in := NEW.datasheet_scan_file,
    plot_geom_kml_in := NEW.plot_geom_kml,
    plot_notes_in := NEW.plot_notes,
    site_name_in := NEW.site_name,
    region_in := NEW.region,
    site_poly_kml_in := NEW.site_poly_kml
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_AnalysisPlot_StudySites_id(
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT,
  site_name_in text,
  region_in text,
  site_poly_kml_in text
) RETURNS UUID AS $$
DECLARE
  Aid UUID;
  apid UUID;
  sid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in,
    plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in) INTO apid;
  SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO sid;
  SELECT
    AnalysisPlot_StudySites_id INTO Aid
  FROM
    AnalysisPlot_StudySites A
  WHERE
    analysis_plot_id = apid AND
    study_sites_id = sid;

  IF (Aid IS NULL) THEN
    RAISE EXCEPTION 'Unknown AnalysisPlot_StudySites: plot_name="%" plot_type="%" physiognomic_type="%"
    plot_description="%" veg_coverage="%" plot_sketch_file="%" datasheet_scan_file="%" plot_geom_kml="%" plot_notes="%"
    site_name="%" region="%" site_poly_kml="%"', plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in,
    veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in, site_name_in, region_in, site_poly_kml_in;
  END IF;

  RETURN Aid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER AnalysisPlot_StudySites_insert_trig
  INSTEAD OF INSERT ON
  AnalysisPlot_StudySites_view FOR EACH ROW
  EXECUTE PROCEDURE insert_AnalysisPlot_StudySites_from_trig();

CREATE TRIGGER AnalysisPlot_StudySites_update_trig
  INSTEAD OF UPDATE ON
  AnalysisPlot_StudySites_view FOR EACH ROW
  EXECUTE PROCEDURE update_AnalysisPlot_StudySites_from_trig();
