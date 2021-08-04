-- TABLE
DROP TABLE IF EXISTS plot_quadrats CASCADE;
CREATE TABLE plot_quadrats (
  plot_quadrats_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  analysis_plot_id UUID REFERENCES analysis_plot NOT NULL,
  quadrats_id UUID REFERENCES quadrats NOT NULL
);
CREATE INDEX plot_quadrats_source_id_idx ON plot_quadrats(source_id);
CREATE INDEX plot_quadrats_analysis_plot_id_idx ON plot_quadrats(analysis_plot_id);
CREATE INDEX plot_quadrats_quadrats_id_idx ON plot_quadrats(quadrats_id);

-- VIEW
CREATE OR REPLACE VIEW plot_quadrats_view AS
  SELECT
    p.plot_quadrats_id AS plot_quadrats_id,
    ap.plot_name AS plot_name,
    ap.plot_type AS plot_type,
    ap.physiognomic_type AS physiognomic_type,
    ap.plot_description AS plot_description,
    ap.veg_coverage AS veg_coverage,
    ap.plot_sketch_file AS plot_sketch_file,
    ap.datasheet_scan_file AS datasheet_scan_file,
    ST_AsKML(ap.plot_geom)  as plot_geom_kml,
    ap.plot_notes AS plot_notes,
    q.quadrat_name AS quadrat_name,
    q.sampled AS sampled,
    q.sampling_method AS sampling_method,
    ST_AsKML(q.quadrat_geom)  as quadrat_geom_kml,

    sc.name AS source_name
  FROM
    plot_quadrats p
LEFT JOIN source sc ON p.source_id = sc.source_id
LEFT JOIN analysis_plot ap ON p.analysis_plot_id = ap.analysis_plot_id
LEFT JOIN quadrats q ON p.quadrats_id = q.quadrats_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_plot_quadrats (
  plot_quadrats_id UUID,
  plot_name TEXT,
  plot_type ANALYSIS_PLOT_TYPE,
  physiognomic_type ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description TEXT,
  veg_coverage FLOAT,
  plot_sketch_file TEXT,
  datasheet_scan_file TEXT,
  plot_geom_kml TEXT,
  plot_notes TEXT,
  quadrat_name TEXT,
  sampled BOOL,
  sampling_method TEXT,
  quadrat_geom_kml TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  apid UUID;
  qid UUID;
BEGIN

  IF( plot_quadrats_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO plot_quadrats_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_analysis_plot_id(plot_name, plot_type, physiognomic_type, plot_description, veg_coverage, plot_sketch_file, datasheet_scan_file, plot_geom_kml, plot_notes) INTO apid;
  SELECT get_quadrats_id(quadrat_name, sampled, sampling_method, quadrat_geom_kml) INTO qid;

  INSERT INTO plot_quadrats (
    plot_quadrats_id, analysis_plot_id, quadrats_id, source_id
  ) VALUES (
    plot_quadrats_id, apid, qid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_plot_quadrats (
  plot_quadrats_id_in UUID,
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT,
  quadrat_name_in TEXT,
  sampled_in BOOL,
  sampling_method_in TEXT,
  quadrat_geom_kml_in TEXT) RETURNS void AS $$
DECLARE
  apid UUID;
  qid UUID;

BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in) INTO apid;
  SELECT get_quadrats_id(quadrat_name_in, sampled_in, sampling_method_in, quadrat_geom_kml_in) INTO qid;

  UPDATE plot_quadrats SET (
    analysis_plot_id, quadrats_id
  ) = (
    apid, qid
  ) WHERE
    plot_quadrats_id = plot_quadrats_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_plot_quadrats_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_plot_quadrats(
    plot_quadrats_id := NEW.plot_quadrats_id,
    plot_name := NEW.plot_name,
    plot_type := NEW.plot_type,
    physiognomic_type := NEW.physiognomic_type,
    plot_description := NEW.plot_description,
    veg_coverage := NEW.veg_coverage,
    plot_sketch_file := NEW.plot_sketch_file,
    datasheet_scan_file := NEW.datasheet_scan_file,
    plot_geom_kml := NEW.plot_geom_kml,
    plot_notes := NEW.plot_notes,
    quadrat_name := NEW.quadrat_name,
    sampled := NEW.sampled,
    sampling_method := NEW.sampling_method,
    quadrat_geom_kml := NEW.quadrat_geom_kml,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_plot_quadrats_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_plot_quadrats(
    plot_quadrats_id_in := NEW.plot_quadrats_id,
    plot_name_in := NEW.plot_name,
    plot_type_in := NEW.plot_type,
    physiognomic_type_in := NEW.physiognomic_type,
    plot_description_in := NEW.plot_description,
    veg_coverage_in := NEW.veg_coverage,
    plot_sketch_file_in := NEW.plot_sketch_file,
    datasheet_scan_file_in := NEW.datasheet_scan_file,
    plot_geom_kml_in := NEW.plot_geom_kml,
    plot_notes_in := NEW.plot_notes,
    quadrat_name_in := NEW.quadrat_name,
    sampled_in := NEW.sampled,
    sampling_method_in := NEW.sampling_method,
    quadrat_geom_kml_in := NEW.quadrat_geom_kml
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_plot_quadrats_id(
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT,
  quadrat_name_in TEXT,
  sampled_in BOOL,
  sampling_method_in TEXT,
  quadrat_geom_kml_in TEXT
) RETURNS UUID AS $$
DECLARE
  pid UUID;
  apid UUID;
  qid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in) INTO apid;
  SELECT get_quadrats_id(quadrat_name_in, sampled_in, sampling_method_in, quadrat_geom_kml_in) INTO qid;

  SELECT
    plot_quadrats_id INTO pid
  FROM
    plot_quadrats p
  WHERE
    analysis_plot_id = apid AND
    quadrats_id = qid;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown plot_quadrats: plot_name="%" plot_type="%" physiognomic_type="%"
    plot_description="%" veg_coverage="%" plot_sketch_file="%" datasheet_scan_file="%" plot_geom_kml="%" plot_notes="%"
    quadrat_name="%" sampled="%" sampling_method="%" quadrat_geom_kml="%"',
    plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in,
    datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in, quadrat_name_in, sampled_in, sampling_method_in, quadrat_geom_kml_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER plot_quadrats_insert_trig
  INSTEAD OF INSERT ON
  plot_quadrats_view FOR EACH ROW
  EXECUTE PROCEDURE insert_plot_quadrats_from_trig();

CREATE TRIGGER plot_quadrats_update_trig
  INSTEAD OF UPDATE ON
  plot_quadrats_view FOR EACH ROW
  EXECUTE PROCEDURE update_plot_quadrats_from_trig();
