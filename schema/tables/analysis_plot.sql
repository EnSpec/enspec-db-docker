-- TABLE
DROP TABLE IF EXISTS analysis_plot CASCADE;
CREATE TABLE analysis_plot (
  analysis_plot_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  plot_name TEXT NOT NULL,
  plot_type ANALYSIS_PLOT_TYPE NOT NULL,
  physiognomic_type ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE NOT NULL,
  plot_description TEXT NOT NULL,
  veg_coverage FLOAT NOT NULL,
  plot_sketch_file TEXT NOT NULL,
  datasheet_scan_file TEXT NOT NULL,
  plot_geom GEOMETRY NOT NULL,
  plot_notes TEXT
);
CREATE INDEX analysis_plot_source_id_idx ON analysis_plot(source_id);

-- VIEW
CREATE OR REPLACE VIEW analysis_plot_view AS
  SELECT
    a.analysis_plot_id AS analysis_plot_id,
    a.plot_name  as plot_name,
    a.plot_type  as plot_type,
    a.physiognomic_type  as physiognomic_type,
    a.plot_description  as plot_description,
    a.veg_coverage  as veg_coverage,
    a.plot_sketch_file  as plot_sketch_file,
    a.datasheet_scan_file  as datasheet_scan_file,
    ST_AsKML(a.plot_geom)  as plot_geom_kml,
    a.plot_notes  as plot_notes,

    sc.name AS source_name
  FROM
    analysis_plot a
LEFT JOIN source sc ON a.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_analysis_plot (
  analysis_plot_id UUID,
  plot_name TEXT,
  plot_type ANALYSIS_PLOT_TYPE,
  physiognomic_type ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description TEXT,
  veg_coverage FLOAT,
  plot_sketch_file TEXT,
  datasheet_scan_file TEXT,
  plot_geom_kml TEXT,
  plot_notes TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  plot_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(plot_geom_kml) INTO plot_kml_to_geom;
  IF( analysis_plot_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO analysis_plot_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO analysis_plot (
    analysis_plot_id, plot_name, plot_type, physiognomic_type, plot_description, veg_coverage, plot_sketch_file, datasheet_scan_file, plot_geom, plot_notes, source_id
  ) VALUES (
    analysis_plot_id, plot_name, plot_type, physiognomic_type, plot_description, veg_coverage, plot_sketch_file, datasheet_scan_file, plot_kml_to_geom, plot_notes, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_analysis_plot (
  analysis_plot_id_in UUID,
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT) RETURNS void AS $$
DECLARE
plot_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(plot_geom_kml_in) INTO plot_kml_to_geom;
  UPDATE analysis_plot SET (
    plot_name, plot_type, physiognomic_type, plot_description, veg_coverage, plot_sketch_file, datasheet_scan_file, plot_geom, plot_notes
  ) = (
    plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_kml_to_geom, plot_notes_in
  ) WHERE
    analysis_plot_id = analysis_plot_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_analysis_plot_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_analysis_plot(
    analysis_plot_id := NEW.analysis_plot_id,
    plot_name := NEW.plot_name,
    plot_type := NEW.plot_type,
    physiognomic_type := NEW.physiognomic_type,
    plot_description := NEW.plot_description,
    veg_coverage := NEW.veg_coverage,
    plot_sketch_file := NEW.plot_sketch_file,
    datasheet_scan_file := NEW.datasheet_scan_file,
    plot_geom_kml := NEW.plot_geom_kml,
    plot_notes := NEW.plot_notes,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_analysis_plot_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_analysis_plot(
    analysis_plot_id_in := NEW.analysis_plot_id,
    plot_name_in := NEW.plot_name,
    plot_type_in := NEW.plot_type,
    physiognomic_type_in := NEW.physiognomic_type,
    plot_description_in := NEW.plot_description,
    veg_coverage_in := NEW.veg_coverage,
    plot_sketch_file_in := NEW.plot_sketch_file,
    datasheet_scan_file_in := NEW.datasheet_scan_file,
    plot_geom_kml_in := NEW.plot_geom_kml,
    plot_notes_in := NEW.plot_notes
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_analysis_plot_id(
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT
) RETURNS UUID AS $$
DECLARE
  aid UUID;
  plot_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(plot_geom_kml_in) INTO plot_kml_to_geom;
  IF (plot_notes_in is NULL) THEN
    SELECT
      analysis_plot_id INTO aid
    FROM
      analysis_plot a
    WHERE
      plot_name = plot_name_in AND
      plot_type = plot_type_in AND
      physiognomic_type = physiognomic_type_in AND
      plot_description = plot_description_in AND
      veg_coverage = veg_coverage_in AND
      plot_sketch_file = plot_sketch_file_in AND
      datasheet_scan_file = datasheet_scan_file_in AND
      plot_geom = plot_kml_to_geom AND
      plot_notes IS NULL;
  ELSE
    SELECT
      analysis_plot_id INTO aid
    FROM
      analysis_plot a
    WHERE
      plot_name = plot_name_in AND
      plot_type = plot_type_in AND
      physiognomic_type = physiognomic_type_in AND
      plot_description = plot_description_in AND
      veg_coverage = veg_coverage_in AND
      plot_sketch_file = plot_sketch_file_in AND
      datasheet_scan_file = datasheet_scan_file_in AND
      plot_geom = plot_kml_to_geom AND
      plot_notes = plot_notes_in;
  END IF;

  IF (aid IS NULL) THEN
    RAISE EXCEPTION 'Unknown analysis_plot: plot_name="%" plot_type="%" physiognomic_type="%"
    plot_description="%" veg_coverage="%" plot_sketch_file="%" datasheet_scan_file="%" plot_geom_kml="%" plot_notes="%"',
    plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in,
    datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in;
  END IF;

  RETURN aid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER analysis_plot_insert_trig
  INSTEAD OF INSERT ON
  analysis_plot_view FOR EACH ROW
  EXECUTE PROCEDURE insert_analysis_plot_from_trig();

CREATE TRIGGER analysis_plot_update_trig
  INSTEAD OF UPDATE ON
  analysis_plot_view FOR EACH ROW
  EXECUTE PROCEDURE update_analysis_plot_from_trig();
