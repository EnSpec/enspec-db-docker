-- TABLE
DROP TABLE IF EXISTS plot_tree_data CASCADE;
CREATE TABLE plot_tree_data (
  plot_tree_data_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  analysis_plot_id UUID REFERENCES analysis_plot NOT NULL,
  tree_data_id UUID REFERENCES tree_data NOT NULL
);
CREATE INDEX plot_tree_data_source_id_idx ON plot_tree_data(source_id);
CREATE INDEX plot_tree_data_plot_id_idx ON plot_tree_data(analysis_plot_id);
CREATE INDEX plot_tree_data_tree_data_id_idx ON plot_tree_data(tree_data_id);

-- VIEW
CREATE OR REPLACE VIEW plot_tree_data_view AS
  SELECT
    p.plot_tree_data_id AS plot_tree_data_id,
    a.plot_name  as plot_name,
    a.plot_type  as plot_type,
    a.physiognomic_type  as physiognomic_type,
    a.plot_description  as plot_description,
    a.veg_coverage  as veg_coverage,
    a.plot_sketch_file  as plot_sketch_file,
    a.datasheet_scan_file  as datasheet_scan_file,
    ST_AsKML(a.plot_geom)  as plot_geom_kml,
    a.plot_notes  as plot_notes,
    t.canopy_level  as canopy_level,
    ST_AsKML(t.crown_poly)  as crown_poly_kml,
    ST_X(t.tree_location)  as tree_loc_long,
    ST_Y(t.tree_location)  as tree_loc_lat,

    sc.name AS source_name
  FROM
    plot_tree_data p
LEFT JOIN source sc ON p.source_id = sc.source_id
LEFT JOIN analysis_plot a ON p.analysis_plot_id = a.analysis_plot_id
LEFT JOIN tree_data t ON p.tree_data_id = t.tree_data_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_plot_tree_data (
  plot_tree_data_id UUID,
  plot_name TEXT,
  plot_type ANALYSIS_PLOT_TYPE,
  physiognomic_type ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description TEXT,
  veg_coverage FLOAT,
  plot_sketch_file TEXT,
  datasheet_scan_file TEXT,
  plot_geom_kml TEXT,
  plot_notes TEXT,
  canopy_level TREE_CANOPY_LEVEL,
  crown_poly_kml TEXT,
  tree_loc_long FLOAT,
  tree_loc_lat FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  aid UUID;
  tid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name, plot_type, physiognomic_type, plot_description, veg_coverage, plot_sketch_file, datasheet_scan_file, plot_geom_kml, plot_notes) INTO aid;
  SELECT get_tree_data_id(canopy_level, crown_poly_kml, tree_loc_long, tree_loc_lat) INTO tid;

  IF( plot_tree_data_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO plot_tree_data_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO plot_tree_data (
    plot_tree_data_id, analysis_plot_id, tree_data_id, source_id
  ) VALUES (
    plot_tree_data_id, aid, tid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_plot_tree_data (
  plot_tree_data_id_in UUID,
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT,
  canopy_level_in TREE_CANOPY_LEVEL,
  crown_poly_kml_in TEXT,
  tree_loc_long_in FLOAT,
  tree_loc_lat_in FLOAT
  ) RETURNS void AS $$
DECLARE
aid UUID;
tid UUID;

BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in) INTO aid;
  SELECT get_tree_data_id(canopy_level_in, crown_poly_kml_in, tree_loc_long_in, tree_loc_lat_in) INTO tid;

  UPDATE plot_tree_data SET (
    analysis_plot_id, tree_data_id
  ) = (
    aid, tid
  ) WHERE
    plot_tree_data_id = plot_tree_data_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_plot_tree_data_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_plot_tree_data(
    plot_tree_data_id := NEW.plot_tree_data_id,
    plot_name := NEW.plot_name,
    plot_type := NEW.plot_type,
    physiognomic_type := NEW.physiognomic_type,
    plot_description := NEW.plot_description,
    veg_coverage := NEW.veg_coverage,
    plot_sketch_file := NEW.plot_sketch_file,
    datasheet_scan_file := NEW.datasheet_scan_file,
    plot_geom_kml := NEW.plot_geom_kml,
    plot_notes := NEW.plot_notes,
    canopy_level := NEW.canopy_level,
    crown_poly_kml := NEW.crown_poly_kml,
    tree_loc_long := NEW.tree_loc_long,
    tree_loc_lat := NEW.tree_loc_lat,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_plot_tree_data_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_plot_tree_data(
    plot_tree_data_id_in := NEW.plot_tree_data_id,
    plot_name_in := NEW.plot_name,
    plot_type_in := NEW.plot_type,
    physiognomic_type_in := NEW.physiognomic_type,
    plot_description_in := NEW.plot_description,
    veg_coverage_in := NEW.veg_coverage,
    plot_sketch_file_in := NEW.plot_sketch_file,
    datasheet_scan_file_in := NEW.datasheet_scan_file,
    plot_geom_kml_in := NEW.plot_geom_kml,
    plot_notes_in := NEW.plot_notes,
    canopy_level_in := NEW.canopy_level,
    crown_poly_kml_in := NEW.crown_poly_kml,
    tree_loc_long_in := NEW.tree_loc_long,
    tree_loc_lat_in := NEW.tree_loc_lat
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_plot_tree_data_id(
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT,
  canopy_level_in TREE_CANOPY_LEVEL,
  crown_poly_kml_in TEXT,
  tree_loc_long_in FLOAT,
  tree_loc_lat_in FLOAT
) RETURNS UUID AS $$
DECLARE
  pid UUID;
  aid UUID;
  tid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in) INTO aid;
  SELECT get_tree_data_id(canopy_level_in, crown_poly_kml_in, tree_loc_long_in, tree_loc_lat_in) INTO tid;

  SELECT
    plot_tree_data_id INTO pid
  FROM
    plot_tree_data p
  WHERE
    analysis_plot_id = aid AND
    tree_data_id = tid;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown plot_tree_data: plot_name="%" plot_type="%" physiognomic_type="%"
    plot_description="%" veg_coverage="%" plot_sketch_file="%" datasheet_scan_file="%" plot_geom_kml="%" plot_notes="%"
    canopy_level="%" crown_poly_kml="%" tree_loc_long="%" tree_loc_lat="%"', plot_name_in, plot_type_in, physiognomic_type_in,
    plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in,
    canopy_level_in, crown_poly_kml_in, tree_loc_long_in, tree_loc_lat_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER plot_tree_data_insert_trig
  INSTEAD OF INSERT ON
  plot_tree_data_view FOR EACH ROW
  EXECUTE PROCEDURE insert_plot_tree_data_from_trig();

CREATE TRIGGER plot_tree_data_update_trig
  INSTEAD OF UPDATE ON
  plot_tree_data_view FOR EACH ROW
  EXECUTE PROCEDURE update_plot_tree_data_from_trig();
