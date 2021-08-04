-- TABLE
DROP TABLE IF EXISTS plot_observations CASCADE;
CREATE TABLE plot_observations (
  plot_observations_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  analysis_plot_id UUID REFERENCES analysis_plot NOT NULL,
  observations_id UUID REFERENCES observations NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX plot_observations_source_id_idx ON plot_observations(source_id);
CREATE INDEX plot_observations_plot_id_idx ON plot_observations(analysis_plot_id);
CREATE INDEX plot_observations_observations_id_idx ON plot_observations(observations_id);
CREATE INDEX plot_observations_variables_id_idx ON plot_observations(variables_id);
CREATE INDEX plot_observations_units_id_idx ON plot_observations(units_id);

-- VIEW
CREATE OR REPLACE VIEW plot_observations_view AS
  SELECT
    p.plot_observations_id AS plot_observations_id,
    a.plot_name  as plot_name,
    a.plot_type  as plot_type,
    a.physiognomic_type  as physiognomic_type,
    a.plot_description  as plot_description,
    a.veg_coverage  as veg_coverage,
    a.plot_sketch_file  as plot_sketch_file,
    a.datasheet_scan_file  as datasheet_scan_file,
    ST_AsKML(a.plot_geom)  as plot_geom_kml,
    a.plot_notes  as plot_notes,
    o.observation_type  as observation_type,
    o.observation_subtype  as observation_subtype,
    o.value_precision  as value_precision,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,
    p.value as value,

    sc.name AS source_name
  FROM
    plot_observations p
LEFT JOIN source sc ON p.source_id = sc.source_id
LEFT JOIN analysis_plot a ON p.analysis_plot_id = a.analysis_plot_id
LEFT JOIN observations o ON p.observations_id = o.observations_id
LEFT JOIN variables v ON p.variables_id = v.variables_id
LEFT JOIN units u ON p.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_plot_observations (
  plot_observations_id UUID,
  plot_name TEXT,
  plot_type ANALYSIS_PLOT_TYPE,
  physiognomic_type ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description TEXT,
  veg_coverage FLOAT,
  plot_sketch_file TEXT,
  datasheet_scan_file TEXT,
  plot_geom_kml TEXT,
  plot_notes TEXT,
  observation_type TEXT,
  observation_subtype TEXT,
  value_precision INTEGER,
  variable_name TEXT,
  variable_type TEXT,
  units_name TEXT,
  units_type TEXT,
  units_description TEXT,
  value FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  aid UUID;
  obid UUID;
  vid UUID;
  uid UUID;
BEGIN

  SELECT get_analysis_plot_id(plot_name, plot_type, physiognomic_type, plot_description, veg_coverage, plot_sketch_file, datasheet_scan_file, plot_geom_kml, plot_notes) INTO aid;
  SELECT get_observations_id(observation_type, observation_subtype, value_precision) INTO obid;
  SELECT get_variables_id(variable_name, variable_type) INTO vid;
  SELECT get_units_id(units_name, units_type, units_description) INTO uid;

  IF( plot_observations_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO plot_observations_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO plot_observations (
    plot_observations_id, analysis_plot_id, observations_id, variables_id, units_id, value, source_id
  ) VALUES (
    plot_observations_id, aid, obid, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_plot_observations (
  plot_observations_id_in UUID,
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT,
  observation_type_in TEXT,
  observation_subtype_in TEXT,
  value_precision_in INTEGER,
  variable_name_in TEXT,
  variable_type_in TEXT,
  units_name_in TEXT,
  units_type_in TEXT,
  units_description_in TEXT,
  value_in FLOAT
) RETURNS void AS $$
DECLARE
aid UUID;
obid UUID;
vid UUID;
uid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in) INTO aid;
  SELECT get_observations_id(observation_type_in, observation_subtype_in, value_precision_in) INTO obid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;

  UPDATE plot_observations SET (
    analysis_plot_id, observations_id, variables_id, units_id, value
  ) = (
    aid, obid, vid, uid, value_in
  ) WHERE
    plot_observations_id = plot_observations_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_plot_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_plot_observations(
    plot_observations_id := NEW.plot_observations_id,
    plot_name := NEW.plot_name,
    plot_type := NEW.plot_type,
    physiognomic_type := NEW.physiognomic_type,
    plot_description := NEW.plot_description,
    veg_coverage := NEW.veg_coverage,
    plot_sketch_file := NEW.plot_sketch_file,
    datasheet_scan_file := NEW.datasheet_scan_file,
    plot_geom_kml := NEW.plot_geom_kml,
    plot_notes := NEW.plot_notes,
    observation_type := NEW.observation_type,
    observation_subtype := NEW.observation_subtype,
    value_precision := NEW.value_precision,
    variable_name := NEW.variable_name,
    variable_type := NEW.variable_type,
    units_name := NEW.units_name,
    units_type := NEW.units_type,
    units_description := NEW.units_description,
    value := NEW.value,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_plot_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_plot_observations(
    plot_observations_id_in := NEW.plot_observations_id,
    plot_name_in := NEW.plot_name,
    plot_type_in := NEW.plot_type,
    physiognomic_type_in := NEW.physiognomic_type,
    plot_description_in := NEW.plot_description,
    veg_coverage_in := NEW.veg_coverage,
    plot_sketch_file_in := NEW.plot_sketch_file,
    datasheet_scan_file_in := NEW.datasheet_scan_file,
    plot_geom_kml_in := NEW.plot_geom_kml,
    plot_notes_in := NEW.plot_notes,
    observation_type_in := NEW.observation_type,
    observation_subtype_in := NEW.observation_subtype,
    value_precision_in := NEW.value_precision,
    variable_name_in := NEW.variable_name,
    variable_type_in := NEW.variable_type,
    units_name_in := NEW.units_name,
    units_type_in := NEW.units_type,
    units_description_in := NEW.units_description,
    value_in := NEW.value
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_plot_observations_id(
  plot_name_in TEXT,
  plot_type_in ANALYSIS_PLOT_TYPE,
  physiognomic_type_in ANALYSIS_PLOT_PHYSIOGNOMIC_TYPE,
  plot_description_in TEXT,
  veg_coverage_in FLOAT,
  plot_sketch_file_in TEXT,
  datasheet_scan_file_in TEXT,
  plot_geom_kml_in TEXT,
  plot_notes_in TEXT,
  observation_type_in TEXT,
  observation_subtype_in TEXT,
  value_precision_in INTEGER,
  variable_name_in text,
  variable_type_in text,
  units_name_in text,
  units_type_in text,
  units_description_in text,
  value_in FLOAT
) RETURNS UUID AS $$
DECLARE
  pid UUID;
  aid UUID;
  obid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_analysis_plot_id(plot_name_in, plot_type_in, physiognomic_type_in, plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in) INTO aid;
  SELECT get_observations_id(observation_type_in, observation_subtype_in, value_precision_in) INTO obid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;

  SELECT
    plot_observations_id INTO pid
  FROM
    plot_observations p
  WHERE
    analysis_plot_id = aid AND
    observations_id = obid AND
    variables_id = vid AND
    units_id = uid AND
    value = value_in;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown plot_observations: plot_name="%" plot_type="%" physiognomic_type="%"
    plot_description="%" veg_coverage="%" plot_sketch_file="%" datasheet_scan_file="%" plot_geom_kml="%" plot_notes="%"
    observation_type="%" observation_subtype="%" value_precision="%" variable_name="%" variable_type="%" units_name="%"
    units_type="%" units_description="%" value="%"', plot_name_in, plot_type_in, physiognomic_type_in,
    plot_description_in, veg_coverage_in, plot_sketch_file_in, datasheet_scan_file_in, plot_geom_kml_in, plot_notes_in,
    observation_type_in, observation_subtype_in, value_precision_in, variable_name_in, variable_type_in,
    units_name_in, units_type_in, units_description_in, value_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER plot_observations_insert_trig
  INSTEAD OF INSERT ON
  plot_observations_view FOR EACH ROW
  EXECUTE PROCEDURE insert_plot_observations_from_trig();

CREATE TRIGGER plot_observations_update_trig
  INSTEAD OF UPDATE ON
  plot_observations_view FOR EACH ROW
  EXECUTE PROCEDURE update_plot_observations_from_trig();
