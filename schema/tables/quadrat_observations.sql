-- TABLE
DROP TABLE IF EXISTS quadrat_observations CASCADE;
CREATE TABLE quadrat_observations (
  quadrat_observations_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  quadrats_id UUID REFERENCES quadrats NOT NULL,
  observations_id UUID REFERENCES observations NOT NULL,
  variables_id UUID REFERENCES variables NOT NULL,
  units_id UUID REFERENCES units NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX quadrat_observations_source_id_idx ON quadrat_observations(source_id);
CREATE INDEX quadrat_observations_quadrats_id_idx ON quadrat_observations(quadrats_id);
CREATE INDEX quadrat_observations_observations_id_idx ON quadrat_observations(observations_id);
CREATE INDEX quadrat_observations_variables_id_idx ON quadrat_observations(variables_id);
CREATE INDEX quadrat_observations_units_id_idx ON quadrat_observations(units_id);

-- VIEW
CREATE OR REPLACE VIEW quadrat_observations_view AS
  SELECT
    q.quadrat_observations_id AS quadrat_observations_id,
    qd.quadrat_name AS quadrat_name,
    qd.sampled AS sampled,
    qd.sampling_method AS sampling_method,
    ST_AsKML(qd.quadrat_geom)  as quadrat_geom_kml,
    o.observation_type AS observation_type,
    o.observation_subtype AS observation_subtype,
    o.value_precision AS value_precision,
    v.variable_name  as variable_name,
    v.variable_type  as variable_type,
    u.units_name  as units_name,
    u.units_type  as units_type,
    u.units_description  as units_description,
    q.value as value,

    sc.name AS source_name
  FROM
    quadrat_observations q
LEFT JOIN source sc ON q.source_id = sc.source_id
LEFT JOIN quadrats qd on q.quadrats_id = qd.quadrats_id
LEFT JOIN observations o ON q.observations_id = o.observations_id
LEFT JOIN variables v ON q.variables_id = v.variables_id
LEFT JOIN units u ON q.units_id = u.units_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_quadrat_observations (
  quadrat_observations_id UUID,
  quadrat_name TEXT,
  sampled BOOL,
  sampling_method TEXT,
  quadrat_geom_kml TEXT,
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
  qid UUID;
  obid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_quadrats_id(quadrat_name, sampled, sampling_method, quadrat_geom_kml) INTO qid;
  SELECT get_observations_id(observation_type, observation_subtype, value_precision) INTO obid;
  SELECT get_variables_id(variable_name, variable_type) INTO vid;
  SELECT get_units_id(units_name, units_type, units_description) INTO uid;

  IF( quadrat_observations_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO quadrat_observations_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO quadrat_observations (
    quadrat_observations_id, quadrats_id, observations_id, value, source_id
  ) VALUES (
    quadrat_observations_id, qid, obid, vid, uid, value, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_quadrat_observations (
  quadrat_observations_id_in UUID,
  quadrat_name_in TEXT,
  sampled_in BOOL,
  sampling_method_in TEXT,
  quadrat_geom_kml_in TEXT,
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
  qid UUID;
  obid UUID;
  vid UUID;
  uid UUID;
BEGIN
  SELECT get_quadrats_id(quadrat_name_in, sampled_in, sampling_method_in, quadrat_geom_kml_in) INTO qid;
  SELECT get_observations_id(observation_type_in, observation_subtype_in, value_precision_in) INTO obid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  UPDATE quadrat_observations SET (
    quadrats_id, observations_id, variables_id, units_id, value
  ) = (
    qid, obid, vid, uid, value_in
  ) WHERE
    quadrat_observations_id = quadrat_observations_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_quadrat_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_quadrat_observations(
    quadrat_observations_id := NEW.quadrat_observations_id,
    quadrat_name := NEW.quadrat_name,
    sampled := NEW.sampled,
    sampling_method := NEW.sampling_method,
    quadrat_geom_kml := NEW.quadrat_geom_kml,
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

CREATE OR REPLACE FUNCTION update_quadrat_observations_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_quadrat_observations(
    quadrat_observations_id_in := NEW.quadrat_observations_id,
    quadrat_name_in := NEW.quadrat_name,
    sampled_in := NEW.sampled,
    sampling_method_in := NEW.sampling_method,
    quadrat_geom_kml_in := NEW.quadrat_geom_kml,
    observation_type_in := NEW.observation_type,
    observation_subtype_in := NEW.observation_subtype,
    value_precision_in := NEW.value_precision,
    variable_name_in := NEW.variable_name,
    variable_type_in := NEW.variable_type,
    units_name_in := NEW.units_name,
    units_type_in := NEW.units_type,
    units_description_in := NEW.units_description,
    value_in := NEW.value_in
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_quadrat_observations_id(
  quadrat_name_in TEXT,
  sampled_in BOOL,
  sampling_method_in TEXT,
  quadrat_geom_kml_in TEXT,
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
  qid UUID;
  qdid UUID;
  obid UUID;
  vid UUID;
  uid UUID;
BEGIN

  SELECT get_quadrats_id(quadrat_name_in ,sampled_in ,sampling_method_in ,quadrat_geom_kml_in) INTO qdid;
  SELECT get_observations_id(observation_type_in,observation_subtype_in,value_precision_in) INTO obid;
  SELECT get_variables_id(variable_name_in, variable_type_in) INTO vid;
  SELECT get_units_id(units_name_in, units_type_in, units_description_in) INTO uid;
  SELECT
    quadrat_observations_id INTO qid
  FROM
    quadrat_observations q
  WHERE
    quadrats_id = qdid AND
    observations_id = obid AND
    variables_id = vid AND
    units_id = uid AND
    value = value_in;


  IF (qid IS NULL) THEN
    RAISE EXCEPTION 'Unknown quadrat_observations: quadrat_name="%" sampled="%" sampling_method="%" quadrat_geom_kml="%"
    observation_type="%" observation_subtype="%" value_precision="%" variable_name="%" variable_type="%"
    units_name="%" units_type="%" units_description="%" value="%"', quadrat_name_in, sampled_in,
    sampling_method_in, quadrat_geom_kml_in, observation_type_in, observation_subtype_in, value_precision_in,
    variable_name_in, variable_type_in, units_name_in, units_type_in, units_description_in, value_in;
  END IF;

  RETURN qid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER quadrat_observations_insert_trig
  INSTEAD OF INSERT ON
  quadrat_observations_view FOR EACH ROW
  EXECUTE PROCEDURE insert_quadrat_observations_from_trig();

CREATE TRIGGER quadrat_observations_update_trig
  INSTEAD OF UPDATE ON
  quadrat_observations_view FOR EACH ROW
  EXECUTE PROCEDURE update_quadrat_observations_from_trig();
