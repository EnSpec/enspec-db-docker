-- TABLE
DROP TABLE IF EXISTS quadrats CASCADE;
CREATE TABLE quadrats (
  quadrats_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  quadrat_name TEXT NOT NULL,
  sampled BOOL NOT NULL,
  sampling_method TEXT NOT NULL,
  quadrat_geom GEOMETRY NOT NULL
);
CREATE INDEX quadrats_source_id_idx ON quadrats(source_id);

-- VIEW
CREATE OR REPLACE VIEW quadrats_view AS
  SELECT
    q.quadrats_id AS quadrats_id,
    q.quadrat_name  as quadrat_name,
    q.sampled  as sampled,
    q.sampling_method  as sampling_method,
    ST_AsKML(q.quadrat_geom)  as quadrat_geom_kml,

    sc.name AS source_name
  FROM
    quadrats q
LEFT JOIN source sc ON q.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_quadrats (
  quadrats_id UUID,
  quadrat_name TEXT,
  sampled BOOL,
  sampling_method TEXT,
  quadrat_geom_kml TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  quadrat_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(quadrat_geom_kml) INTO quadrat_kml_to_geom;
  IF( quadrats_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO quadrats_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO quadrats (
    quadrats_id, quadrat_name, sampled, sampling_method, quadrat_geom, source_id
  ) VALUES (
    quadrats_id, quadrat_name, sampled, sampling_method, quadrat_kml_to_geom, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_quadrats (
  quadrats_id_in UUID,
  quadrat_name_in TEXT,
  sampled_in BOOL,
  sampling_method_in TEXT,
  quadrat_geom_kml_in TEXT) RETURNS void AS $$
DECLARE
quadrat_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(quadrat_geom_kml_in) INTO quadrat_kml_to_geom;
  UPDATE quadrats SET (
    quadrat_name, sampled, sampling_method, quadrat_geom
  ) = (
    quadrat_name_in, sampled_in, sampling_method_in, quadrat_kml_to_geom
  ) WHERE
    quadrats_id = quadrats_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_quadrats_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_quadrats(
    quadrats_id := NEW.quadrats_id,
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

CREATE OR REPLACE FUNCTION update_quadrats_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_quadrats(
    quadrats_id_in := NEW.quadrats_id,
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
CREATE OR REPLACE FUNCTION get_quadrats_id(
  quadrat_name_in TEXT,
  sampled_in BOOL,
  sampling_method_in TEXT,
  quadrat_geom_kml_in TEXT
) RETURNS UUID AS $$
DECLARE
  qid UUID;
  quadrat_kml_to_geom GEOMETRY;
BEGIN
  SELECT ST_GeomFromKML(quadrat_geom_kml_in) INTO quadrat_kml_to_geom;
  SELECT
    quadrats_id INTO qid
  FROM
    quadrats q
  WHERE
    quadrat_name = quadrat_name_in AND
    sampled = sampled_in AND
    sampling_method = sampling_method_in AND
    quadrat_geom = quadrat_kml_to_geom;

  IF (qid IS NULL) THEN
    RAISE EXCEPTION 'Unknown quadrats: quadrat_name="%" sampled="%" sampling_method="%" quadrat_geom_kml="%"',
    quadrat_name_in, sampled_in, sampling_method_in, quadrat_geom_kml_in;
  END IF;

  RETURN qid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER quadrats_insert_trig
  INSTEAD OF INSERT ON
  quadrats_view FOR EACH ROW
  EXECUTE PROCEDURE insert_quadrats_from_trig();

CREATE TRIGGER quadrats_update_trig
  INSTEAD OF UPDATE ON
  quadrats_view FOR EACH ROW
  EXECUTE PROCEDURE update_quadrats_from_trig();
