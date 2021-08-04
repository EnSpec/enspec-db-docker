-- TABLE
DROP TABLE IF EXISTS measurements CASCADE;
CREATE TABLE measurements (
  measurements_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  measurement_type PHYSICAL_OR_CHEMICAL NOT NULL,
  file_location TEXT NOT NULL,
  assay TEXT NOT NULL
);
CREATE INDEX measurements_source_id_idx ON measurements(source_id);

-- VIEW
CREATE OR REPLACE VIEW measurements_view AS
  SELECT
    m.measurements_id AS measurements_id,
    m.measurement_type  as measurement_type,
    m.file_location  as file_location,
    m.assay  as assay,

    sc.name AS source_name
  FROM
    measurements m
LEFT JOIN source sc ON m.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_measurements (
  measurements_id UUID,
  measurement_type PHYSICAL_OR_CHEMICAL,
  file_location TEXT,
  assay TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( measurements_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO measurements_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO measurements (
    measurements_id, measurement_type, file_location, assay, source_id
  ) VALUES (
    measurements_id, measurement_type, file_location, assay, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_measurements (
  measurements_id_in UUID,
  measurement_type_in PHYSICAL_OR_CHEMICAL,
  file_location_in TEXT,
  assay_in TEXT) RETURNS void AS $$
DECLARE

BEGIN

  UPDATE measurements SET (
    measurement_type, file_location, assay
  ) = (
    measurement_type_in, file_location_in, assay_in
  ) WHERE
    measurements_id = measurements_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_measurements_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_measurements(
    measurements_id := NEW.measurements_id,
    measurement_type := NEW.measurement_type,
    file_location := NEW.file_location,
    assay := NEW.assay,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_measurements_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_measurements(
    measurements_id_in := NEW.measurements_id,
    measurement_type_in := NEW.measurement_type,
    file_location_in := NEW.file_location,
    assay_in := NEW.assay
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_measurements_id(
  measurement_type_in PHYSICAL_OR_CHEMICAL,
  file_location_in TEXT,
  assay_in TEXT
) RETURNS UUID AS $$
DECLARE
  mid UUID;
BEGIN

  SELECT
    measurements_id INTO mid
  FROM
    measurements m
  WHERE
    measurement_type = measurement_type_in AND
    file_location = file_location_in AND
    assay = assay_in;

  IF (mid IS NULL) THEN
    RAISE EXCEPTION 'Unknown measurements: measurement_type="%" file_location="%" assay="%"', measurement_type_in, file_location_in, assay_in;
  END IF;

  RETURN mid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER measurements_insert_trig
  INSTEAD OF INSERT ON
  measurements_view FOR EACH ROW
  EXECUTE PROCEDURE insert_measurements_from_trig();

CREATE TRIGGER measurements_update_trig
  INSTEAD OF UPDATE ON
  measurements_view FOR EACH ROW
  EXECUTE PROCEDURE update_measurements_from_trig();
