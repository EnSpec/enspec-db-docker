-- TABLE
DROP TABLE IF EXISTS calibration CASCADE;
CREATE TABLE calibration (
  calibration_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  calib_date DATE NOT NULL,
  calib_facility TEXT NOT NULL,
  calib_technician TEXT NOT NULL,
  calib_file TEXT NOT NULL,
  expiration_date DATE NOT NULL
);
CREATE INDEX calibration_source_id_idx ON calibration(source_id);

-- VIEW
CREATE OR REPLACE VIEW calibration_view AS
  SELECT
    c.calibration_id AS calibration_id,
    c.calib_date  as calib_date,
    c.calib_facility  as calib_facility,
    c.calib_technician  as calib_technician,
    c.calib_file  as calib_file,
    c.expiration_date  as expiration_date,

    sc.name AS source_name
  FROM
    calibration c
LEFT JOIN source sc ON c.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_calibration (
  calibration_id UUID,
  calib_date DATE,
  calib_facility TEXT,
  calib_technician TEXT,
  calib_file TEXT,
  expiration_date DATE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( calibration_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO calibration_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO calibration (
    calibration_id, calib_date, calib_facility, calib_technician, calib_file, expiration_date, source_id
  ) VALUES (
    calibration_id, calib_date, calib_facility, calib_technician, calib_file, expiration_date, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_calibration (
  calibration_id_in UUID,
  calib_date_in DATE,
  calib_facility_in TEXT,
  calib_technician_in TEXT,
  calib_file_in TEXT,
  expiration_date_in DATE) RETURNS void AS $$

BEGIN

  UPDATE calibration SET (
    calib_date, calib_facility, calib_technician, calib_file, expiration_date
  ) = (
    calib_date_in, calib_facility_in, calib_technician_in, calib_file_in, expiration_date_in
  ) WHERE
    calibration_id = calibration_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_calibration_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_calibration(
    calibration_id := NEW.calibration_id,
    calib_date := NEW.calib_date,
    calib_facility := NEW.calib_facility,
    calib_technician := NEW.calib_technician,
    calib_file := NEW.calib_file,
    expiration_date := NEW.expiration_date,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_calibration_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_calibration(
    calibration_id_in := NEW.calibration_id,
    calib_date_in := NEW.calib_date,
    calib_facility_in := NEW.calib_facility,
    calib_technician_in := NEW.calib_technician,
    calib_file_in := NEW.calib_file,
    expiration_date_in := NEW.expiration_date
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_calibration_id(calib_date_in date, calib_facility_in text, calib_technician_in text, calib_file_in text, expiration_date_in date) RETURNS UUID AS $$
DECLARE
  cid UUID;
BEGIN

  SELECT
    calibration_id INTO cid
  FROM
    calibration c
  WHERE
    calib_date = calib_date_in AND
    calib_facility = calib_facility_in AND
    calib_technician = calib_technician_in AND
    calib_file = calib_file_in AND
    expiration_date = expiration_date_in;

  IF (cid IS NULL) THEN
    RAISE EXCEPTION 'Unknown calibration: calib_date="%" calib_facility="%" calib_technician="%" calib_file="%" expiration_date="%"',
    calib_date_in, calib_facility_in, calib_technician_in, calib_file_in, expiration_date_in;
  END IF;

  RETURN cid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER calibration_insert_trig
  INSTEAD OF INSERT ON
  calibration_view FOR EACH ROW
  EXECUTE PROCEDURE insert_calibration_from_trig();

CREATE TRIGGER calibration_update_trig
  INSTEAD OF UPDATE ON
  calibration_view FOR EACH ROW
  EXECUTE PROCEDURE update_calibration_from_trig();
