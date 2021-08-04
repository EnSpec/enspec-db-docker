-- TABLE
DROP TABLE IF EXISTS instrument_install CASCADE;
CREATE TABLE instrument_install (
  instrument_install_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  instruments_id UUID REFERENCES instruments NOT NULL,
  install_id UUID REFERENCES installations NOT NULL,
  calibration_id UUID REFERENCES calibration UNIQUE
);
CREATE INDEX instrument_install_source_id_idx ON instrument_install(source_id);
CREATE INDEX instrument_install_instruments_id_idx ON instrument_install(instruments_id);
CREATE INDEX instrument_install_install_id_idx ON instrument_install(install_id);
CREATE INDEX instrument_install_calibration_id_idx ON instrument_install(calibration_id);

-- VIEW
CREATE OR REPLACE VIEW instrument_install_view AS
  SELECT
    i.instrument_install_id AS instrument_install_id,
    instr.make as make,
    instr.model as model,
    instr.serial_number as serial_number,
    instr.type as type,
    instl.install_date  as install_date,
    instl.removal_date  as removal_date,
    instl.dir_location  as dir_location,
    c.calib_date  as calib_date,
    c.calib_facility  as calib_facility,
    c.calib_technician  as calib_technician,
    c.calib_file  as calib_file,
    c.expiration_date  as expiration_date,
    sc.name AS source_name
  FROM
    instrument_install i
LEFT JOIN instruments instr ON i.instruments_id = instr.instruments_id
LEFT JOIN installations instl ON i.install_id = instl.installations_id
LEFT JOIN calibration c ON i.calibration_id = c.calibration_id
LEFT JOIN source sc ON i.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_instrument_install (
  instrument_install_id UUID,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  serial_number TEXT,
  type INSTRUMENT_TYPE,
  install_date DATE,
  removal_date DATE,
  dir_location TEXT,
  calib_date DATE,
  calib_facility TEXT,
  calib_technician TEXT,
  calib_file TEXT,
  expiration_date DATE,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  instr_id UUID;
  instl_id UUID;
  cid UUID;
BEGIN

  IF( instrument_install_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO instrument_install_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_instruments_id(make, model, serial_number, type) INTO instr_id;
  SELECT get_installations_id(install_date, removal_date, dir_location) INTO instl_id;
  SELECT get_calibration_id(calib_date, calib_facility, calib_technician, calib_file, expiration_date) INTO cid;

  INSERT INTO instrument_install (
    instrument_install_id, instruments_id, install_id, calibration_id, source_id
  ) VALUES (
    instrument_install_id, instr_id, instl_id, cid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_instrument_install (
  instrument_install_id_in UUID,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  serial_number TEXT,
  type INSTRUMENT_TYPE,
  install_date DATE,
  removal_date DATE,
  dir_location TEXT,
  calib_date DATE,
  calib_facility TEXT,
  calib_technician TEXT,
  calib_file TEXT,
  expiration_date DATE) RETURNS void AS $$
DECLARE
  instr_id UUID;
  instl_id UUID;
  cid UUID;

BEGIN
  SELECT get_instruments_id(make, model, serial_number, type) INTO instr_id;
  SELECT get_installations_id(install_date, removal_date, dir_location) INTO instl_id;
  SELECT get_calibration_id(calib_date, calib_facility, calib_technician, calib_file, expiration_date) INTO cid;

  UPDATE instrument_install SET (
    instruments_id, install_id, calibration_id
  ) = (
    instr_id, instl_id, cid
  ) WHERE
    instrument_install_id = instrument_install_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_instrument_install_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_instrument_install(
    instrument_install_id := NEW.instrument_install_id,
    make := NEW.make,
    model := NEW.model,
    serial_number := NEW.serial_number,
    type := NEW.type,
    install_date := NEW.install_date,
    removal_date := NEW.removal_date,
    dir_location := NEW.dir_location,
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

CREATE OR REPLACE FUNCTION update_instrument_install_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_instrument_install(
    instrument_install_id_in := NEW.instrument_install_id,
    make := NEW.make,
    model := NEW.model,
    serial_number := NEW.serial_number,
    type := NEW.type,
    install_date := NEW.install_date,
    removal_date := NEW.removal_date,
    dir_location := NEW.dir_location,
    calib_date := NEW.calib_date,
    calib_facility := NEW.calib_facility,
    calib_technician := NEW.calib_technician,
    calib_file := NEW.calib_file,
    expiration_date := NEW.expiration_date
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_instrument_install_id(
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  serial_number text,
  type INSTRUMENT_TYPE,
  install_date date,
  removal_date date,
  dir_location text,
  calib_date date,
  calib_facility text,
  calib_technician text,
  calib_file text,
  expiration_date date) RETURNS UUID AS $$
DECLARE
  iid UUID;
  instr_id UUID;
  instl_id UUID;
  cid UUID;
BEGIN
  SELECT get_instruments_id(make, model, serial_number, type) INTO instr_id;
  SELECT get_installations_id(install_date, removal_date, dir_location) INTO instl_id;
  SELECT get_calibration_id(calib_date, calib_facility, calib_technician, calib_file, expiration_date) INTO cid;
  IF (cid is NULL) THEN
    SELECT
      instrument_install_id INTO iid
    FROM
      instrument_install i
    WHERE
      instruments_id = instr_id AND
      install_id = instl_id;
  ELSE
    SELECT
      instrument_install_id INTO iid
    FROM
      instrument_install i
    WHERE
      instruments_id = instr_id AND
      install_id = instl_id AND
      calibration_id = cid;
  END IF;

  IF (iid IS NULL) THEN
    RAISE EXCEPTION 'Unknown instrument_install: make="%" model="%" serial_number="%" type="%" install_date="%"
    removal_date="%" dir_location="%" calib_date="%" calib_facility="%" calib_technician="%" calib_file="%" expiration_date="%"',
    make, model, serial_number, type, install_date, removal_date, dir_location, calib_date, calib_facility, calib_technician, calib_file, expiration_date;
  END IF;

  RETURN iid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER instrument_install_insert_trig
  INSTEAD OF INSERT ON
  instrument_install_view FOR EACH ROW
  EXECUTE PROCEDURE insert_instrument_install_from_trig();

CREATE TRIGGER instrument_install_update_trig
  INSTEAD OF UPDATE ON
  instrument_install_view FOR EACH ROW
  EXECUTE PROCEDURE update_instrument_install_from_trig();
