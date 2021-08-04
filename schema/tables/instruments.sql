-- TABLE
DROP TABLE IF EXISTS instruments CASCADE;
CREATE TABLE instruments (
  instruments_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  make INSTRUMENT_MAKE NOT NULL,
  model INSTRUMENT_MODEL NOT NULL,
  serial_number TEXT UNIQUE NOT NULL,
  type INSTRUMENT_TYPE NOT NULL,
  UNIQUE(make, model, type)
);
CREATE INDEX instruments_source_id_idx ON instruments(source_id);

-- VIEW
CREATE OR REPLACE VIEW instruments_view AS
  SELECT
    i.instruments_id AS instruments_id,
    i.make as make,
    i.model as model,
    i.serial_number as serial_number,
    i.type as type,

    sc.name AS source_name
  FROM
    instruments i
LEFT JOIN source sc ON i.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_instruments (
  instruments_id UUID,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  serial_number TEXT,
  type INSTRUMENT_TYPE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( instruments_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO instruments_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO instruments (
    instruments_id, make, model, serial_number, type, source_id
  ) VALUES (
    instruments_id, make, model, serial_number, type, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_instruments (
  instruments_id_in UUID,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE) RETURNS void AS $$
BEGIN

  UPDATE instruments SET (
    make, model, serial_number, type
  ) = (
    make_in, model_in, serial_number_in, type_in
  ) WHERE
    instruments_id = instruments_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_instruments_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_instruments(
    instruments_id := NEW.instruments_id,
    make := NEW.make,
    model := NEW.model,
    serial_number := NEW.serial_number,
    type := NEW.type,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_instruments_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_instruments(
    instruments_id_in := NEW.instruments_id,
    make_in := NEW.make,
    model_in := NEW.model,
    serial_number_in := NEW.serial_number,
    type_in := NEW.type
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_instruments_id(make_in INSTRUMENT_MAKE, model_in INSTRUMENT_MODEL, serial_number_in TEXT, type_in INSTRUMENT_TYPE) RETURNS UUID AS $$
DECLARE
  iid UUID;
BEGIN

  SELECT
    instruments_id INTO iid
  FROM
    instruments i
  WHERE
    make = make_in AND
    model = model_in AND
    serial_number = serial_number_in AND
    type = type_in;

  IF (iid IS NULL) THEN
    RAISE EXCEPTION 'Unknown instruments: serial_number="%"', serial_number_in;
  END IF;

  RETURN iid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER instruments_insert_trig
  INSTEAD OF INSERT ON
  instruments_view FOR EACH ROW
  EXECUTE PROCEDURE insert_instruments_from_trig();

CREATE TRIGGER instruments_update_trig
  INSTEAD OF UPDATE ON
  instruments_view FOR EACH ROW
  EXECUTE PROCEDURE update_instruments_from_trig();
