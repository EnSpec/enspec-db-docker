-- TABLE
DROP TABLE IF EXISTS instrument_specification CASCADE;
CREATE TABLE instrument_specification (
  instrument_specification_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  instruments_id UUID REFERENCES instruments NOT NULL,
  specifications_id UUID REFERENCES specifications NOT NULL,
  UNIQUE(instruments_id, specifications_id)
);
CREATE INDEX instrument_specification_source_id_idx ON instrument_specification(source_id);

-- VIEW
CREATE OR REPLACE VIEW instrument_specification_view AS
  SELECT
    i.instrument_specification_id AS instrument_specification_id,
    inst.make  as make,
    inst.model  as model,
    inst.serial_number  as serial_number,
    inst.type  as type,
    sp.name  as name,
    sp.value_ranges  as value_ranges,
    sp.other_details  as other_details,

    sc.name AS source_name
  FROM
    instrument_specification i
LEFT JOIN source sc ON i.source_id = sc.source_id
LEFT JOIN instruments inst ON i.instruments_id = inst.instruments_id
LEFT JOIN specifications sp ON i.specifications_id = sp.specifications_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_instrument_specification (
  instrument_specification_id UUID,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  serial_number TEXT,
  type INSTRUMENT_TYPE,
  name TEXT,
  value_ranges TEXT,
  other_details TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  inst_id UUID;
  sp_id UUID;
BEGIN

  IF( instrument_specification_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO instrument_specification_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_instruments_id(make, model, serial_number, type) INTO inst_id;
  SELECT get_specifications_id(name, value_ranges, other_details) INTO sp_id;

  INSERT INTO instrument_specification (
    instrument_specification_id, instruments_id, specifications_id, source_id
  ) VALUES (
    instrument_specification_id, inst_id, sp_id, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_instrument_specification (
  instrument_specification_id_in UUID,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE,
  name_in TEXT,
  value_ranges_in TEXT,
  other_details_in TEXT) RETURNS void AS $$
DECLARE
  inst_id UUID;
  sp_id UUID;

BEGIN

  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO inst_id;
  SELECT get_specifications_id(name_in, value_ranges_in, other_details_in) INTO sp_id;

  UPDATE instrument_specification SET (
    instruments_id, specifications_id
  ) = (
    instruments_id_in, specifications_id_in
  ) WHERE
    instrument_specification_id = instrument_specification_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_instrument_specification_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_instrument_specification(
    instrument_specification_id := NEW.instrument_specification_id,
    make := NEW.make,
    model := NEW.model,
    serial_number := NEW.serial_number,
    type := NEW.type,
    name := NEW.name,
    value_ranges := NEW.value_ranges,
    other_details := NEW.other_details,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_instrument_specification_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_instrument_specification(
    instrument_specification_id_in := NEW.instrument_specification_id,
    make_in := NEW.make,
    model_in := NEW.model,
    serial_number_in := NEW.serial_number,
    type_in := NEW.type,
    name_in := NEW.name,
    value_ranges_in := NEW.value_ranges,
    other_details_in := NEW.other_details
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_instrument_specification_id(
  make INSTRUMENT_MAKE, model INSTRUMENT_MODEL, serial_number TEXT, type INSTRUMENT_TYPE, name TEXT, value_ranges TEXT, other_details TEXT) RETURNS UUID AS $$
DECLARE
  iid UUID;
  inst_id UUID;
  sp_id UUID;
BEGIN

  SELECT get_instruments_id(make, model, serial_number, type) INTO inst_id;
  SELECT get_specifications_id(name, value_ranges, other_details) INTO sp_id;
  SELECT
    instrument_specification_id INTO iid
  FROM
    instrument_specification i
  WHERE
    instruments_id = inst_id AND
    specifications_id = sp_id;

  IF (iid IS NULL) THEN
    RAISE EXCEPTION 'Unknown instrument_specification: make="%" model="%" serial_number="%" type="%" name="%" value_ranges="%" other_details="%"', make, model, serial_number, type, name, value_ranges, other_details;
  END IF;
  
  RETURN iid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER instrument_specification_insert_trig
  INSTEAD OF INSERT ON
  instrument_specification_view FOR EACH ROW
  EXECUTE PROCEDURE insert_instrument_specification_from_trig();

CREATE TRIGGER instrument_specification_update_trig
  INSTEAD OF UPDATE ON
  instrument_specification_view FOR EACH ROW
  EXECUTE PROCEDURE update_instrument_specification_from_trig();
