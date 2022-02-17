-- TABLE
DROP TABLE IF EXISTS boresight_offsets CASCADE;
CREATE TABLE boresight_offsets (
  boresight_offsets_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  calculation_method TEXT  NOT NULL,
  roll_offset FLOAT NOT NULL,
  pitch_offset FLOAT NOT NULL,
  heading_offset FLOAT NOT NULL,
  rmse FLOAT NOT NULL,
  gcp_file TEXT NOT NULL
);
CREATE INDEX boresight_offsets_source_id_idx ON boresight_offsets(source_id);

ALTER TABLE boresight_offsets ADD CONSTRAINT uniq_bo_row UNIQUE(calculation_method, roll_offset, pitch_offset, heading_offset, rmse, gcp_file);

-- VIEW
CREATE OR REPLACE VIEW boresight_offsets_view AS
  SELECT
    b.boresight_offsets_id AS boresight_offsets_id,
    b.calculation_method  as calculation_method,
    b.roll_offset  as roll_offset,
    b.pitch_offset  as pitch_offset,
    b.heading_offset  as heading_offset,
    b.rmse  as rmse,
    b.gcp_file  as gcp_file,

    sc.name AS source_name
  FROM
    boresight_offsets b
LEFT JOIN source sc ON b.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_boresight_offsets (
  boresight_offsets_id UUID,
  calculation_method TEXT,
  roll_offset FLOAT,
  pitch_offset FLOAT,
  heading_offset FLOAT,
  rmse FLOAT,
  gcp_file TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( boresight_offsets_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO boresight_offsets_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO boresight_offsets (
    boresight_offsets_id, calculation_method, roll_offset, pitch_offset, heading_offset, rmse, gcp_file, source_id
  ) VALUES (
    boresight_offsets_id, calculation_method, roll_offset, pitch_offset, heading_offset, rmse, gcp_file, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_boresight_offsets (
  boresight_offsets_id_in UUID,
  calculation_method_in TEXT,
  roll_offset_in FLOAT,
  pitch_offset_in FLOAT,
  heading_offset_in FLOAT,
  rmse_in FLOAT,
  gcp_file_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE boresight_offsets SET (
    calculation_method, roll_offset, pitch_offset, heading_offset, rmse, gcp_file
  ) = (
    calculation_method_in, roll_offset_in, pitch_offset_in, heading_offset_in, rmse_in, gcp_file_in
  ) WHERE
    boresight_offsets_id = boresight_offsets_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_boresight_offsets_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_boresight_offsets(
    boresight_offsets_id := NEW.boresight_offsets_id,
    calculation_method := NEW.calculation_method,
    roll_offset := NEW.roll_offset,
    pitch_offset := NEW.pitch_offset,
    heading_offset := NEW.heading_offset,
    rmse := NEW.rmse,
    gcp_file := NEW.gcp_file,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_boresight_offsets_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_boresight_offsets(
    boresight_offsets_id_in := NEW.boresight_offsets_id,
    calculation_method_in := NEW.calculation_method,
    roll_offset_in := NEW.roll_offset,
    pitch_offset_in := NEW.pitch_offset,
    heading_offset_in := NEW.heading_offset,
    rmse_in := NEW.rmse,
    gcp_file_in := NEW.gcp_file
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_boresight_offsets_id(
  calculation_method_in TEXT,
  roll_offset_in FLOAT,
  pitch_offset_in FLOAT,
  heading_offset_in FLOAT,
  rmse_in FLOAT,
  gcp_file_in TEXT) RETURNS UUID AS $$
DECLARE
  bid UUID;
BEGIN

  SELECT
    boresight_offsets_id INTO bid
  FROM
    boresight_offsets b
  WHERE
    calculation_method = calculation_method_in AND
    roll_offset = roll_offset_in AND
    pitch_offset = pitch_offset_in AND
    heading_offset = heading_offset_in AND
    rmse = rmse_in AND
    gcp_file = gcp_file_in;

  IF (bid IS NULL) THEN
    RAISE EXCEPTION 'Unknown boresight_offsets: calculation_method="%" roll_offset="%" pitch_offset="%" heading_offset="%" rmse="%" gcp_file="%"',
    calculation_method_in, roll_offset_in, pitch_offset_in, heading_offset_in, rmse_in, gcp_file_in;
  END IF;

  RETURN bid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER boresight_offsets_insert_trig
  INSTEAD OF INSERT ON
  boresight_offsets_view FOR EACH ROW
  EXECUTE PROCEDURE insert_boresight_offsets_from_trig();

CREATE TRIGGER boresight_offsets_update_trig
  INSTEAD OF UPDATE ON
  boresight_offsets_view FOR EACH ROW
  EXECUTE PROCEDURE update_boresight_offsets_from_trig();
