-- TABLE
DROP TABLE IF EXISTS spectra CASCADE;
CREATE TABLE spectra (
  spectra_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  spectrum_type FRESH_OR_DRY NOT NULL,
  leaf_number INTEGER NOT NULL,
  specdal_version TEXT NOT NULL,
  spectra_group_naming TEXT NOT NULL,
  raw_file_location TEXT NOT NULL,
  processing_spectra_loc TEXT NOT NULL
);
CREATE INDEX spectra_source_id_idx ON spectra(source_id);

-- VIEW
CREATE OR REPLACE VIEW spectra_view AS
  SELECT
    s.spectra_id AS spectra_id,
    s.spectrum_type  as spectrum_type,
    s.leaf_number  as leaf_number,
    s.specdal_version  as specdal_version,
    s.spectra_group_naming  as spectra_group_naming,
    s.raw_file_location  as raw_file_location,
    s.processing_spectra_loc  as processing_spectra_loc,

    sc.name AS source_name
  FROM
    spectra s
LEFT JOIN source sc ON s.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_spectra (
  spectra_id UUID,
  spectrum_type FRESH_OR_DRY,
  leaf_number INTEGER,
  specdal_version TEXT,
  spectra_group_naming TEXT,
  raw_file_location TEXT,
  processing_spectra_loc TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( spectra_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO spectra_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO spectra (
    spectra_id, spectrum_type, leaf_number, specdal_version, spectra_group_naming, raw_file_location, processing_spectra_loc, source_id
  ) VALUES (
    spectra_id, spectrum_type, leaf_number, specdal_version, spectra_group_naming, raw_file_location, processing_spectra_loc, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_spectra (
  spectra_id_in UUID,
  spectrum_type_in FRESH_OR_DRY,
  leaf_number_in INTEGER,
  specdal_version_in TEXT,
  spectra_group_naming_in TEXT,
  raw_file_location_in TEXT,
  processing_spectra_loc_in TEXT) RETURNS void AS $$
DECLARE

BEGIN

  UPDATE spectra SET (
    spectrum_type, leaf_number, specdal_version, spectra_group_naming, raw_file_location, processing_spectra_loc
  ) = (
    spectrum_type_in, leaf_number_in, specdal_version_in, spectra_group_naming_in, raw_file_location_in, processing_spectra_loc_in
  ) WHERE
    spectra_id = spectra_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_spectra_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_spectra(
    spectra_id := NEW.spectra_id,
    spectrum_type := NEW.spectrum_type,
    leaf_number := NEW.leaf_number,
    specdal_version := NEW.specdal_version,
    spectra_group_naming := NEW.spectra_group_naming,
    raw_file_location := NEW.raw_file_location,
    processing_spectra_loc := NEW.processing_spectra_loc,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_spectra_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_spectra(
    spectra_id_in := NEW.spectra_id,
    spectrum_type_in := NEW.spectrum_type,
    leaf_number_in := NEW.leaf_number,
    specdal_version_in := NEW.specdal_version,
    spectra_group_naming_in := NEW.spectra_group_naming,
    raw_file_location_in := NEW.raw_file_location,
    processing_spectra_loc_in := NEW.processing_spectra_loc
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_spectra_id(
  spectrum_type_in FRESH_OR_DRY,
  leaf_number_in INTEGER,
  specdal_version_in TEXT,
  spectra_group_naming_in TEXT,
  raw_file_location_in TEXT,
  processing_spectra_loc_in TEXT
) RETURNS UUID AS $$
DECLARE
  sid UUID;
BEGIN

  SELECT
    spectra_id INTO sid
  FROM
    spectra s
  WHERE
    spectrum_type = spectrum_type_in AND
    leaf_number = leaf_number_in AND
    specdal_version = specdal_version_in AND
    spectra_group_naming = spectra_group_naming_in AND
    raw_file_location = raw_file_location_in AND
    processing_spectra_loc = processing_spectra_loc_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown spectra: spectrum_type="%" leaf_number="%" specdal_version="%" spectra_group_naming="%" raw_file_location="%" processing_spectra_loc="%"', 
    spectrum_type_in, leaf_number_in, specdal_version_in, spectra_group_naming_in, raw_file_location_in, processing_spectra_loc_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER spectra_insert_trig
  INSTEAD OF INSERT ON
  spectra_view FOR EACH ROW
  EXECUTE PROCEDURE insert_spectra_from_trig();

CREATE TRIGGER spectra_update_trig
  INSTEAD OF UPDATE ON
  spectra_view FOR EACH ROW
  EXECUTE PROCEDURE update_spectra_from_trig();
