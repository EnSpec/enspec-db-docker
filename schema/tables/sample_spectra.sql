-- TABLE
DROP TABLE IF EXISTS sample_spectra CASCADE;
CREATE TABLE sample_spectra (
  sample_spectra_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  samples_id UUID REFERENCES samples NOT NULL,
  spectra_id UUID REFERENCES spectra NOT NULL,
  spectrometer_id UUID REFERENCES instruments NOT NULL
);
CREATE INDEX sample_spectra_source_id_idx ON sample_spectra(source_id);
CREATE INDEX sample_spectra_samples_id_idx ON sample_spectra(samples_id);
CREATE INDEX sample_spectra_spectra_id_idx ON sample_spectra(spectra_id);
CREATE INDEX sample_spectra_spectrometer_id_idx ON sample_spectra(spectrometer_id);

-- VIEW
CREATE OR REPLACE VIEW sample_spectra_view AS
  SELECT
    s.sample_spectra_id AS sample_spectra_id,
    sam.sample_alive  as sample_alive,
    sam.physical_storage  as physical_storage,
    sam.sample_notes  as sample_notes,
    sp.spectrum_type  as spectrum_type,
    sp.leaf_number  as leaf_number,
    sp.specdal_version  as specdal_version,
    sp.spectra_group_naming  as spectra_group_naming,
    sp.raw_file_location  as raw_file_location,
    sp.processing_spectra_loc  as processing_spectra_loc,
    i.make as make,
    i.model as model,
    i.serial_number as serial_number,
    i.type as type,

    sc.name AS source_name
  FROM
    sample_spectra s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN samples sam ON s.samples_id = sam.samples_id
LEFT JOIN spectra sp ON s.spectra_id = sp.spectra_id
LEFT JOIN instruments i ON s.spectrometer_id = i.instruments_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_sample_spectra (
  sample_spectra_id UUID,
  sample_alive BOOL,
  physical_storage SAMPLE_STORAGE,
  sample_notes TEXT,
  spectrum_type FRESH_OR_DRY,
  leaf_number INTEGER,
  specdal_version TEXT,
  spectra_group_naming TEXT,
  raw_file_location TEXT,
  processing_spectra_loc TEXT,
  make INSTRUMENT_MAKE,
  model INSTRUMENT_MODEL,
  serial_number TEXT,
  type INSTRUMENT_TYPE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  samid UUID;
  spid UUID;
  iid UUID;
BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_spectra_id(spectrum_type_in, leaf_number_in, specdal_version_in, spectra_group_naming_in, raw_file_location_in, processing_spectra_loc_in) INTO spid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;

  IF( sample_spectra_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sample_spectra_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO sample_spectra (
    sample_spectra_id, samples_id, spectra_id, spectrometer_id, source_id
  ) VALUES (
    sample_spectra_id, samid, spid, iid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sample_spectra (
  sample_spectra_id_in UUID,
  sample_alive_in BOOL,
  physical_storage_in SAMPLE_STORAGE,
  sample_notes_in TEXT,
  spectrum_type_in FRESH_OR_DRY,
  leaf_number_in INTEGER,
  specdal_version_in TEXT,
  spectra_group_naming_in TEXT,
  raw_file_location_in TEXT,
  processing_spectra_loc_in TEXT,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE
) RETURNS void AS $$
DECLARE
samid UUID;
spid UUID;
iid UUID;

BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_spectra_id(spectrum_type_in, leaf_number_in, specdal_version_in, spectra_group_naming_in, raw_file_location_in, processing_spectra_loc_in) INTO spid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;

  UPDATE sample_spectra SET (
    samples_id, spectra_id, spectrometer_id
  ) = (
    samid, spid, iid
  ) WHERE
    sample_spectra_id = sample_spectra_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_sample_spectra_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_sample_spectra(
    sample_spectra_id := NEW.sample_spectra_id,
    sample_alive := NEW.sample_alive,
    physical_storage := NEW.physical_storage,
    sample_notes := NEW.sample_notes,
    spectrum_type := NEW.spectrum_type,
    leaf_number := NEW.leaf_number,
    specdal_version := NEW.specdal_version,
    spectra_group_naming := NEW.spectra_group_naming,
    raw_file_location := NEW.raw_file_location,
    processing_spectra_loc := NEW.processing_spectra_loc,
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

CREATE OR REPLACE FUNCTION update_sample_spectra_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_sample_spectra(
    sample_spectra_id_in := NEW.sample_spectra_id,
    sample_alive_in := NEW.sample_alive,
    physical_storage_in := NEW.physical_storage,
    sample_notes_in := NEW.sample_notes,
    spectrum_type_in := NEW.spectrum_type,
    leaf_number_in := NEW.leaf_number,
    specdal_version_in := NEW.specdal_version,
    spectra_group_naming_in := NEW.spectra_group_naming,
    raw_file_location_in := NEW.raw_file_location,
    processing_spectra_loc_in := NEW.processing_spectra_loc,
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
CREATE OR REPLACE FUNCTION get_sample_spectra_id(
  sample_alive_in bool,
  physical_storage_in sample_storage,
  sample_notes_in text,
  spectrum_type_in FRESH_OR_DRY,
  leaf_number_in INTEGER,
  specdal_version_in TEXT,
  spectra_group_naming_in TEXT,
  raw_file_location_in TEXT,
  processing_spectra_loc_in TEXT,
  make_in INSTRUMENT_MAKE,
  model_in INSTRUMENT_MODEL,
  serial_number_in TEXT,
  type_in INSTRUMENT_TYPE
) RETURNS UUID AS $$
DECLARE
  sid UUID;
  samid UUID;
  spid UUID;
  iid UUID;
BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_spectra_id(spectrum_type_in, leaf_number_in, specdal_version_in, spectra_group_naming_in, raw_file_location_in, processing_spectra_loc_in) INTO spid;
  SELECT get_instruments_id(make_in, model_in, serial_number_in, type_in) INTO iid;

  SELECT
    sample_spectra_id INTO sid
  FROM
    sample_spectra s
  WHERE
  samples_id = samid AND
  spectra_id = spid AND
  spectrometer_id = iid;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sample_spectra: sample_alive="%" physical_storage="%" sample_notes="%" spectrum_type="%" leaf_number="%"
    specdal_version="%" spectra_group_naming="%" raw_file_location="%" processing_spectra_loc="%" serial_number="%"', sample_alive_in,
    physical_storage_in, sample_notes_in, spectrum_type_in, leaf_number_in, specdal_version_in, spectra_group_naming_in, raw_file_location_in,
    processing_spectra_loc_in, serial_number_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER sample_spectra_insert_trig
  INSTEAD OF INSERT ON
  sample_spectra_view FOR EACH ROW
  EXECUTE PROCEDURE insert_sample_spectra_from_trig();

CREATE TRIGGER sample_spectra_update_trig
  INSTEAD OF UPDATE ON
  sample_spectra_view FOR EACH ROW
  EXECUTE PROCEDURE update_sample_spectra_from_trig();
