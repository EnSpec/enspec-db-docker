-- TABLE
DROP TABLE IF EXISTS samples_species CASCADE;
CREATE TABLE samples_species (
  samples_species_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  samples_id UUID REFERENCES samples NOT NULL,
  species_id UUID REFERENCES species NOT NULL
);
CREATE INDEX samples_species_source_id_idx ON samples_species(source_id);
CREATE INDEX samples_species_samples_id_idx ON samples_species(samples_id);
CREATE INDEX samples_species_species_id_idx ON samples_species(species_id);

-- VIEW
CREATE OR REPLACE VIEW samples_species_view AS
  SELECT
    s.samples_species_id AS samples_species_id,
    sam.sample_alive  as sample_alive,
    sam.physical_storage  as physical_storage,
    sam.sample_notes  as sample_notes,
    sp.common_name  as common_name,
    sp.family  as family,
    sp.genus  as genus,
    sp.species  as species,
    sp.authority  as authority,
    sp.synonym_symbol  as synonym_symbol,
    sp.subspecies  as subspecies,
    sp.symbol  as symbol,
    sc.name AS source_name
  FROM
    samples_species s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN samples sam ON s.samples_id = sam.samples_id
LEFT JOIN species sp ON s.species_id = sp.species_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_samples_species (
  samples_species_id UUID,
  sample_alive BOOL,
  physical_storage SAMPLE_STORAGE,
  sample_notes TEXT,
  common_name TEXT,
  family TEXT,
  genus TEXT,
  species TEXT,
  authority TEXT,
  synonym_symbol TEXT,
  subspecies TEXT,
  symbol TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  samid UUID;
  spid UUID;
BEGIN
  SELECT get_samples_id(sample_alive, physical_storage, sample_notes) INTO samid;
  SELECT get_species_id(common_name, family, genus, species, authority, synonym_symbol, subspecies, symbol) INTO spid;
  IF( samples_species_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO samples_species_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO samples_species (
    samples_species_id, samples_id, species_id, source_id
  ) VALUES (
    samples_species_id, samid, spid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_samples_species (
  samples_species_id_in UUID,
  sample_alive_in BOOL,
  physical_storage_in SAMPLE_STORAGE,
  sample_notes_in TEXT,
  common_name_in TEXT,
  family_in TEXT,
  genus_in TEXT,
  species_in TEXT,
  authority_in TEXT,
  synonym_symbol_in TEXT,
  subspecies_in TEXT,
  symbol_in TEXT
  ) RETURNS void AS $$
DECLARE
samid UUID;
spid UUID;
BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_species_id(common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in) INTO spid;
  UPDATE samples_species SET (
    samples_id, species_id
  ) = (
    samid, spid
  ) WHERE
    samples_species_id = samples_species_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_samples_species_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_samples_species(
    samples_species_id := NEW.samples_species_id,
    sample_alive := NEW.sample_alive,
    physical_storage := NEW.physical_storage,
    sample_notes := NEW.sample_notes,
    common_name := NEW.common_name,
    family := NEW.family,
    genus := NEW.genus,
    species := NEW.species,
    authority := NEW.authority,
    synonym_symbol := NEW.synonym_symbol,
    subspecies := NEW.subspecies,
    symbol := NEW.symbol,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_samples_species_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_samples_species(
    samples_species_id_in := NEW.samples_species_id,
    sample_alive_in := NEW.sample_alive,
    physical_storage_in := NEW.physical_storage,
    sample_notes_in := NEW.sample_notes,
    common_name_in := NEW.common_name,
    family_in := NEW.family,
    genus_in := NEW.genus,
    species_in := NEW.species,
    authority_in := NEW.authority,
    synonym_symbol_in := NEW.synonym_symbol,
    subspecies_in := NEW.subspecies,
    symbol_in := NEW.symbol
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_samples_species_id(
  sample_alive_in bool,
  physical_storage_in sample_storage,
  sample_notes_in text,
  common_name_in TEXT,
  family_in TEXT,
  genus_in TEXT,
  species_in TEXT,
  authority_in TEXT,
  synonym_symbol_in TEXT,
  subspecies_in TEXT,
  symbol_in TEXT
) RETURNS UUID AS $$
DECLARE
  sid UUID;
  samid UUID;
  spid UUID;
BEGIN
  SELECT get_samples_id(sample_alive_in, physical_storage_in, sample_notes_in) INTO samid;
  SELECT get_species_id(common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in) INTO spid;
  SELECT
    samples_species_id INTO sid
  FROM
    samples_species s
  WHERE
    samples_id = samid AND
    species_id = spid;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown samples_species: sample_alive="%" physical_storage="%" sample_notes="%"
    common_name="%" family="%" genus="%" species="%" authority="%" synonym_symbol="%" subspecies="%" symbol="%"',
    sample_alive_in, physical_storage_in, sample_notes_in, common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER samples_species_insert_trig
  INSTEAD OF INSERT ON
  samples_species_view FOR EACH ROW
  EXECUTE PROCEDURE insert_samples_species_from_trig();

CREATE TRIGGER samples_species_update_trig
  INSTEAD OF UPDATE ON
  samples_species_view FOR EACH ROW
  EXECUTE PROCEDURE update_samples_species_from_trig();
