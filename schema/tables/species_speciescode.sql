-- TABLE
DROP TABLE IF EXISTS species_speciescode CASCADE;
CREATE TABLE species_speciescode (
  species_speciescode_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  species_id UUID REFERENCES species NOT NULL,
  species_code_id UUID REFERENCES species_code NOT NULL
);
CREATE INDEX species_speciescode_source_id_idx ON species_speciescode(source_id);
CREATE INDEX species_speciescode_species_id_idx ON species_speciescode(species_id);
CREATE INDEX species_speciescode_speciescode_id_idx ON species_speciescode(species_code_id);

-- VIEW
CREATE OR REPLACE VIEW species_speciescode_view AS
  SELECT
    s.species_speciescode_id AS species_speciescode_id,
    sp.common_name  as common_name,
    sp.family  as family,
    sp.genus  as genus,
    sp.species  as species,
    sp.authority  as authority,
    sp.synonym_symbol  as synonym_symbol,
    sp.subspecies  as subspecies,
    sp.symbol  as symbol,
    spc.cn_number_fia  as cn_number_fia,
    spc.code_type  as code_type,
    spc.custom_code  as custom_code,
    sc.name AS source_name
  FROM
    species_speciescode s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN species sp ON s.species_id = sp.species_id
LEFT JOIN species_code spc ON s.species_code_id = spc.species_code_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_species_speciescode (
  species_speciescode_id UUID,
  common_name TEXT,
  family TEXT,
  genus TEXT,
  species TEXT,
  authority TEXT,
  synonym_symbol TEXT,
  subspecies TEXT,
  symbol TEXT,
  cn_number_fia TEXT,
  code_type SPECIES_CODE_TYPE,
  custom_code TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  spid UUID;
  spcid UUID;
BEGIN
  SELECT get_species_id(common_name, family, genus, species, authority, synonym_symbol, subspecies, symbol) INTO spid;
  SELECT get_species_code_id(cn_number_fia, code_type, custom_code) INTO spcid;
  IF( species_speciescode_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO species_speciescode_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO species_speciescode (
    species_speciescode_id, species_id, species_code_id, source_id
  ) VALUES (
    species_speciescode_id, spid, spcid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_species_speciescode (
  species_speciescode_id_in UUID,
  common_name_in TEXT,
  family_in TEXT,
  genus_in TEXT,
  species_in TEXT,
  authority_in TEXT,
  synonym_symbol_in TEXT,
  subspecies_in TEXT,
  symbol_in TEXT,
  cn_number_fia_in TEXT,
  code_type_in SPECIES_CODE_TYPE,
  custom_code_in TEXT
) RETURNS void AS $$
DECLARE
spid UUID;
spcid UUID;

BEGIN
  SELECT get_species_id(common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in) INTO spid;
  SELECT get_species_code_id(cn_number_fia_in, code_type_in, custom_code_in) INTO spcid;

  UPDATE species_speciescode SET (
    species_id, species_code_id
  ) = (
    spid, spcid
  ) WHERE
    species_speciescode_id = species_speciescode_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_species_speciescode_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_species_speciescode(
    species_speciescode_id := NEW.species_speciescode_id,
    common_name := NEW.common_name,
    family := NEW.family,
    genus := NEW.genus,
    species := NEW.species,
    authority := NEW.authority,
    synonym_symbol := NEW.synonym_symbol,
    subspecies := NEW.subspecies,
    symbol := NEW.symbol,
    cn_number_fia := NEW.cn_number_fia,
    code_type := NEW.code_type,
    custom_code := NEW.custom_code,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_species_speciescode_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_species_speciescode(
    species_speciescode_id_in := NEW.species_speciescode_id,
    common_name_in := NEW.common_name,
    family_in := NEW.family,
    genus_in := NEW.genus,
    species_in := NEW.species,
    authority_in := NEW.authority,
    synonym_symbol_in := NEW.synonym_symbol,
    subspecies_in := NEW.subspecies,
    symbol_in := NEW.symbol,
    cn_number_fia_in := NEW.cn_number_fia,
    code_type_in := NEW.code_type,
    custom_code_in := NEW.custom_code
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_species_speciescode_id(
  common_name_in TEXT,
  family_in TEXT,
  genus_in TEXT,
  species_in TEXT,
  authority_in TEXT,
  synonym_symbol_in TEXT,
  subspecies_in TEXT,
  symbol_in TEXT,
  cn_number_fia_in text,
  code_type_in SPECIES_CODE_TYPE,
  custom_code_in text
) RETURNS UUID AS $$
DECLARE
  sid UUID;
  spid UUID;
  spcid UUID;
BEGIN
  SELECT get_species_id(common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in) INTO spid;
  SELECT get_species_code_id(cn_number_fia_in, code_type_in, custom_code_in) INTO spcid;

  SELECT
    species_speciescode_id INTO sid
  FROM
    species_speciescode s
  WHERE
    species_id = spid AND
    species_code_id = spcid;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown species_speciescode: common_name="%" family="%" genus="%" species="%" authority="%" synonym_symbol="%" subspecies="%" symbol="%"
    cn_number_fia="%" code_type="%" custom_code="%"', common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in,
    cn_number_fia_in, code_type_in, custom_code_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER species_speciescode_insert_trig
  INSTEAD OF INSERT ON
  species_speciescode_view FOR EACH ROW
  EXECUTE PROCEDURE insert_species_speciescode_from_trig();

CREATE TRIGGER species_speciescode_update_trig
  INSTEAD OF UPDATE ON
  species_speciescode_view FOR EACH ROW
  EXECUTE PROCEDURE update_species_speciescode_from_trig();
