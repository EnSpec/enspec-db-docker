-- TABLE
DROP TABLE IF EXISTS species CASCADE;
CREATE TABLE species (
  species_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  common_name TEXT UNIQUE NOT NULL,
  family TEXT NOT NULL,
  genus TEXT NOT NULL,
  species TEXT UNIQUE NOT NULL,
  authority TEXT NOT NULL,
  synonym_symbol TEXT NOT NULL,
  subspecies TEXT NOT NULL,
  symbol TEXT NOT NULL
);
CREATE INDEX species_source_id_idx ON species(source_id);

-- VIEW
CREATE OR REPLACE VIEW species_view AS
  SELECT
    s.species_id AS species_id,
    s.common_name  as common_name,
    s.family  as family,
    s.genus  as genus,
    s.species  as species,
    s.authority  as authority,
    s.synonym_symbol  as synonym_symbol,
    s.subspecies  as subspecies,
    s.symbol  as symbol,

    sc.name AS source_name
  FROM
    species s
LEFT JOIN source sc ON s.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_species (
  species_id UUID,
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
BEGIN

  IF( species_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO species_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO species (
    species_id, common_name, family, genus, species, authority, synonym_symbol, subspecies, symbol, source_id
  ) VALUES (
    species_id, common_name, family, genus, species, authority, synonym_symbol, subspecies, symbol, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_species (
  species_id_in UUID,
  common_name_in TEXT,
  family_in TEXT,
  genus_in TEXT,
  species_in TEXT,
  authority_in TEXT,
  synonym_symbol_in TEXT,
  subspecies_in TEXT,
  symbol_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE species SET (
    common_name, family, genus, species, authority, synonym_symbol, subspecies, symbol
  ) = (
    common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in
  ) WHERE
    species_id = species_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_species_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_species(
    species_id := NEW.species_id,
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

CREATE OR REPLACE FUNCTION update_species_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_species(
    species_id_in := NEW.species_id,
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
CREATE OR REPLACE FUNCTION get_species_id(
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
BEGIN

  SELECT
    species_id INTO sid
  FROM
    species s
  WHERE
    common_name = common_name_in AND
    family = family_in AND
    genus = genus_in AND
    species = species_in AND
    authority = authority_in AND
    synonym_symbol = synonym_symbol_in AND
    subspecies = subspecies_in AND
    symbol = symbol_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown species: common_name="%" family="%" genus="%" species="%" authority="%" synonym_symbol="%" subspecies="%" symbol="%"',
    common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER species_insert_trig
  INSTEAD OF INSERT ON
  species_view FOR EACH ROW
  EXECUTE PROCEDURE insert_species_from_trig();

CREATE TRIGGER species_update_trig
  INSTEAD OF UPDATE ON
  species_view FOR EACH ROW
  EXECUTE PROCEDURE update_species_from_trig();
