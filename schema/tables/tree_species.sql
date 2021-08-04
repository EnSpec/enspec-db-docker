-- TABLE
DROP TABLE IF EXISTS tree_species CASCADE;
CREATE TABLE tree_species (
  tree_species_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  tree_data_id UUID REFERENCES tree_data NOT NULL,
  species_id UUID REFERENCES species NOT NULL
);
CREATE INDEX tree_species_source_id_idx ON tree_species(source_id);
CREATE INDEX tree_species_tree_data_id_idx ON tree_species(tree_data_id);
CREATE INDEX tree_species_species_id_idx ON tree_species(species_id);

-- VIEW
CREATE OR REPLACE VIEW tree_species_view AS
  SELECT
    t.tree_species_id AS tree_species_id,
    td.canopy_level  as canopy_level,
    ST_AsKML(td.crown_poly)  as crown_poly_kml,
    ST_AsKML(td.tree_location)  as tree_location_kml,
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
    tree_species t
LEFT JOIN source sc ON t.source_id = sc.source_id
LEFT JOIN tree_data td ON t.tree_data_id = td.tree_data_id
LEFT JOIN species s ON t.species_id = s.species_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_tree_species (
  tree_species_id UUID,
  canopy_level TREE_CANOPY_LEVEL,
  crown_poly_kml TEXT,
  tree_location_kml TEXT,
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
  tdid UUID;
  sid UUID;
BEGIN
  SELECT get_tree_data_id(canopy_level, crown_poly_kml, tree_location_kml) INTO tdid;
  SELECT get_species_id(common_name, family, genus, species, authority, synonym_symbol, subspecies, symbol) INTO sid;
  IF( tree_species_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO tree_species_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO tree_species (
    tree_species_id, tree_data_id, species_id, source_id
  ) VALUES (
    tree_species_id, tdid, sid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_tree_species (
  tree_species_id_in UUID,
  canopy_level_in TREE_CANOPY_LEVEL,
  crown_poly_kml_in TEXT,
  tree_location_kml_in TEXT,
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
tdid UUID;
sid UUID;
BEGIN
  SELECT get_tree_data_id(canopy_level_in, crown_poly_kml_in, tree_location_kml_in) INTO tdid;
  SELECT get_species_id(common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in) INTO sid;

  UPDATE tree_species SET (
    tree_data_id, species_id
  ) = (
    tdid, sid
  ) WHERE
    tree_species_id = tree_species_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_tree_species_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_tree_species(
    tree_species_id := NEW.tree_species_id,
    canopy_level := NEW.canopy_level,
    crown_poly_kml := NEW.crown_poly_kml,
    tree_location_kml := NEW.tree_location_kml,
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

CREATE OR REPLACE FUNCTION update_tree_species_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_tree_species(
    tree_species_id_in := NEW.tree_species_id,
    canopy_level_in := NEW.canopy_level,
    crown_poly_kml_in := NEW.crown_poly_kml,
    tree_location_kml_in := NEW.tree_location_kml,
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
CREATE OR REPLACE FUNCTION get_tree_species_id(
  canopy_level_in TREE_CANOPY_LEVEL,
  crown_poly_kml_in TEXT,
  tree_location_kml_in TEXT,
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
  tid UUID;
  tdid UUID;
  sid UUID;
BEGIN
  SELECT get_tree_data_id(canopy_level_in, crown_poly_kml_in, tree_location_kml_in) INTO tdid;
  SELECT get_species_id(common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in) INTO sid;

  SELECT
    tree_species_id INTO tid
  FROM
    tree_species t
  WHERE
    tree_data_id = tdid AND
    species_id = sid;

  IF (tid IS NULL) THEN
    RAISE EXCEPTION 'Unknown tree_species: canopy_level="%" crown_poly_kml="%" tree_location_kml="%" common_name="%" family="%"
    genus="%" species="%" authority="%" synonym_symbol="%" subspecies="%" symbol="%"', canopy_level_in, crown_poly_kml_in, tree_location_kml_in,
    common_name_in, family_in, genus_in, species_in, authority_in, synonym_symbol_in, subspecies_in, symbol_in;
  END IF;

  RETURN tid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER tree_species_insert_trig
  INSTEAD OF INSERT ON
  tree_species_view FOR EACH ROW
  EXECUTE PROCEDURE insert_tree_species_from_trig();

CREATE TRIGGER tree_species_update_trig
  INSTEAD OF UPDATE ON
  tree_species_view FOR EACH ROW
  EXECUTE PROCEDURE update_tree_species_from_trig();
