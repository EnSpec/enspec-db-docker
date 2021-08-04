-- TABLE
DROP TABLE IF EXISTS species_code CASCADE;
CREATE TABLE species_code (
  species_code_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  cn_number_fia TEXT NOT NULL,
  code_type SPECIES_CODE_TYPE NOT NULL,
  custom_code TEXT NOT NULL
);
CREATE INDEX species_code_source_id_idx ON species_code(source_id);

-- VIEW
CREATE OR REPLACE VIEW species_code_view AS
  SELECT
    s.species_code_id AS species_code_id,
    s.cn_number_fia  as cn_number_fia,
    s.code_type  as code_type,
    s.custom_code  as custom_code,

    sc.name AS source_name
  FROM
    species_code s
LEFT JOIN source sc ON s.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_species_code (
  species_code_id UUID,
  cn_number_fia TEXT,
  code_type SPECIES_CODE_TYPE,
  custom_code TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( species_code_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO species_code_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO species_code (
    species_code_id, cn_number_fia, code_type, custom_code, source_id
  ) VALUES (
    species_code_id, cn_number_fia, code_type, custom_code, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_species_code (
  species_code_id_in UUID,
  cn_number_fia_in TEXT,
  code_type_in SPECIES_CODE_TYPE,
  custom_code_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE species_code SET (
    cn_number_fia, code_type, custom_code
  ) = (
    cn_number_fia_in, code_type_in, custom_code_in
  ) WHERE
    species_code_id = species_code_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_species_code_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_species_code(
    species_code_id := NEW.species_code_id,
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

CREATE OR REPLACE FUNCTION update_species_code_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_species_code(
    species_code_id_in := NEW.species_code_id,
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
CREATE OR REPLACE FUNCTION get_species_code_id(cn_number_fia_in text, code_type_in SPECIES_CODE_TYPE, custom_code_in text) RETURNS UUID AS $$
DECLARE
  sid UUID;
BEGIN

  SELECT
    species_code_id INTO sid
  FROM
    species_code s
  WHERE
    cn_number_fia = cn_number_fia_in AND
    code_type = code_type_in AND
    custom_code = custom_code_in;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown species_code: cn_number_fia="%" code_type="%" custom_code="%"', cn_number_fia_in, code_type_in, custom_code_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER species_code_insert_trig
  INSTEAD OF INSERT ON
  species_code_view FOR EACH ROW
  EXECUTE PROCEDURE insert_species_code_from_trig();

CREATE TRIGGER species_code_update_trig
  INSTEAD OF UPDATE ON
  species_code_view FOR EACH ROW
  EXECUTE PROCEDURE update_species_code_from_trig();
