-- TABLE
DROP TABLE IF EXISTS image_output CASCADE;
CREATE TABLE image_output (
  image_output_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  image_dir TEXT NOT NULL,
  image_dir_owner TEXT NOT NULL,
  image_exists BOOL NOT NULL,
  processing_date DATE NOT NULL,
  expiration_date DATE NOT NULL,
  expiration_type IMAGE_OUTPUT_EXPIRATION_TYPE NOT NULL
);
CREATE INDEX image_output_source_id_idx ON image_output(source_id);

-- VIEW
CREATE OR REPLACE VIEW image_output_view AS
  SELECT
    i.image_output_id AS image_output_id,
    i.image_dir  as image_dir,
    i.image_dir_owner  as image_dir_owner,
    i.image_exists  as image_exists,
    i.processing_date  as processing_date,
    i.expiration_date  as expiration_date,
    i.expiration_type  as expiration_type,

    sc.name AS source_name
  FROM
    image_output i
LEFT JOIN source sc ON i.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_image_output (
  image_output_id UUID,
  image_dir TEXT,
  image_dir_owner TEXT,
  image_exists BOOL,
  processing_date DATE,
  expiration_date DATE,
  expiration_type IMAGE_OUTPUT_EXPIRATION_TYPE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( image_output_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO image_output_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO image_output (
    image_output_id, image_dir, image_dir_owner, image_exists, processing_date, expiration_date, expiration_type, source_id
  ) VALUES (
    image_output_id, image_dir, image_dir_owner, image_exists, processing_date, expiration_date, expiration_type, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_image_output (
  image_output_id_in UUID,
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE) RETURNS void AS $$
BEGIN

  UPDATE image_output SET (
    image_dir, image_dir_owner, image_exists, processing_date, expiration_date, expiration_type
  ) = (
    image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in
  ) WHERE
    image_output_id = image_output_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_image_output_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_image_output(
    image_output_id := NEW.image_output_id,
    image_dir := NEW.image_dir,
    image_dir_owner := NEW.image_dir_owner,
    image_exists := NEW.image_exists,
    processing_date := NEW.processing_date,
    expiration_date := NEW.expiration_date,
    expiration_type := NEW.expiration_type,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_image_output_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_image_output(
    image_output_id_in := NEW.image_output_id,
    image_dir_in := NEW.image_dir,
    image_dir_owner_in := NEW.image_dir_owner,
    image_exists_in := NEW.image_exists,
    processing_date_in := NEW.processing_date,
    expiration_date_in := NEW.expiration_date,
    expiration_type_in := NEW.expiration_type
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_image_output_id(
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE
) RETURNS UUID AS $$
DECLARE
  iid UUID;
BEGIN

  SELECT
    image_output_id INTO iid
  FROM
    image_output i
  WHERE
    image_dir = image_dir_in AND
    image_dir_owner = image_dir_owner_in AND
    image_exists = image_exists_in AND
    processing_date = processing_date_in AND
    expiration_date = expiration_date_in AND
    expiration_type = expiration_type_in;

  IF (iid IS NULL) THEN
    RAISE EXCEPTION 'Unknown image_output: image_dir="%" image_dir_owner="%" image_exists="%" processing_date="%" expiration_date="%" expiration_type="%"',
    image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in;
  END IF;

  RETURN iid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER image_output_insert_trig
  INSTEAD OF INSERT ON
  image_output_view FOR EACH ROW
  EXECUTE PROCEDURE insert_image_output_from_trig();

CREATE TRIGGER image_output_update_trig
  INSTEAD OF UPDATE ON
  image_output_view FOR EACH ROW
  EXECUTE PROCEDURE update_image_output_from_trig();
