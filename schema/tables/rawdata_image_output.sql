-- TABLE
DROP TABLE IF EXISTS rawdata_image_output CASCADE;
CREATE TABLE rawdata_image_output (
  rawdata_image_output_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  rawdata_id UUID REFERENCES rawdata NOT NULL,
  image_output_id UUID REFERENCES image_output NOT NULL
);
CREATE INDEX rawdata_image_output_source_id_idx ON rawdata_image_output(source_id);
CREATE INDEX rawdata_image_output_rawdata_id_idx ON rawdata_image_output(rawdata_id);
CREATE INDEX rawdata_image_output_image_output_id_idx ON rawdata_image_output(image_output_id);

-- VIEW
CREATE OR REPLACE VIEW rawdata_image_output_view AS
  SELECT
    r.rawdata_image_output_id AS rawdata_image_output_id,
    rd.line_id AS line_id,
    rd.line_no AS line_no,
    rd.quality AS quality,
    io.image_dir AS image_dir,
    io.image_dir_owner AS image_dir_owner,
    io.image_exists AS image_exists,
    io.processing_date AS processing_date,
    io.expiration_date AS expiration_date,
    io.expiration_date AS expiration_type,

    sc.name AS source_name
  FROM
    rawdata_image_output r
LEFT JOIN source sc ON r.source_id = sc.source_id
LEFT JOIN rawdata rd ON r.rawdata_id = rd.rawdata_id
LEFT JOIN image_output io ON r.image_output_id = io.image_output_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rawdata_image_output (
  rawdata_image_output_id UUID,
  line_id FLOAT,
  line_no FLOAT,
  quality RAWDATA_QUALITY,
  image_dir TEXT,
  image_dir_owner TEXT,
  image_exists BOOL,
  processing_date DATE,
  expiration_date DATE,
  expiration_type IMAGE_OUTPUT_EXPIRATION_TYPE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  rdid UUID;
  ioid UUID;
BEGIN
  SELECT get_rawdata_id(line_id, line_no, quality) INTO rdid;
  SELECT get_image_output_id(image_dir, image_dir_owner, image_exists, processing_date, expiration_date, expiration_type) INTO ioid;

  IF( rawdata_image_output_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_image_output_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata_image_output (
    rawdata_image_output_id, rawdata_id, image_output_id, source_id
  ) VALUES (
    rawdata_image_output_id, rdid, ioid, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rawdata_image_output (
  rawdata_image_output_id_in UUID,
  rawdata_id_in UUID,
  image_output_id_in UUID) RETURNS void AS $$
DECLARE
rdid UUID;
ioid UUID;

BEGIN
  SELECT get_rawdata_id(line_id, line_no, quality) INTO rdid;
  SELECT get_image_output_id(image_dir, image_dir_owner, image_exists, processing_date, expiration_date, expiration_type) INTO ioid;

  UPDATE rawdata_image_output SET (
    rawdata_id, image_output_id
  ) = (
    rdid, ioid
  ) WHERE
    rawdata_image_output_id = rawdata_image_output_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_rawdata_image_output_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_rawdata_image_output(
    rawdata_image_output_id := NEW.rawdata_image_output_id,
    rline_no := NEW.line_no,
    quality := NEW.quality,
    source_name := NEW.source_name,
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

CREATE OR REPLACE FUNCTION update_rawdata_image_output_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_rawdata_image_output(
    rawdata_image_output_id_in := NEW.rawdata_image_output_id,
    line_id_in := NEW.line_id,
    line_no_in := NEW.line_no,
    quality_in := NEW.quality,
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
CREATE OR REPLACE FUNCTION get_rawdata_image_output_id(
  line_id_in float,
  line_no_in float,
  quality_in RAWDATA_QUALITY,
  image_dir_in TEXT,
  image_dir_owner_in TEXT,
  image_exists_in BOOL,
  processing_date_in DATE,
  expiration_date_in DATE,
  expiration_type_in IMAGE_OUTPUT_EXPIRATION_TYPE
) RETURNS UUID AS $$
DECLARE
  rid UUID;
  rdid UUID;
  ioid UUID;
BEGIN
  SELECT get_rawdata_id(line_id_in, line_no_in, quality_in) INTO rdid;
  SELECT get_image_output_id(image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in) INTO ioid;

  SELECT
    rawdata_image_output_id INTO rid
  FROM
    rawdata_image_output r
  WHERE
    rawdata_id = rdid AND
    image_output_id = ioid;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata_image_output: line_id="%" line_no="%" quality="%" image_dir="%"
    image_dir_owner="%" image_exists="%" processing_date="%" expiration_date="%" expiration_type="%"',
    line_id_in, line_no_in, quality_in, image_dir_in, image_dir_owner_in, image_exists_in, processing_date_in, expiration_date_in, expiration_type_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER rawdata_image_output_insert_trig
  INSTEAD OF INSERT ON
  rawdata_image_output_view FOR EACH ROW
  EXECUTE PROCEDURE insert_rawdata_image_output_from_trig();

CREATE TRIGGER rawdata_image_output_update_trig
  INSTEAD OF UPDATE ON
  rawdata_image_output_view FOR EACH ROW
  EXECUTE PROCEDURE update_rawdata_image_output_from_trig();
