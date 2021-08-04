-- TABLE
DROP TABLE IF EXISTS rawdata CASCADE;
CREATE TABLE rawdata (
  rawdata_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  line_id FLOAT NOT NULL,
  line_no FLOAT NOT NULL,
  quality RAWDATA_QUALITY
);
CREATE INDEX rawdata_source_id_idx ON rawdata(source_id);

-- VIEW
CREATE OR REPLACE VIEW rawdata_view AS
  SELECT
    r.rawdata_id AS rawdata_id,
    r.line_id  as line_id,
    r.line_no  as line_no,
    r.quality  as quality,
    sc.name AS source_name
  FROM
    rawdata r
LEFT JOIN source sc ON r.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rawdata (
  rawdata_id UUID,
  line_id FLOAT,
  line_no FLOAT,
  quality RAWDATA_QUALITY,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( rawdata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata (
    rawdata_id, line_id, line_no, quality, source_id
  ) VALUES (
    rawdata_id, line_id, line_no, quality, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rawdata (
  rawdata_id_in UUID,
  line_id_in FLOAT,
  line_no_in FLOAT,
  quality_in RAWDATA_QUALITY) RETURNS void AS $$
BEGIN

  UPDATE rawdata SET (
    line_id, line_no, quality
  ) = (
    line_id_in, line_no_in, quality_in
  ) WHERE
    rawdata_id = rawdata_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_rawdata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_rawdata(
    rawdata_id := NEW.rawdata_id,
    line_id := NEW.line_id,
    line_no := NEW.line_no,
    quality := NEW.quality,
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rawdata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_rawdata(
    rawdata_id_in := NEW.rawdata_id,
    line_id_in := NEW.line_id,
    line_no_in := NEW.line_no,
    quality_in := NEW.quality
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_rawdata_id(line_id_in float, line_no_in float, quality_in RAWDATA_QUALITY) RETURNS UUID AS $$
DECLARE
  rid UUID;
BEGIN
  IF quality_in IS NULL THEN
    SELECT
      rawdata_id INTO rid
    FROM
      rawdata r
    WHERE
        line_id = line_id_in AND
        line_no = line_no_in AND
        quality IS NULL;
  ELSE
    SELECT
      rawdata_id INTO rid
    FROM
      rawdata r
    WHERE
        line_id = line_id_in AND
        line_no = line_no_in AND
        quality = quality_in;
  END IF;
  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata: line_id="%" line_no="%" quality="%"', line_id_in, line_no_in, quality_in;
  END IF;

  RETURN rid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER rawdata_insert_trig
  INSTEAD OF INSERT ON
  rawdata_view FOR EACH ROW
  EXECUTE PROCEDURE insert_rawdata_from_trig();

CREATE TRIGGER rawdata_update_trig
  INSTEAD OF UPDATE ON
  rawdata_view FOR EACH ROW
  EXECUTE PROCEDURE update_rawdata_from_trig();
