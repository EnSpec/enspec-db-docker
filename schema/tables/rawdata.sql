-- TABLE
DROP TABLE IF EXISTS rawdata CASCADE;
CREATE TABLE rawdata (
  rawdata_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  quality RAWDATA_QUALITY NOT NULL,
  cold_storage TEXT NOT NULL,
  hot_storage TEXT NOT NULL,
  hot_storage_expiration DATE NOT NULL
);
CREATE INDEX rawdata_source_id_idx ON rawdata(source_id);

-- VIEW
CREATE OR REPLACE VIEW rawdata_view AS
  SELECT
    r.rawdata_id AS rawdata_id,
    r.quality  as quality,
    r.cold_storage as cold_storage,
    r.hot_storage as hot_storage,
    r.hot_storage_expiration as hot_storage_expiration,
    sc.name AS source_name
  FROM
    rawdata r
LEFT JOIN source sc ON r.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_rawdata (
  rawdata_id UUID,
  quality RAWDATA_QUALITY,
  cold_storage TEXT,
  hot_storage TEXT,
  hot_storage_expiration DATE,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( rawdata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO rawdata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO rawdata (
    rawdata_id, quality, cold_storage, hot_storage, hot_storage_expiration, source_id
  ) VALUES (
    rawdata_id, quality, cold_storage, hot_storage, hot_storage_expiration, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_rawdata (
  rawdata_id_in UUID,
  quality_in RAWDATA_QUALITY,
  cold_storage_in TEXT,
  hot_storage_in TEXT,
  hot_storage_expiration_in DATE) RETURNS void AS $$
BEGIN

  UPDATE rawdata SET (
    quality, cold_storage, hot_storage, hot_storage_expiration
  ) = (
    quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in
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
    quality := NEW.quality,
    cold_storage := NEW.cold_storage,
    hot_storage := NEW.hot_storage,
    hot_storage_expiration := NEW.hot_storage_expiration,
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
    quality_in := NEW.quality,
    cold_storage_in := NEW.cold_storage,
    hot_storage_in := NEW.hot_storage,
    hot_storage_expiration_in := NEW.hot_storage_expiration
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_rawdata_id(quality_in RAWDATA_QUALITY, cold_storage_in TEXT, hot_storage_in TEXT, hot_storage_expiration_in DATE) RETURNS UUID AS $$
DECLARE
  rid UUID;
BEGIN
  SELECT
    rawdata_id INTO rid
  FROM
    rawdata r
  WHERE
      quality = quality_in AND
      cold_storage = cold_storage_in AND
      hot_storage = hot_storage_in;

  IF (rid IS NULL) THEN
    RAISE EXCEPTION 'Unknown rawdata: quality="%" cold_storage="%" hot_storage="%" hot_storage_expiration="%"', quality_in, cold_storage_in, hot_storage_in, hot_storage_expiration_in;
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
