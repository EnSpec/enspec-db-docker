-- TABLE
DROP TABLE IF EXISTS flightlines_rawdata CASCADE;
CREATE TABLE flightlines_rawdata (
  flightlines_rawdata_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  flightlines_id UUID REFERENCES flightlines NOT NULL,
  rawdata_id UUID REFERENCES rawdata NOT NULL,
  UNIQUE(flightlines_id, rawdata_id)
);
CREATE INDEX flightlines_rawdata_source_id_idx ON flightlines_rawdata(source_id);
CREATE INDEX flightlines_rawdata_flightlines_id_idx ON flightlines_rawdata(flightlines_id);
CREATE INDEX flightlines_rawdata_rawdata_id_idx ON flightlines_rawdata(rawdata_id);

-- VIEW
CREATE OR REPLACE VIEW flightlines_rawdata_view AS
  SELECT
    f.flightlines_rawdata_id AS flightlines_rawdata_id,
    fl.start_time  as fl_start_time,
    fl.end_time  as fl_end_time,
    fl.media_files  as fl_media_files,
    fl.line_notes  as fl_line_notes,
    fl.line_number as fl_line_number,
    fl.line_index as fl_line_index,
    rd.quality  as rd_quality,
    rd.cold_storage as rd_cold_storage,
    rd.hot_storage as rd_hot_storage,
    rd.hot_storage_expiration as rd_hot_storage_expiration,

    sc.name AS source_name
  FROM
    flightlines_rawdata f
LEFT JOIN source sc ON f.source_id = sc.source_id
LEFT JOIN flightlines fl on f.flightlines_id = fl.flightlines_id
LEFT JOIN rawdata rd on f.rawdata_id = rd.rawdata_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_flightlines_rawdata (
  flightlines_rawdata_id UUID,
  fl_start_time TIME,
  fl_end_time TIME,
  fl_media_files TEXT,
  fl_line_notes TEXT,
  line_number FLOAT,
  line_index FLOAT,
  rd_quality RAWDATA_QUALITY,
  rd_cold_storage TEXT,
  rd_hot_storage TEXT,
  rd_hot_storage_expiration DATE,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  fl_id UUID;
  rd_id UUID;
BEGIN

  IF( flightlines_rawdata_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO flightlines_rawdata_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_flightlines_id(fl_start_time, fl_end_time, fl_media_files, fl_line_notes, fl_line_number, fl_line_index) INTO fl_id;
  SELECT get_rawdata_id(rd_quality, cold_storage, hot_storage, hot_storage_expiration) INTO rd_id;

  INSERT INTO flightlines_rawdata (
    flightlines_rawdata_id, flightlines_id, rawdata_id, source_id
  ) VALUES (
    flightlines_rawdata_id, fl_id, rd_id, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flightlines_rawdata (
  flightlines_rawdata_id_in UUID,
  fl_start_time_in TIME,
  fl_end_time_in TIME,
  fl_media_files_in TEXT,
  fl_line_notes_in TEXT,
  fl_line_number_in FLOAT,
  fl_line_index_in FLOAT,
  rd_quality_in RAWDATA_QUALITY,
  rd_cold_storage_in TEXT,
  rd_hot_storage_in TEXT,
  rd_hot_storage_expiration_in DATE) RETURNS void AS $$
DECLARE
  fl_id UUID;
  rd_id UUID;
BEGIN

  SELECT get_flightlines_id(fl_start_time_in, fl_end_time_in, fl_media_files_in, fl_line_notes_in, fl_line_number_in, fl_line_index_in) INTO fl_id;
  SELECT get_rawdata_id(rd_quality_in, rd_cold_storage_in, rd_hot_storage_in, rd_hot_storage_expiration) INTO rd_id;

  UPDATE flightlines_rawdata SET (
    flightlines_id, rawdata_id
  ) = (
    fl_id, rd_in
  ) WHERE
    flightlines_rawdata_id = flightlines_rawdata_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_flightlines_rawdata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_flightlines_rawdata(
    flightlines_rawdata_id := NEW.flightlines_rawdata_id,
    fl_start_time := NEW.fl_start_time,
    fl_end_time := NEW.fl_end_time,
    fl_media_files := NEW.fl_media_files,
    fl_line_notes := NEW.fl_line_notes,
    fl_line_number := NEW.fl_line_number,
    fl_line_index := NEW.fl_line_index,
    rd_quality := NEW.rd_quality,
    rd_cold_storage := NEW.rd_cold_storage,
    rd_hot_storage := NEW.rd_hot_storage,
    rd_hot_storage_expiration := NEW.rd_hot_storage_expiration,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flightlines_rawdata_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_flightlines_rawdata(
    flightlines_rawdata_id_in := NEW.flightlines_rawdata_id,
    fl_start_time_in := NEW.fl_start_time,
    fl_end_time_in := NEW.fl_end_time,
    fl_media_files_in := NEW.fl_media_files,
    fl_line_notes_in := NEW.fl_line_notes,
    fl_line_number_in := NEW.fl_line_number,
    fl_line_index_in := NEW.fl_line_index,
    rd_quality_in := NEW.rd_quality,
    rd_cold_storage_in := NEW.rd_cold_storage,
    rd_hot_storage_in := NEW.rd_hot_storage,
    rd_hot_storage_expiration_in := NEW.hot_storage_expiration
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_flightlines_rawdata_id(
  fl_start_time TIME,
  fl_end_time TIME,
  fl_media_files TEXT,
  fl_line_notes TEXT,
  fl_line_number FLOAT,
  fl_line_index FLOAT,
  quality RAWDATA_QUALITY,
  cold_storage TEXT, 
  hot_storage TEXT,
  hot_storage_expiration DATE) RETURNS UUID AS $$
DECLARE
  fid UUID;
  fl_id UUID;
  rd_id UUID;
BEGIN

SELECT get_flightlines_id(fl_start_time, fl_end_time, fl_media_files, fl_line_notes, fl_line_number, fl_line_index) INTO fl_id;
SELECT get_rawdata_id(quality, cold_storage, hot_storage, hot_storage_expiration) INTO rd_id;

  SELECT
    flightlines_rawdata_id INTO fid
  FROM
    flightlines_rawdata f
  WHERE
    flightlines_id = fl_id AND
    rawdata_id = rd_id;

  IF (fid IS NULL) THEN
    RAISE EXCEPTION 'Unknown flightlines_rawdata: fl_start_time="%" fl_end_time="%"
    fl_media_files="%" fl_line_notes="%" fl_line_number="%" fl_line_index="%" rd_quality="%"
    rd_cold_storage="%" rd_hot_storage="%" rd_hot_storage_expiration="%"', fl_start_time, fl_end_time,
    fl_media_files, fl_line_notes, fl_line_number, fl_line_index, quality, cold_storage, hot_storage, hot_storage_expiration;
  END IF;

  RETURN fid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER flightlines_rawdata_insert_trig
  INSTEAD OF INSERT ON
  flightlines_rawdata_view FOR EACH ROW
  EXECUTE PROCEDURE insert_flightlines_rawdata_from_trig();

CREATE TRIGGER flightlines_rawdata_update_trig
  INSTEAD OF UPDATE ON
  flightlines_rawdata_view FOR EACH ROW
  EXECUTE PROCEDURE update_flightlines_rawdata_from_trig();
