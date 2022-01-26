-- TABLE
DROP TABLE IF EXISTS flightlines CASCADE;
CREATE TABLE flightlines (
  flightlines_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  session_id UUID REFERENCES source NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  media_files TEXT NOT NULL,
  line_notes TEXT,
  line_number FLOAT NOT NULL,
  line_index FLOAT NOT NULL,
  UNIQUE (start_time, end_time, media_files, line_number, line_index)
);
CREATE INDEX flightlines_source_id_idx ON flightlines(source_id);

-- VIEW
CREATE OR REPLACE VIEW flightlines_view AS
  SELECT
    fl.flightlines_id AS flightlines_id,
    f.date AS flight_date,
    s.name AS session_name,
    fl.start_time  as start_time,
    fl.end_time  as end_time,
    fl.media_files  as media_files,
    fl.line_notes  as line_notes,
    fl.line_number as line_number,
    fl.line_index as line_index,

    sc.name AS source_name
  FROM
    flightlines fl
LEFT JOIN sessions s ON fl.session_id = s.session_id
LEFT JOIN flight f ON s.session_id = f.flight_id
LEFT JOIN source sc ON fl.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_flightlines (
  flightlines_id UUID,
  flight_date DATE,
  sessions_name TEXT,
  start_time TIME,
  end_time TIME,
  media_files TEXT,
  line_notes TEXT,
  line_number FLOAT,
  line_index FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  session_id UUID;
BEGIN

  IF( flightlines_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO flightlines_id;
  END IF;
  select get_session_id(flight_name, session_name) into session_id;

  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO flightlines (
    flightlines_id, start_time, end_time, media_files, line_notes, line_number, line_index, source_id
  ) VALUES (
    flightlines_id, start_time, end_time, media_files, line_notes, line_number, line_index, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;



-- FUNCTIONS
CREATE OR REPLACE FUNCTION epi_insert_flightlines (
  flightlines_id UUID,
  session_id TEXT,
  start_time TIME,
  end_time TIME,
  media_files TEXT,
  line_notes TEXT,
  line_number FLOAT,
  line_index FLOAT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( flightlines_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO flightlines_id;
  END IF;

  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO flightlines (
    flightlines_id, session_id, start_time, end_time, media_files, line_notes, line_number, line_index, source_id
  ) VALUES (
    flightlines_id, session_id, start_time, end_time, media_files, line_notes, line_number, line_index, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_flightlines (
  flightlines_id_in UUID,
  start_time_in TIME,
  end_time_in TIME,
  media_files_in TEXT,
  line_notes_in TEXT,
  line_number FLOAT,
  line_index FLOAT) RETURNS void AS $$

BEGIN

  UPDATE flightlines SET (
    start_time, end_time, media_files, line_notes, line_number, line_index
  ) = (
    start_time_in, end_time_in, media_files_in, line_notes_in, line_number_in, line_index_in
  ) WHERE
    flightlines_id = flightlines_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_flightlines_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_flightlines(
    flightlines_id := NEW.flightlines_id,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    media_files := NEW.media_files,
    line_notes := NEW.line_notes,
    line_number := NEW.line_number,
    line_index := NEW.line_index,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION insert_api_flightlines_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_api_flightlines(
    flightlines_id := NEW.flightlines_id,
    session_id := NEW.session_id,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    media_files := NEW.media_files,
    line_notes := NEW.line_notes,
    line_number := NEW.line_number,
    line_index := NEW.line_index,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flightlines_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_flightlines(
    flightlines_id_in := NEW.flightlines_id,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    media_files_in := NEW.media_files,
    line_notes_in := NEW.line_notes,
    line_number_in := NEW.line_number,
    line_index_in := NEW.line_index
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_flightlines_id(
start_time_in TIME,
end_time_in TIME,
media_files_in TEXT,
line_notes_in TEXT,
line_number_in FLOAT,
line_index_in FLOAT) RETURNS UUID AS $$
DECLARE
  fid UUID;
BEGIN

  SELECT
    flightlines_id INTO fid
  FROM
    flightlines f
  WHERE
    start_time = start_time_in AND
    end_time = end_time_in AND
    media_files = media_files_in AND
    line_number = line_number_in AND
    line_index = line_index_in;

  IF (fid IS NULL) THEN
    RAISE EXCEPTION 'Unknown flightlines: start_time="%" end_time="%" media_files="%" line_notes="%" line_number="%" line_index="%"', start_time_in, end_time_in, media_files_in, line_notes_in, line_number_in, line_index_in;
  END IF;

  RETURN fid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER flightlines_insert_trig
  INSTEAD OF INSERT ON
  flightlines_view FOR EACH ROW
  EXECUTE PROCEDURE insert_flightlines_from_trig();

CREATE TRIGGER flightlines_insert_api_trig
  INSTEAD OF INSERT ON
  flightlines FOR EACH ROW
  EXECUTE PROCEDURE insert_flightlines_api_from_trig();

CREATE TRIGGER flightlines_update_trig
  INSTEAD OF UPDATE ON
  flightlines_view FOR EACH ROW
  EXECUTE PROCEDURE update_flightlines_from_trig();
