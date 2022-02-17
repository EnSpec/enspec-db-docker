-- TABLE
DROP TABLE IF EXISTS flightlines CASCADE;
CREATE TABLE flightlines (
  flightlines_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  sessions_id UUID REFERENCES sessions NOT NULL,
  start_time TIME,
  end_time TIME,
  media_files TEXT,
  line_notes TEXT,
  line_number FLOAT NOT NULL,
  line_index FLOAT
);
CREATE INDEX flightlines_source_id_idx ON flightlines(source_id);

ALTER TABLE flightlines ADD CONSTRAINT uniq_flightlines_row UNIQUE (sessions_id, line_number);

-- VIEW
CREATE OR REPLACE VIEW flightlines_view AS
  SELECT
    f.flightlines_id AS flightlines_id,
    fl.flight_date AS flight_date,
    fl.pilot AS pilot,
    fl.operator AS operator,
    fl.liftoff_time AS liftoff_time,
    s.session_name AS session_name,
    s.line_count AS line_count,
    f.start_time  as start_time,
    f.end_time  as end_time,
    f.media_files  as media_files,
    f.line_notes  as line_notes,
    f.line_number as line_number,
    f.line_index as line_index,

    sc.name AS source_name
  FROM
    flightlines f
LEFT JOIN sessions s ON f.sessions_id = s.sessions_id
LEFT JOIN flights fl ON s.flights_id = fl.flights_id
LEFT JOIN source sc ON f.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_flightlines (
  flightlines_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  liftoff_time TIME,
  session_name TEXT,
  line_count FLOAT,
  start_time TIME,
  end_time TIME,
  media_files TEXT,
  line_notes TEXT,
  line_number FLOAT,
  line_index FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  fl_id UUID;
  sid UUID;
BEGIN

  IF( flightlines_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO flightlines_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_flights_id(flight_date, pilot, operator, liftoff_time) INTO fl_id;
  SELECT get_epi_sessions_id(fl_id, session_name, line_count) INTO sid;

  INSERT INTO flightlines (
    flightlines_id, sessions_id, start_time, end_time, media_files, line_notes, line_number, line_index, source_id
  ) VALUES (
    flightlines_id, sid, start_time, end_time, media_files, line_notes, line_number, line_index, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION insert_epi_flightlines (
  flightlines_id UUID,
  sessions_id UUID,
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
    flightlines_id, sessions_id, start_time, end_time, media_files, line_notes, line_number, line_index, source_id
  ) VALUES (
    flightlines_id, sessions_id, start_time, end_time, media_files, line_notes, line_number, line_index, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_flightlines (
  flightlines_id_in UUID,
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  liftoff_time_in TIME,
  session_name TEXT,
  line_count FLOAT,
  start_time_in TIME,
  end_time_in TIME,
  media_files_in TEXT,
  line_notes_in TEXT,
  line_number FLOAT,
  line_index FLOAT) RETURNS void AS $$
DECLARE
fl_id UUID;
sid UUID;

BEGIN

  SELECT get_flights_id(flight_date_in, pilot_in, operator_in, liftoff_time_in) INTO fl_id;
  SELECT get_epi_sessions_id(fl_id, session_name, line_count) INTO sid;

  UPDATE flightlines SET (
    sessions_id, start_time, end_time, media_files, line_notes, line_number, line_index
  ) = (
    sid, start_time_in, end_time_in, media_files_in, line_notes_in, line_number_in, line_index_in
  ) WHERE
    flightlines_id = flightlines_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_epi_flightlines (
  flightlines_id_in UUID,
  sessions_id_in UUID,
  start_time_in TIME,
  end_time_in TIME,
  media_files_in TEXT,
  line_notes_in TEXT,
  line_number FLOAT,
  line_index FLOAT) RETURNS void AS $$

BEGIN

  UPDATE flightlines SET (
    sessions_id, start_time, end_time, media_files, line_notes, line_number, line_index
  ) = (
    sessions_id_in, start_time_in, end_time_in, media_files_in, line_notes_in, line_number_in, line_index_in
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
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    liftoff_time := NEW.liftoff_time,
    session_name := NEW.session_name,
    line_count := NEW.line_count,
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

CREATE OR REPLACE FUNCTION insert_epi_flightlines_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_flightlines(
    flightlines_id := NEW.flightlines_id,
    sessions_id := NEW.sessions_id,
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
    flight_date_in := NEW.flight_date,
    pilot_in := NEW.pilot,
    operator_in := NEW.operator,
    liftoff_time_in := NEW.liftoff_time,
    session_name_in := NEW.session_name,
    line_count_in := NEW.line_count,
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

CREATE OR REPLACE FUNCTION update_epi_flightlines_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_epi_flightlines(
    flightlines_id_in := NEW.flightlines_id,
    sessions_id_in := NEW.sessions_id,
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
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  liftoff_time_in TIME,
  session_name_in TEXT,
  line_count_in FLOAT,
  line_number_in FLOAT) RETURNS UUID AS $$
DECLARE
  fid UUID;
  fl_id UUID;
  sid UUID;
BEGIN
  SELECT get_flightlines_id(flight_date_in, pilot_in, operator_in, liftoff_time_in) INTO fl_id;
  SELECT get_sessions_id(fl_id, session_name_in) INTO sid;
  SELECT
    flightlines_id INTO fid
  FROM
    flightlines f
  WHERE
    sessions_id = sid AND
    line_number = line_number_in;

  IF (fid IS NULL) THEN
    RAISE EXCEPTION 'Unknown flightlines: flight_date="%" pilot="%" operator="%" liftoff_time="%" session_name="%" line_count="%" line_number="%"', flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, line_count_in, line_number_in;
  END IF;

  RETURN fid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_epi_flightlines_id(
  sessions_id_in UUID,
  start_time_in TIME,
  end_time_in TIME,
  media_files_in TEXT,
  line_notes_in TEXT,
  line_number_in FLOAT,
  line_index_in FLOAT) RETURNS UUID AS $$
DECLARE
  fid UUID;
  fl_id UUID;
  sid UUID;
BEGIN

  SELECT
    flightlines_id INTO fid
  FROM
    flightlines f
  WHERE
    sessions_id = sessions_id_in AND
    line_number = line_number_in;

  IF (fid IS NULL) THEN
    RAISE EXCEPTION 'Unknown flightlines: flight_date="%" pilot="%" operator="%" liftoff_time="%" session_name="%" line_count="%" line_number="%"', flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, line_count_in, line_number_in;
  END IF;

  RETURN fid;
END ;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_lightweight_flightlines_id(
  sessions_id_in UUID,
  line_number_in FLOAT) RETURNS UUID AS $$
DECLARE
  fid UUID;
BEGIN

  SELECT
    flightlines_id INTO fid
  FROM
    flightlines f
  WHERE
    sessions_id = sessions_id AND
    line_number = line_number_in;

  IF (fid IS NULL) THEN
    RAISE EXCEPTION 'Unknown flightlines: sessions_id="%" line_number="%"', sessions_id_in, line_number_in;
  END IF;

  RETURN fid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER flightlines_insert_trig
  INSTEAD OF INSERT ON
  flightlines_view FOR EACH ROW
  EXECUTE PROCEDURE insert_flightlines_from_trig();

CREATE TRIGGER flightlines_epi_insert_trig
  INSTEAD OF INSERT ON
  flightlines_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_flightlines_from_trig();

CREATE TRIGGER flightlines_update_trig
  INSTEAD OF UPDATE ON
  flightlines_view FOR EACH ROW
  EXECUTE PROCEDURE update_flightlines_from_trig();

CREATE TRIGGER flightlines_epi_update_trig
  INSTEAD OF UPDATE ON
  flightlines_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_flightlines_from_trig();
