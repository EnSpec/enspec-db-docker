-- TABLE
DROP TABLE IF EXISTS sessions_flightlines CASCADE;
CREATE TABLE sessions_flightlines (
  sessions_flightlines_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  flightlines_id UUID REFERENCES flightlines NOT NULL,
  sessions_id UUID REFERENCES sessions NOT NULL
);
CREATE INDEX sessions_flightlines_source_id_idx ON sessions_flightlines(source_id);
CREATE INDEX sessions_flightlines_sessions_id_idx ON sessions_flightlines(sessions_id);
CREATE INDEX sessions_flightlines_flightlines_id_idx ON sessions_flightlines(flightlines_id);

-- VIEW
CREATE OR REPLACE VIEW sessions_flightlines_view AS
  SELECT
    s.sessions_flightlines_id AS sessions_flightlines_id,
    ses.session_name  as session_name,
    ses.start_time  as session_start_time,
    ses.end_time  as session_end_time,
    ses.line_count  as session_line_count,
    ses.session_notes  as session_notes,
    fl.start_time  as fl_start_time,
    fl.end_time  as fl_end_time,
    fl.media_files  as fl_media_files,
    fl.line_notes  as line_notes,
    fl.line_number as line_number,
    fl.line_index as line_index,

    sc.name AS source_name
  FROM
    sessions_flightlines s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN sessions ses ON s.sessions_id = ses.sessions_id
LEFT JOIN flightlines fl ON s.flightlines_id = fl.flightlines_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_sessions_flightlines (
  sessions_flightlines_id UUID,
  session_name TEXT,
  session_start_time TIME,
  session_end_time TIME,
  session_line_count FLOAT,
  session_notes TEXT,
  fl_start_time TIME,
  fl_end_time TIME,
  fl_media_files TEXT,
  line_notes TEXT,
  line_number FLOAT,
  line_index FLOAT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  ses_id UUID;
  fl_id UUID;
BEGIN

  IF( sessions_flightlines_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_flightlines_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_sessions_id(session_name, session_start_time, session_end_time, session_line_count, session_notes) INTO ses_id;
  SELECT get_flightlines_id(fl_start_time, fl_end_time, fl_media_files, line_notes, line_number, line_index) INTO fl_id;

  INSERT INTO sessions_flightlines (
    sessions_flightlines_id, sessions_id, flightlines_id, source_id
  ) VALUES (
    sessions_flightlines_id, ses_id, fl_id, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions_flightlines (
  sessions_flightlines_id_in UUID,
  session_name_in TEXT,
  session_start_time_in TIME,
  session_end_time_in TIME,
  session_line_count_in FLOAT,
  session_notes_in TEXT,
  fl_start_time_in TIME,
  fl_end_time_in TIME,
  fl_media_files_in TEXT,
  line_notes_in TEXT,
  line_number_in FLOAT,
  line_index_in FLOAT) RETURNS void AS $$
DECLARE
ses_id UUID;
fl_id UUID;
BEGIN
SELECT get_sessions_id(session_name_in, session_start_time_in, session_end_time_in, session_line_count_in, session_notes_in) INTO ses_id;
SELECT get_flightlines_id(fl_start_time_in, fl_end_time_in, fl_media_files_in, line_notes_in, line_number_in, line_index_in) INTO fl_id;

  UPDATE sessions_flightlines SET (
    sessions_id, flightlines_id
  ) = (
    ses_id, fl_id
  ) WHERE
    sessions_flightlines_id = sessions_flightlines_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_sessions_flightlines_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_sessions_flightlines(
    sessions_flightlines_id := NEW.sessions_flightlines_id,
    session_name := NEW.session_name,
    session_start_time := NEW.session_start_time,
    session_end_time := NEW.session_end_time,
    session_line_count := NEW.session_line_count,
    session_notes := NEW.session_notes,
    fl_start_time := NEW.fl_start_time,
    fl_end_time := NEW.fl_end_time,
    fl_media_files := NEW.fl_media_files,
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

CREATE OR REPLACE FUNCTION update_sessions_flightlines_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_sessions_flightlines(
    sessions_flightlines_id_in := NEW.sessions_flightlines_id,
    session_name_in := NEW.session_name,
    session_start_time_in := NEW.session_start_time,
    session_end_time_in := NEW.session_end_time,
    session_line_count_in := NEW.session_line_count,
    session_notes_in := NEW.session_notes,
    fl_start_time_in := NEW.fl_start_time,
    fl_end_time_in := NEW.fl_end_time,
    fl_media_files_in := NEW.fl_media_files,
    line_notes_in := NEW.line_notes,
    line_number_in := NEW.line_number,
    fl_line_index_in := NEW.line_index
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_sessions_flightlines_id(
  session_name TEXT,
  session_start_time TIME,
  session_end_time TIME,
  session_line_count FLOAT,
  session_notes TEXT,
  fl_start_time TIME,
  fl_end_time TIME,
  fl_media_files TEXT,
  line_notes TEXT,
  line_number FLOAT,
  line_index FLOAT) RETURNS UUID AS $$
DECLARE
  sid UUID;
  ses_id UUID;
  fl_id UUID;
BEGIN
SELECT get_sessions_id(session_name, session_start_time, session_end_time, session_line_count, session_notes) INTO ses_id;
SELECT get_flightlines_id(fl_start_time, fl_end_time, fl_media_files, line_notes, line_number, line_index) INTO fl_id;

  SELECT
    sessions_flightlines_id INTO sid
  FROM
    sessions_flightlines s
  WHERE
    sessions_id = ses_id AND
    flightlines_id = fl_id;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions_flightlines: session_name="%" session_start_time="%" session_end_time="%" session_line_count="%" session_notes="%" fl_start_time="%" fl_end_time="%" fl_media_files="%" line_notes="%" line_number="%" line_index="%"', session_name, session_start_time, session_end_time, session_line_count, session_notes, fl_start_time, fl_end_time, fl_media_files, line_notes, line_number, line_index;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER sessions_flightlines_insert_trig
  INSTEAD OF INSERT ON
  sessions_flightlines_view FOR EACH ROW
  EXECUTE PROCEDURE insert_sessions_flightlines_from_trig();

CREATE TRIGGER sessions_flightlines_update_trig
  INSTEAD OF UPDATE ON
  sessions_flightlines_view FOR EACH ROW
  EXECUTE PROCEDURE update_sessions_flightlines_from_trig();
