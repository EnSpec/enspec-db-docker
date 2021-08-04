-- TABLE
DROP TABLE IF EXISTS sessions CASCADE;
CREATE TABLE sessions (
  sessions_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  session_name TEXT NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  line_count FLOAT NOT NULL,
  bad_lines TEXT
);
CREATE INDEX sessions_source_id_idx ON sessions(source_id);

-- VIEW
CREATE OR REPLACE VIEW sessions_view AS
  SELECT
    s.sessions_id AS sessions_id,
    s.session_name  as session_name,
    s.start_time  as start_time,
    s.end_time  as end_time,
    s.line_count  as line_count,
    s.bad_lines  as bad_lines,
    sc.name AS source_name
  FROM
    sessions s
LEFT JOIN source sc ON s.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_sessions (
  sessions_id UUID,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  bad_lines TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( sessions_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO sessions (
    sessions_id, session_name, start_time, end_time, line_count, bad_lines, source_id
  ) VALUES (
    sessions_id, session_name, start_time, end_time, line_count, bad_lines, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions (
  sessions_id_in UUID,
  session_name_in TEXT,
  start_time_in TIME,
  end_time_in TIME,
  line_count_in FLOAT,
  bad_lines_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE sessions SET (
    session_name, start_time, end_time, line_count, bad_lines
  ) = (
    session_name_in, start_time_in, end_time_in, line_count_in, bad_lines_in
  ) WHERE
    sessions_id = sessions_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_sessions_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_sessions(
    sessions_id := NEW.sessions_id,
    session_name := NEW.session_name,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    line_count := NEW.line_count,
    bad_lines := NEW.bad_lines,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_sessions(
    sessions_id_in := NEW.sessions_id,
    session_name_in := NEW.session_name,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    line_count_in := NEW.line_count,
    bad_lines_in := NEW.bad_lines
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_sessions_id(session_name_in text, start_time_in time, end_time_in time, line_count_in float, bad_lines_in text) RETURNS UUID AS $$
DECLARE
  sid UUID;
BEGIN

  IF bad_lines_in is NULL THEN
    SELECT
      sessions_id INTO sid
    FROM
      sessions s
    WHERE
      session_name = session_name_in AND
      start_time = start_time_in AND
      end_time = end_time_in AND
      line_count = line_count_in AND
      bad_lines is NULL;
  ELSE
    SELECT
      sessions_id INTO sid
    FROM
      sessions s
    WHERE
      session_name = session_name_in AND
      start_time = start_time_in AND
      end_time = end_time_in AND
      line_count = line_count_in AND
      bad_lines = bad_lines_in;
  END IF;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions: session_name="%" start_time="%" end_time="%" line_count="%" bad_lines="%"', session_name_in, start_time_in, end_time_in, line_count_in, bad_lines_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER sessions_insert_trig
  INSTEAD OF INSERT ON
  sessions_view FOR EACH ROW
  EXECUTE PROCEDURE insert_sessions_from_trig();

CREATE TRIGGER sessions_update_trig
  INSTEAD OF UPDATE ON
  sessions_view FOR EACH ROW
  EXECUTE PROCEDURE update_sessions_from_trig();