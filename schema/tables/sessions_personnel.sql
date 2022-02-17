-- TABLE
DROP TABLE IF EXISTS sessions_personnel CASCADE;
CREATE TABLE sessions_personnel (
  sessions_personnel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  sessions_id UUID REFERENCES sessions NOT NULL,
  personnel_id UUID REFERENCES personnel NOT NULL,
  activity_id UUID REFERENCES activity NOT NULL,
  activity_date_started DATE NOT NULL,
  activity_date_completed DATE,
  activity_lead TEXT NOT NULL
);
CREATE INDEX sessions_personnel_source_id_idx ON sessions_personnel(source_id);
CREATE INDEX sessions_personnel_sessions_id_idx ON sessions_personnel(sessions_id);
CREATE INDEX sessions_personnel_personnel_id_idx ON sessions_personnel(personnel_id);
CREATE INDEX sessions_personnel_activity_id_idx ON sessions_personnel(activity_id);

ALTER TABLE sessions_personnel ADD CONSTRAINT uniq_ses_personnel_row UNIQUE(sessions_id, personnel_id, activity_id, activity_date_started, activity_lead);

-- VIEW
CREATE OR REPLACE VIEW sessions_personnel_view AS
  SELECT
    s.sessions_personnel_id AS sessions_personnel_id,
    f.flight_date  as flight_date,
    f.pilot  as pilot,
    f.operator  as operator,
    f.liftoff_time  as liftoff_time,
    ses.session_name  as session_name,
    ses.start_time  as start_time,
    ses.end_time  as end_time,
    ses.line_count  as line_count,
    ses.session_notes as session_notes,
    per.personnel_name  as personnel_name,
    per.personnel_role  as personnel_role,
    per.organization  as organization,
    per.office_phone  as office_phone,
    per.cell_phone  as cell_phone,
    per.email  as email,
    per.personnel_notes  as personnel_notes,
    ac.activity  as activity,
    ac.activity_description  as activity_description,
    s.activity_date_started  as activity_date_started,
    s.activity_date_completed  as activity_date_completed,
    s.activity_lead  as activity_lead,

    sc.name AS source_name
  FROM
    sessions_personnel s
LEFT JOIN source sc ON s.source_id = sc.source_id
LEFT JOIN sessions ses ON s.sessions_id = ses.sessions_id
LEFT JOIN flights f ON ses.flights_id = f.flights_id
LEFT JOIN personnel per ON s.personnel_id = per.personnel_id
LEFT JOIN activity ac ON s.activity_id = ac.activity_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_sessions_personnel (
  sessions_personnel_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  liftoff_time TIME,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  session_notes TEXT,
  personnel_name TEXT,
  personnel_role TEXT,
  organization TEXT,
  office_phone TEXT,
  cell_phone TEXT,
  email TEXT,
  personnel_notes TEXT,
  activity TEXT,
  activity_description TEXT,
  activity_date_started DATE,
  activity_date_completed DATE,
  activity_lead TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  sesid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_sessions_id(flight_date, pilot, operator, liftoff_time, session_name, start_time, end_time, line_count, session_notes) INTO sesid;
  SELECT get_personnel_id(personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes) INTO perid;
  SELECT get_activity_id(activity, activity_description) INTO acid;

  IF( sessions_personnel_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_personnel_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO sessions_personnel (
    sessions_personnel_id, sessions_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead, source_id
  ) VALUES (
    sessions_personnel_id, sesid, perid, acid, activity_date_started, activity_date_completed, activity_lead, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_sessions_personnel (
  sessions_personnel_id UUID,
  flights_id UUID,
  session_name TEXT,
  start_time TIME,
  end_time TIME,
  line_count FLOAT,
  session_notes TEXT,
  personnel_name TEXT,
  personnel_role TEXT,
  organization TEXT,
  office_phone TEXT,
  cell_phone TEXT,
  email TEXT,
  personnel_notes TEXT,
  activity TEXT,
  activity_description TEXT,
  activity_date_started DATE,
  activity_date_completed DATE,
  activity_lead TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  sesid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_epi_sessions_id(flights_id, session_name, start_time, end_time, line_count, session_notes) INTO sesid;
  SELECT get_personnel_id(personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes) INTO perid;
  SELECT get_activity_id(activity, activity_description) INTO acid;

  IF( sessions_personnel_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO sessions_personnel_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO sessions_personnel (
    sessions_personnel_id, sessions_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead, source_id
  ) VALUES (
    sessions_personnel_id, sesid, perid, acid, activity_date_started, activity_date_completed, activity_lead, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions_personnel (
  sessions_personnel_id_in UUID,
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  liftoff_time_in TIME,
  session_name_in TEXT,
  start_time_in TIME,
  end_time_in TIME,
  line_count_in FLOAT,
  session_notes_in TEXT,
  personnel_name_in TEXT,
  personnel_role_in TEXT,
  organization_in TEXT,
  office_phone_in TEXT,
  cell_phone_in TEXT,
  email_in TEXT,
  personnel_notes_in TEXT,
  activity_in TEXT,
  activity_description_in TEXT,
  activity_date_started_in DATE,
  activity_date_completed_in DATE,
  activity_lead_in TEXT) RETURNS void AS $$
DECLARE
sesid UUID;
perid UUID;
acid UUID;
BEGIN
  SELECT get_sessions_id(flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in) INTO sesid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  UPDATE sessions_personnel SET (
    sessions_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead
  ) = (
    sesid, perid, acid, activity_date_started_in, activity_date_completed_in, activity_lead_in
  ) WHERE
    sessions_personnel_id = sessions_personnel_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_sessions_personnel (
  sessions_personnel_id_in UUID,
  flights_id_in UUID,
  session_name_in TEXT,
  start_time_in TIME,
  end_time_in TIME,
  line_count_in FLOAT,
  session_notes_in TEXT,
  personnel_name_in TEXT,
  personnel_role_in TEXT,
  organization_in TEXT,
  office_phone_in TEXT,
  cell_phone_in TEXT,
  email_in TEXT,
  personnel_notes_in TEXT,
  activity_in TEXT,
  activity_description_in TEXT,
  activity_date_started_in DATE,
  activity_date_completed_in DATE,
  activity_lead_in TEXT) RETURNS void AS $$
DECLARE
sesid UUID;
perid UUID;
acid UUID;
BEGIN
  SELECT get_epi_sessions_id(flights_id_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in) INTO sesid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  UPDATE sessions_personnel SET (
    sessions_id, personnel_id, activity_id, activity_date_started, activity_date_completed, activity_lead
  ) = (
    sesid, perid, acid, activity_date_started_in, activity_date_completed_in, activity_lead_in
  ) WHERE
    sessions_personnel_id = sessions_personnel_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_sessions_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_sessions_personnel(
    sessions_personnel_id := NEW.sessions_personnel_id,
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    liftoff_time := NEW.liftoff_time,
    session_name := NEW.session_name,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    line_count := NEW.line_count,
    session_notes := NEW.session_notes,
    personnel_name := NEW.personnel_name,
    personnel_role := NEW.personnel_role,
    organization := NEW.organization,
    office_phone := NEW.office_phone,
    cell_phone := NEW.cell_phone,
    email := NEW.email,
    personnel_notes := NEW.personnel_notes,
    activity := NEW.activity,
    activity_description := NEW.activity_description,
    activity_date_started := NEW.activity_date_started,
    activity_date_completed := NEW.activity_date_completed,
    activity_lead := NEW.activity_lead,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_epi_sessions_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_epi_sessions_personnel(
    sessions_personnel_id := NEW.sessions_personnel_id,
    flights_id := NEW.flights_id,
    session_name := NEW.session_name,
    start_time := NEW.start_time,
    end_time := NEW.end_time,
    line_count := NEW.line_count,
    session_notes := NEW.session_notes,
    personnel_name := NEW.personnel_name,
    personnel_role := NEW.personnel_role,
    organization := NEW.organization,
    office_phone := NEW.office_phone,
    cell_phone := NEW.cell_phone,
    email := NEW.email,
    personnel_notes := NEW.personnel_notes,
    activity := NEW.activity,
    activity_description := NEW.activity_description,
    activity_date_started := NEW.activity_date_started,
    activity_date_completed := NEW.activity_date_completed,
    activity_lead := NEW.activity_lead,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_sessions_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_sessions_personnel(
    sessions_personnel_id_in := NEW.sessions_personnel_id,
    flight_date_in := NEW.flight_date,
    pilot_in := NEW.pilot,
    operator_in := NEW.operator,
    liftoff_time_in := NEW.liftoff_time,
    session_name_in := NEW.session_name,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    line_count_in := NEW.line_count,
    session_notes_in := NEW.session_notes,
    personnel_name_in := NEW.personnel_name,
    personnel_role_in := NEW.personnel_role,
    organization_in := NEW.organization,
    office_phone_in := NEW.office_phone,
    cell_phone_in := NEW.cell_phone,
    email_in := NEW.email,
    personnel_notes_in := NEW.personnel_notes,
    activity_in := NEW.activity,
    activity_description_in := NEW.activity_description,
    activity_date_started_in := NEW.activity_date_started,
    activity_date_completed_in := NEW.activity_date_completed,
    activity_lead_in := NEW.activity_lead
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_epi_sessions_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_epi_sessions_personnel(
    sessions_personnel_id_in := NEW.sessions_personnel_id,
    flights_id_in := NEW.flights_id,
    session_name_in := NEW.session_name,
    start_time_in := NEW.start_time,
    end_time_in := NEW.end_time,
    line_count_in := NEW.line_count,
    session_notes_in := NEW.session_notes,
    personnel_name_in := NEW.personnel_name,
    personnel_role_in := NEW.personnel_role,
    organization_in := NEW.organization,
    office_phone_in := NEW.office_phone,
    cell_phone_in := NEW.cell_phone,
    email_in := NEW.email,
    personnel_notes_in := NEW.personnel_notes,
    activity_in := NEW.activity,
    activity_description_in := NEW.activity_description,
    activity_date_started_in := NEW.activity_date_started,
    activity_date_completed_in := NEW.activity_date_completed,
    activity_lead_in := NEW.activity_lead
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_sessions_personnel_id(
  flight_date_in date, pilot_in text, operator_in text, liftoff_time_in time,
  session_name_in text, start_time_in time, end_time_in time, line_count_in float, session_notes_in text,
  personnel_name_in TEXT, personnel_role_in TEXT, organization_in TEXT, office_phone_in TEXT,
  cell_phone_in TEXT, email_in TEXT, personnel_notes_in TEXT, activity_in TEXT, activity_description_in TEXT,
  activity_date_started_in DATE, activity_date_completed_in DATE, activity_lead_in TEXT
) RETURNS UUID AS $$
DECLARE
  sid UUID;
  sesid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_sessions_id(flight_date_in, pilot_in, operator_in, liftoff_time_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in) INTO sesid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  IF (activity_date_completed is NULL) THEN
    SELECT
      sessions_personnel_id INTO sid
    FROM
      sessions_personnel s
    WHERE
      sessions_id = sesid AND personnel_id = perid AND activity_id = acid AND
      activity_date_started = activity_date_started_in AND  activity_date_completed is NULL
      AND activity_lead = activity_lead_in;
  ELSE
    SELECT
      sessions_personnel_id INTO sid
    FROM
      sessions_personnel s
    WHERE
      sessions_id = sesid AND personnel_id = perid AND activity_id = acid AND
      activity_date_started = activity_date_started_in AND  activity_date_completed = activity_date_completed_in
      AND activity_lead = activity_lead_in;
  END IF;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions_personnel: session_name="%" start_time="%" end_time="%" line_count="%" session_notes="%"
    personnel_name="%" personnel_role="%" organization="%" office_phone="%" cell_phone="%" email="%" personnel_notes="%"
    activity="%" activity_description="%" activity_date_started ="%" activity_date_completed="%" activity_lead="%"',
    session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in,
    personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in,
    activity_in, activity_description_in, activity_date_started_in, activity_date_completed_in, activity_lead_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_epi_sessions_personnel_id(
  flights_id_in UUID,
  session_name_in text, start_time_in time, end_time_in time, line_count_in float, session_notes_in text,
  personnel_name_in TEXT, personnel_role_in TEXT, organization_in TEXT, office_phone_in TEXT,
  cell_phone_in TEXT, email_in TEXT, personnel_notes_in TEXT, activity_in TEXT, activity_description_in TEXT,
  activity_date_started_in DATE, activity_date_completed_in DATE, activity_lead_in TEXT
) RETURNS UUID AS $$
DECLARE
  sid UUID;
  sesid UUID;
  perid UUID;
  acid UUID;
BEGIN
  SELECT get_epi_sessions_id(flights_id_in, session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in) INTO sesid;
  SELECT get_personnel_id(personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in) INTO perid;
  SELECT get_activity_id(activity_in, activity_description_in) INTO acid;

  IF (activity_date_completed is NULL) THEN
    SELECT
      sessions_personnel_id INTO sid
    FROM
      sessions_personnel s
    WHERE
      sessions_id = sesid AND personnel_id = perid AND activity_id = acid AND
      activity_date_started = activity_date_started_in AND  activity_date_completed is NULL
      AND activity_lead = activity_lead_in;
  ELSE
    SELECT
      sessions_personnel_id INTO sid
    FROM
      sessions_personnel s
    WHERE
      sessions_id = sesid AND personnel_id = perid AND activity_id = acid AND
      activity_date_started = activity_date_started_in AND  activity_date_completed = activity_date_completed_in
      AND activity_lead = activity_lead_in;
  END IF;

  IF (sid IS NULL) THEN
    RAISE EXCEPTION 'Unknown sessions_personnel: session_name="%" start_time="%" end_time="%" line_count="%" session_notes="%"
    personnel_name="%" personnel_role="%" organization="%" office_phone="%" cell_phone="%" email="%" personnel_notes="%"
    activity="%" activity_description="%" activity_date_started ="%" activity_date_completed="%" activity_lead="%"',
    session_name_in, start_time_in, end_time_in, line_count_in, session_notes_in,
    personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in,
    activity_in, activity_description_in, activity_date_started_in, activity_date_completed_in, activity_lead_in;
  END IF;

  RETURN sid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER sessions_personnel_insert_trig
  INSTEAD OF INSERT ON
  sessions_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE insert_sessions_personnel_from_trig();

CREATE TRIGGER sessions_personnel_update_trig
  INSTEAD OF UPDATE ON
  sessions_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE update_sessions_personnel_from_trig();

CREATE TRIGGER sessions_personnel_insert_epi_trig
  INSTEAD OF INSERT ON
  sessions_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE insert_epi_sessions_personnel_from_trig();

CREATE TRIGGER sessions_personnel_update_epi_trig
  INSTEAD OF UPDATE ON
  sessions_personnel_view FOR EACH ROW
  EXECUTE PROCEDURE update_epi_sessions_personnel_from_trig();
